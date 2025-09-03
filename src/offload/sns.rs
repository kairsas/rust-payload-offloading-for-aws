use super::id_provider::RandomUuidProvider;
use super::offloading::{s3_client, try_offload_body_blocking, OFFLOADED_MARKER_ATTRIBUTE};
use super::{error::OffloadInterceptorError, id_provider::IdProvider};
use aws_config::SdkConfig;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sns::types::{MessageAttributeValue, PublishBatchRequestEntry};
use aws_sdk_sns::{
    config::{Config as SnsConfig, Intercept, RuntimeComponents},
    operation::{publish::PublishInput, publish_batch::PublishBatchInput},
    Client as SnsClient,
};
use aws_smithy_runtime_api::client::interceptors::context::{self};
use aws_smithy_types::config_bag::ConfigBag;
use core::str;
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Debug)]
pub struct S3OffloadInterceptor<Idp: IdProvider> {
    s3_client: S3Client,
    id_provider: Idp,
    bucket_name: String,
    max_body_size: usize,
}

impl<Idp: IdProvider + Sync + Send + std::fmt::Debug> S3OffloadInterceptor<Idp> {
    pub fn new(
        aws_config: &SdkConfig,
        id_provider: Idp,
        bucket_name: String,
        max_body_size: usize,
    ) -> Self {
        S3OffloadInterceptor {
            s3_client: s3_client(aws_config),
            id_provider,
            bucket_name,
            max_body_size,
        }
    }
}

impl<Idp: IdProvider + Sync + Send + std::fmt::Debug> Intercept for S3OffloadInterceptor<Idp> {
    fn name(&self) -> &'static str {
        "SNSS3OffloadInterceptor"
    }

    fn modify_before_serialization(
        &self,
        context: &mut context::BeforeSerializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        info!("Modifying context before serialization: {:?}", context);
        let input_mut = context.input_mut();
        if let Some(original_input) = input_mut.downcast_mut::<PublishInput>() {
            if let Some(modified_input) = try_offload_sns_publish_input(
                original_input,
                &self.s3_client,
                &self.id_provider,
                &self.bucket_name,
                self.max_body_size,
            )? {
                info!("Sns PublishInput Body modified: {:?}", modified_input);
                *original_input = modified_input;
            }
        } else if let Some(original_input) = input_mut.downcast_mut::<PublishBatchInput>() {
            if let Some(modified_input) = try_offload_sns_publish_batch_input(
                original_input,
                &self.s3_client,
                &self.id_provider,
                &self.bucket_name,
                self.max_body_size,
            )? {
                info!("Sns PublishBatchInput Body modified: {:?}", modified_input);
                *original_input = modified_input;
            }
        }
        Ok(())
    }
}

fn try_offload_sns_publish_input<Idp: IdProvider>(
    original_input: &PublishInput,
    s3_client: &S3Client,
    id_provider: &Idp,
    bucket_name: &str,
    max_body_size: usize,
) -> Result<Option<PublishInput>, OffloadInterceptorError> {
    let maybe_modified_body_and_size = original_input.message().and_then(|body| {
        try_offload_body_blocking(body, s3_client, id_provider, bucket_name, max_body_size)
            .inspect_err(|e| error!("Error offloading content: {}", e))
            .ok()
            .flatten()
            .map(|rr| (rr, body.len()))
    });

    let maybe_modified_input = maybe_modified_body_and_size.map(|(body, original_length)| {
        let mut modified_builder = original_input.to_owned();
        modified_builder.message = Some(body);

        modified_builder.message_attributes = Some(put_offloaded_content_marker_attribute(
            modified_builder.message_attributes,
            original_length,
        )?);

        Ok(modified_builder)
    });

    maybe_modified_input.transpose()
}

fn try_offload_sns_publish_batch_input<Idp: IdProvider>(
    original_input: &mut PublishBatchInput,
    s3_client: &S3Client,
    id_provider: &Idp,
    bucket_name: &str,
    max_body_size: usize,
) -> Result<Option<PublishBatchInput>, OffloadInterceptorError> {
    let any_offload_candidates = original_input
        .publish_batch_request_entries()
        .iter()
        .any(|e| (e.message().len() > max_body_size));

    if !any_offload_candidates {
        return Ok(None);
    }

    let mut modified_builder = PublishBatchInput::builder();
    if let Some(value) = original_input.topic_arn() {
        modified_builder = modified_builder.topic_arn(value);
    }

    modified_builder = modified_builder.set_publish_batch_request_entries(
        original_input
            .publish_batch_request_entries
            .as_ref()
            .map(|entries| {
                entries
                    .iter()
                    .flat_map(|orig_entry| {
                        let offloaded = try_offload_body_blocking(
                            orig_entry.message(),
                            s3_client,
                            id_provider,
                            bucket_name,
                            max_body_size,
                        );

                        let mut modified_entry = orig_entry.to_owned();

                        match offloaded {
                            Ok(Some(offloaded_body)) => {
                                modified_entry.message = offloaded_body;

                                modified_entry.message_attributes = Some(put_offloaded_content_marker_attribute(
                                    modified_entry.message_attributes,
                                    orig_entry.message().len(),
                                )?)
                            },
                            Err(e) => {
                                error!("Error offloading batch entry body \"{}\": {}. Failing back to original body", orig_entry.message(), e);
                            },
                            _ => {}
                        }

                        Ok::<PublishBatchRequestEntry, OffloadInterceptorError>(modified_entry)
                    })
                    .collect()
            }),
    );

    modified_builder
        .build()
        .map_err(|e| OffloadInterceptorError::FailedToBuildType(e.to_string()))
        .map(Some)
}

pub fn offloading_client(
    aws_config: &SdkConfig,
    offloading_bucket: &str,
    max_non_offloaded_size: usize,
) -> SnsClient {
    let s3_offload_interceptor = S3OffloadInterceptor::new(
        aws_config,
        RandomUuidProvider::default(),
        offloading_bucket.to_owned(),
        max_non_offloaded_size,
    );

    SnsClient::from_conf(
        SnsConfig::new(aws_config)
            .to_builder()
            .interceptor(s3_offload_interceptor)
            .build(),
    )
}

// Need to set that for extended client libs to pick it up and download the contents automatically.
//   See: https://github.com/awslabs/amazon-sqs-java-extended-client-lib/blob/07d988c424dea7e4e7d128b217182e1414310560/src/main/java/com/amazon/sqs/javamessaging/SQSExtendedClientConstants.java#L24
fn put_offloaded_content_marker_attribute(
    message_attributes: Option<HashMap<String, MessageAttributeValue>>,
    original_body_length: usize,
) -> Result<HashMap<String, MessageAttributeValue>, OffloadInterceptorError> {
    let mut modified_attributes = message_attributes.clone().unwrap_or_default();
    modified_attributes.insert(
        OFFLOADED_MARKER_ATTRIBUTE.to_owned(),
        MessageAttributeValue::builder()
            .set_data_type(Some("Number".to_owned()))
            .set_string_value(Some(original_body_length.to_string()))
            .build()
            .map_err(|e| {
                OffloadInterceptorError::FailedToRewriteContents(format!(
                    "Error while building sqs message attributes {}",
                    e
                ))
            })?,
    );
    Ok(modified_attributes)
}

#[cfg(test)]
mod tests {
    use core::str;

    use aws_sdk_sns::{
        config::Credentials as SnsCredentials, types::PublishBatchRequestEntry,
        Client as SnsClient, Config as SnsConfig,
    };
    use ctor::ctor;
    use tracing::info;
    use wiremock::{
        matchers::{body_string_contains, method, path},
        Mock, MockServer, Request, ResponseTemplate,
    };

    use crate::offload::{
        id_provider::FixedIdsProvider,
        sns::S3OffloadInterceptor,
        test::{
            self, expect_no_s3_put_calls, given_s3_put_fails, given_s3_put_succeeds,
            mock_aws_endpoint_config, s3_config,
        },
    };

    const TEST_BUCKET: &str = "my-bucket";
    const TEST_TOPIC: &str = "my-topic";
    const TEST_RANDOM_ID: &str = "my-id-123";

    #[ctor]
    fn before_all() {
        test::init();
    }

    // By default test runs on 1 thread and blocks, need to use multi_thread for s3 offloads not to block
    #[tokio::test(flavor = "multi_thread")]
    async fn publish_batch_no_offload_needed() {
        const MSG_BODY: &str = "publish-batch-no-offload-msg";

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len()).await;

        expect_no_s3_put_calls(&mock_server).await;
        expect_final_sns_batch_body(&mock_server, MSG_BODY).await;

        when_calling_sns_publish_batch_with_body(&sns_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_batch_if_offload_fails_then_original_sent() {
        const MSG_BODY: &str = "publish-batch-offload_fails";

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len() - 1).await;

        given_s3_put_fails(&mock_server).await;
        expect_final_sns_batch_body(&mock_server, MSG_BODY).await;

        when_calling_sns_publish_batch_with_body(&sns_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_batch_offload_succeeds() {
        const MSG_BODY: &str = "publish-batch-offload-succeeds";
        let expected_offload_fragment = batch_offloaded_payload(TEST_BUCKET, TEST_RANDOM_ID);

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len() - 1).await;

        given_s3_put_succeeds(&mock_server).await;
        expect_final_sns_batch_body(&mock_server, &expected_offload_fragment).await;

        when_calling_sns_publish_batch_with_body(&sns_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_message_no_offload_needed() {
        const MSG_BODY: &str = "publish-message-no-offload-msg";

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len()).await;

        expect_no_s3_put_calls(&mock_server).await;
        expect_final_sns_message_body(&mock_server, MSG_BODY).await;

        when_calling_sns_publish_message_with_body(&sns_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_message_offload_succeeds() {
        const MSG_BODY: &str = "publish-message-offload-succeeds";
        let expected_offload_fragment = offloaded_payload(TEST_BUCKET, TEST_RANDOM_ID);

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len() - 1).await;

        given_s3_put_succeeds(&mock_server).await;
        expect_final_sns_message_body(&mock_server, &expected_offload_fragment).await;

        when_calling_sns_publish_message_with_body(&sns_client, MSG_BODY).await;
    }

    async fn setup_sdk_client(max_non_offload_size: usize) -> (MockServer, aws_sdk_sns::Client) {
        let mock_server = MockServer::start().await;

        let base_endpoint_url = &mock_server.uri();

        info!("Base endpoint url: {}", base_endpoint_url);
        let s3_offload_interceptor = S3OffloadInterceptor::new(
            &s3_config(base_endpoint_url).await,
            FixedIdsProvider::new(vec![TEST_RANDOM_ID]),
            TEST_BUCKET.to_owned(),
            max_non_offload_size,
        );

        let sns_config = SnsConfig::new(
            &mock_aws_endpoint_config(&base_endpoint_url, "sns")
                .credentials_provider(SnsCredentials::for_tests())
                .load()
                .await,
        )
        .to_builder()
        .interceptor(s3_offload_interceptor)
        .build();

        let sns_client = aws_sdk_sns::Client::from_conf(sns_config);

        (mock_server, sns_client)
    }

    async fn expect_final_sns_batch_body(mock_server: &MockServer, message_body: &str) {
        let expected_body_fragment = format!(
            "PublishBatchRequestEntries.member.1.Message={}",
            message_body.to_owned()
        );

        Mock::given(method("POST"))
            .and(path("/sns/"))
            .and(body_string_contains("Action=PublishBatch"))
            .respond_with(move |r: &Request| {
                info!("SNS Request: {:?}", r);

                let intercepted_body = str::from_utf8(&r.body).unwrap();
                info!("SNS Body: {:?}", intercepted_body);


                assert!(intercepted_body.contains(&expected_body_fragment), "Body does not contain expected fragment: \nbody=\"{}\", \nexpected=\"{}\"", intercepted_body, expected_body_fragment);

                ResponseTemplate::new(200).set_body_raw("<PublishBatchResponse><PublishBatchResult></PublishBatchResult></PublishBatchResponse>", "text/xml")
            }).expect(1)
            .named("Final SNS batch body")
            .mount(mock_server)
            .await;
    }

    async fn expect_final_sns_message_body(mock_server: &MockServer, message_body: &str) {
        let expected_body_fragment = format!("Message={}", message_body.to_owned());

        Mock::given(method("POST"))
            //.and(path("/sns/"))
            //.and(body_string_contains("Action=Publish"))
            .respond_with(move |r: &Request| {
                info!("SNS Request: {:?}", r);

                let intercepted_body = str::from_utf8(&r.body).unwrap();
                info!("SNS Body: {:?}", intercepted_body);

                assert!(
                    intercepted_body.contains(&expected_body_fragment),
                    "Body does not contain expected fragment: \nbody=\"{}\", \nexpected=\"{}\"",
                    intercepted_body,
                    expected_body_fragment
                );

                ResponseTemplate::new(200).set_body_raw(
                    "<PublishResponse><PublishResult></PublishResult></PublishResponse>",
                    "text/xml",
                )
            })
            .expect(1)
            .named("Final SNS batch body")
            .mount(mock_server)
            .await;
    }

    async fn when_calling_sns_publish_batch_with_body(sns_client: &SnsClient, message_body: &str) {
        let _res = sns_client
            .publish_batch()
            .topic_arn(TEST_TOPIC.to_owned())
            .set_publish_batch_request_entries(Some(vec![PublishBatchRequestEntry::builder()
                .id("someid")
                .message(message_body)
                .build()
                .unwrap()]))
            .send()
            .await
            .unwrap();
    }

    async fn when_calling_sns_publish_message_with_body(
        sns_client: &SnsClient,
        message_body: &str,
    ) {
        let _res = sns_client
            .publish()
            .topic_arn(TEST_TOPIC.to_owned())
            .set_message(Some(message_body.to_owned()))
            .send()
            .await
            .unwrap();
    }

    fn offloaded_payload(bucket_name: &str, bucket_key: &str) -> String {
        let msg = format!(
            r#"["software.amazon.payloadoffloading.PayloadS3Pointer",{{"s3BucketName": "{}","s3Key": "{}"}}]"#,
            bucket_name, bucket_key
        );
        format!(
            "{}{}",
            urlencoding::encode(&msg).into_owned(),
            "&MessageAttributes.entry.1.Name=ExtendedPayloadSize"
        )
    }

    fn batch_offloaded_payload(bucket_name: &str, bucket_key: &str) -> String {
        let msg = format!(
            r#"["software.amazon.payloadoffloading.PayloadS3Pointer",{{"s3BucketName": "{}","s3Key": "{}"}}]"#,
            bucket_name, bucket_key
        );
        format!(
            "{}{}",
            urlencoding::encode(&msg).into_owned(),
            "&PublishBatchRequestEntries.member.1.MessageAttributes\
            .entry.1.Name=ExtendedPayloadSize"
        )
    }
}
