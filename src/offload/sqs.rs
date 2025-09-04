use std::collections::HashMap;

use super::id_provider::{IdProvider, RandomUuidProvider};
use super::offloading::{s3_client, try_offload_body_blocking, OFFLOADED_MARKER_ATTRIBUTE};
use super::{error::OffloadInterceptorError, offloading::deserialize_s3_pointer};
use aws_config::SdkConfig;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::types::{MessageAttributeValue, SendMessageBatchRequestEntry};
use aws_sdk_sqs::{
    config::{Config as SqsConfig, Intercept, RuntimeComponents},
    operation::{
        receive_message::ReceiveMessageOutput, send_message::SendMessageInput,
        send_message_batch::SendMessageBatchInput,
    },
    Client as SqsClient,
};
use aws_smithy_runtime_api::client::interceptors::context::{self};
use aws_smithy_types::config_bag::ConfigBag;
use tracing::error;

#[derive(Debug)]
pub struct S3OffloadInterceptor<Idp: IdProvider> {
    s3_client: S3Client,
    id_provider: Idp,
    bucket_name: String,
    max_body_size: usize,
}

impl<Idp: IdProvider> S3OffloadInterceptor<Idp> {
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
        "SQSS3OffloadInterceptor"
    }

    fn modify_before_serialization(
        &self,
        context: &mut context::BeforeSerializationInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        tracing::info!("Modifying context before serialization: {:?}", context);
        let input_mut = context.input_mut();
        if let Some(original_input) = input_mut.downcast_mut::<SendMessageInput>() {
            if let Some(modified_input) = try_offload_sqs_send_message_input(
                original_input,
                &self.s3_client,
                &self.id_provider,
                &self.bucket_name,
                self.max_body_size,
            )? {
                tracing::debug!("Body modified: {:?}", modified_input);
                *original_input = modified_input;
            }
        } else if let Some(original_input) = input_mut.downcast_mut::<SendMessageBatchInput>() {
            if let Some(modified_input) = try_offload_sqs_send_message_batch_input(
                original_input,
                &self.s3_client,
                &self.id_provider,
                &self.bucket_name,
                self.max_body_size,
            )? {
                tracing::debug!("Batch Body modified: {:?}", modified_input);
                *original_input = modified_input;
            }
        }

        Ok(())
    }

    fn modify_before_completion(
        &self,
        context: &mut context::FinalizerInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(Ok(output_mut)) = context.output_or_error_mut() {
            if let Some(original_output) = output_mut.downcast_mut::<ReceiveMessageOutput>() {
                if original_output.messages().is_empty() {
                    return Ok(());
                }

                for m in original_output.messages.as_mut().unwrap() {
                    if let Some(orig_body) = m.body.as_ref() {
                        let downloaded_body_res = try_downloading_body(&self.s3_client, orig_body);
                        match downloaded_body_res {
                            Ok(Some(downloaded_body)) => {
                                m.body = Some(downloaded_body);
                            }
                            Err(e) => {
                                error!("Error downloading batch entry body \"{}\": {}. Failing back to offloaded body", orig_body, e);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn try_offload_sqs_send_message_input<Idp: IdProvider>(
    original_input: &SendMessageInput,
    s3_client: &S3Client,
    id_provider: &Idp,
    bucket_name: &str,
    max_body_size: usize,
) -> Result<Option<SendMessageInput>, OffloadInterceptorError> {
    let maybe_modified_body_and_size = original_input.message_body().and_then(|body| {
        try_offload_body_blocking(body, s3_client, id_provider, bucket_name, max_body_size)
            .inspect_err(|e| error!("Error offloading content: {}", e))
            .ok()
            .flatten()
            .map(|rr| (rr, body.len()))
    });

    let maybe_modified_input = maybe_modified_body_and_size.map(|(body, original_length)| {
        let mut modified_builder = original_input.to_owned();
        modified_builder.message_body = Some(body);

        modified_builder.message_attributes = Some(put_offloaded_content_marker_attribute(
            modified_builder.message_attributes,
            original_length,
        )?);

        Ok(modified_builder)
    });

    maybe_modified_input.transpose()
}

fn try_offload_sqs_send_message_batch_input<Idp: IdProvider>(
    original_input: &mut SendMessageBatchInput,
    s3_client: &S3Client,
    id_provider: &Idp,
    bucket_name: &str,
    max_body_size: usize,
) -> Result<Option<SendMessageBatchInput>, OffloadInterceptorError> {
    let any_offload_candidates = original_input
        .entries()
        .iter()
        .any(|e| (e.message_body().len() > max_body_size));

    if !any_offload_candidates {
        return Ok(None);
    }

    let mut modified_builder = SendMessageBatchInput::builder();
    if let Some(value) = original_input.queue_url() {
        modified_builder = modified_builder.queue_url(value);
    }

    modified_builder =
        modified_builder.set_entries(original_input.entries.as_ref().map(|entries| {
            entries
                .iter()
                .flat_map(|orig_entry| {
                    let offloaded = try_offload_body_blocking(
                        orig_entry.message_body(),
                        s3_client,
                        id_provider,
                        bucket_name,
                        max_body_size,
                    );

                    let mut modified_entry = orig_entry.to_owned();

                    match offloaded {
                        Ok(Some(offloaded_body)) => {
                            modified_entry.message_body = offloaded_body;
                            modified_entry.message_attributes =
                                Some(put_offloaded_content_marker_attribute(
                                    modified_entry.message_attributes,
                                    orig_entry.message_body().len(),
                                )?);
                        }
                        Err(e) => {
                            error!(
                                "Error offloading batch entry body \"{}\": {}. \
                                Failing back to original body",
                                orig_entry.message_body(),
                                e
                            );
                        }
                        _ => {}
                    }

                    Ok::<SendMessageBatchRequestEntry, OffloadInterceptorError>(modified_entry)
                })
                .collect()
        }));

    modified_builder
        .build()
        .map_err(|e| OffloadInterceptorError::FailedToBuildType(e.to_string()))
        .map(Some)
}

pub fn try_downloading_body(
    s3_client: &S3Client,
    b: &str,
) -> Result<Option<String>, OffloadInterceptorError> {
    if !b.contains("PayloadS3Pointer") {
        return Ok(None);
    }

    let deserialized_ptr = deserialize_s3_pointer(b)?;

    Ok(Some(crate::offload::offloading::download_from_s3(
        s3_client,
        &deserialized_ptr,
    )?))
}

pub fn offloading_client(
    aws_config: &SdkConfig,
    offloading_bucket: &str,
    max_non_offloaded_size: usize,
) -> SqsClient {
    let s3_offload_interceptor = S3OffloadInterceptor::new(
        aws_config,
        RandomUuidProvider::default(),
        offloading_bucket.to_owned(),
        max_non_offloaded_size,
    );

    SqsClient::from_conf(
        SqsConfig::new(aws_config)
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
                    "Error while building sqs message attributes {e}"
                ))
            })?,
    );
    Ok(modified_attributes)
}

#[cfg(test)]
mod tests {
    use core::str;

    use aws_sdk_sqs::{
        config::Credentials as SqsCredentials, types::SendMessageBatchRequestEntry,
        Client as SqsClient, Config,
    };
    use ctor::ctor;
    use tracing::info;
    use wiremock::{
        matchers::{header, method, path},
        Mock, MockServer, Request, ResponseTemplate,
    };

    use crate::offload::{
        id_provider::FixedIdsProvider,
        sqs::S3OffloadInterceptor,
        test::{
            self, expect_no_s3_put_calls, given_s3_put_fails, given_s3_put_succeeds,
            mock_aws_endpoint_config, s3_config,
        },
    };

    const TEST_BUCKET: &str = "my-bucket";
    const TEST_QUEUE: &str = "my-queue";
    const TEST_RANDOM_ID: &str = "my-id-123";

    #[ctor]
    fn before_all() {
        test::init();
    }

    // By default test runs on 1 thread and blocks, need to use multi_thread for s3 offloads not to block
    #[tokio::test(flavor = "multi_thread")]
    async fn send_batch_no_offload_needed() {
        const MSG_BODY: &str = "send-batch-no-offload-msg";

        let (mock_server, sqs_client) = setup_sdk_client(MSG_BODY.len()).await;

        expect_no_s3_put_calls(&mock_server).await;
        expect_final_sqs_batch_body(&mock_server, MSG_BODY).await;

        when_sending_sqs_batch_with_body(&sqs_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_batch_if_offload_fails_then_original_sent() {
        const MSG_BODY: &str = "send-batch-offload_fails";

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len() - 1).await;

        given_s3_put_fails(&mock_server).await;
        expect_final_sqs_batch_body(&mock_server, MSG_BODY).await;
        when_sending_sqs_batch_with_body(&sns_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_batch_offload_succeeds() {
        const MSG_BODY: &str = "send-batch-offload-succeeds";
        let expected_offload_fragment = offloaded_payload(TEST_BUCKET, TEST_RANDOM_ID);

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len() - 1).await;

        given_s3_put_succeeds(&mock_server).await;
        expect_final_sqs_batch_body(&mock_server, &expected_offload_fragment).await;
        when_sending_sqs_batch_with_body(&sns_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_message_no_offload_needed() {
        const MSG_BODY: &str = "send-message-no-offload-msg";

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len()).await;

        expect_no_s3_put_calls(&mock_server).await;
        expect_final_sqs_message_body(&mock_server, MSG_BODY).await;

        when_sending_sqs_message_with_body(&sns_client, MSG_BODY).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_message_offload_succeeds() {
        const MSG_BODY: &str = "send-message-offload-succeeds";
        let expected_offload_fragment = offloaded_payload(TEST_BUCKET, TEST_RANDOM_ID);

        let (mock_server, sns_client) = setup_sdk_client(MSG_BODY.len() - 1).await;

        given_s3_put_succeeds(&mock_server).await;
        expect_final_sqs_message_body(&mock_server, &expected_offload_fragment).await;

        when_sending_sqs_message_with_body(&sns_client, MSG_BODY).await;
    }

    fn offloaded_payload(bucket_name: &str, bucket_key: &str) -> String {
        format!(
            "[\\\"software.amazon.payloadoffloading.PayloadS3Pointer\\\",\
            {{\\\"s3BucketName\\\": \\\"{bucket_name}\\\",\\\"s3Key\\\": \\\"{bucket_key}\\\"}}]\",\
            \"MessageAttributes\":{{\"ExtendedPayloadSize\""
        )
    }

    async fn setup_sdk_client(max_non_offload_size: usize) -> (MockServer, aws_sdk_sqs::Client) {
        let mock_server = MockServer::start().await;

        let base_endpoint_url = &mock_server.uri();

        info!("Base endpoint url: {}", base_endpoint_url);
        let s3_offload_interceptor = S3OffloadInterceptor::new(
            &s3_config(base_endpoint_url).await,
            FixedIdsProvider::new(vec![TEST_RANDOM_ID]),
            TEST_BUCKET.to_owned(),
            max_non_offload_size,
        );

        let config = Config::new(
            &mock_aws_endpoint_config(base_endpoint_url, "sqs")
                .credentials_provider(SqsCredentials::for_tests())
                .load()
                .await,
        )
        .to_builder()
        .interceptor(s3_offload_interceptor)
        .build();

        let client = aws_sdk_sqs::Client::from_conf(config);

        (mock_server, client)
    }

    async fn expect_final_sqs_batch_body(mock_server: &MockServer, message_body: &str) {
        let expected_body_fragment = format!(
            r#"{{"QueueUrl":"{}","Entries":[{{"Id":"someid","MessageBody":"{}"#,
            TEST_QUEUE,
            message_body.to_owned()
        );

        Mock::given(method("POST"))
            .and(path("/sqs/"))
            .and(header("x-amz-target", "AmazonSQS.SendMessageBatch"))
            .respond_with(move |r: &Request| {
                info!("SQS Request: {:?}", r);

                let intercepted_body = str::from_utf8(&r.body).unwrap();
                info!("SQS Body: {:?}", intercepted_body);

                assert!(
                    intercepted_body.contains(&expected_body_fragment),
                    "Body does not contain expected fragment: \nbody=\"{intercepted_body}\", \nexpected=\"{expected_body_fragment}\""
                );

                ResponseTemplate::new(200).set_body_raw("{}", "application/json")
            })
            .expect(1)
            .named("Final SQS batch body")
            .mount(mock_server)
            .await;
    }

    async fn expect_final_sqs_message_body(mock_server: &MockServer, message_body: &str) {
        let expected_body_fragment = format!(
            r#"{{"QueueUrl":"{}","MessageBody":"{}"#,
            TEST_QUEUE,
            message_body.to_owned()
        );

        Mock::given(method("POST"))
            .and(path("/sqs/"))
            .and(header("x-amz-target", "AmazonSQS.SendMessage"))
            .respond_with(move |r: &Request| {
                info!("SQS Request: {:?}", r);

                let intercepted_body = str::from_utf8(&r.body).unwrap();
                info!("SQS Body: {:?}", intercepted_body);

                assert!(
                    intercepted_body.contains(&expected_body_fragment),
                    "Body does not contain expected fragment: \nbody=\"{intercepted_body}\", \nexpected=\"{expected_body_fragment}\""
                );

                ResponseTemplate::new(200).set_body_raw("{}", "application/json")
            })
            .expect(1)
            .named("Final SQS body")
            .mount(mock_server)
            .await;
    }

    async fn when_sending_sqs_batch_with_body(sqs_client: &SqsClient, message_body: &str) {
        let _res: aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput = sqs_client
            .send_message_batch()
            .queue_url(TEST_QUEUE.to_owned())
            .set_entries(Some(vec![SendMessageBatchRequestEntry::builder()
                .id("someid")
                .message_body(message_body)
                .build()
                .unwrap()]))
            .send()
            .await
            .unwrap();
    }

    async fn when_sending_sqs_message_with_body(sqs_client: &SqsClient, message_body: &str) {
        let _res: aws_sdk_sqs::operation::send_message::SendMessageOutput = sqs_client
            .send_message()
            .queue_url(TEST_QUEUE.to_owned())
            .set_message_body(Some(message_body.to_owned()))
            .send()
            .await
            .unwrap();
    }
}
