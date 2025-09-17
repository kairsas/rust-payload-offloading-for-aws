use super::{error::OffloadInterceptorError, id_provider::IdProvider};
use aws_config::SdkConfig;
use aws_sdk_s3::Client as S3Client;
use core::str;
use futures::{executor, TryFutureExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// This attribute should be set on SQS messages for extended java client to pick it up
//  and download the offloaded content automatically.
pub const OFFLOADED_MARKER_ATTRIBUTE: &str = "ExtendedPayloadSize";

/// Where original_body > max_body_size this function will offload to s3.
pub(crate) fn try_offload_body_blocking<Idp: IdProvider>(
    original_body: &str,
    s3_client: &S3Client,
    id_provider: &Idp,
    bucket_name: &str,
    max_body_size: usize,
) -> Result<Option<String>, OffloadInterceptorError> {
    let original_body_len = original_body.len();
    if original_body_len > max_body_size {
        let bucket_key = id_provider
            .generate()
            .map_err(OffloadInterceptorError::FailedToGenerateS3Key)?;
        let put_obj_output = executor::block_on(async {
            let bucket_body =
                aws_sdk_s3::primitives::ByteStream::from(original_body.as_bytes().to_vec());

            s3_client
                .put_object()
                .bucket(bucket_name)
                .key(&bucket_key)
                .body(bucket_body)
                .send()
                .await
                .map_err(|e| OffloadInterceptorError::FailedToPutS3Object(e.to_string()))
        })?;

        tracing::info!("Got s3 put result: {:?}", put_obj_output);

        let offloaded_body = format!(
            "[\
            \"software.amazon.payloadoffloading.PayloadS3Pointer\",\
            {{\
                \"s3BucketName\": \"{bucket_name}\",\
                \"s3Key\": \"{bucket_key}\"\
            }}\
        ]"
        );
        Ok(Some(offloaded_body))
    } else {
        Ok(None)
    }
}

pub fn deserialize_s3_pointer(payload: &str) -> Result<PayloadS3Pointer, OffloadInterceptorError> {
    let parsed: Value = serde_json::from_str(payload)
        .map_err(|e| OffloadInterceptorError::DeserialisationError(e.to_string()))?;

    if let Some(array) = parsed.as_array() {
        if array.len() < 2 {
            return Err(deserializer_error());
        }

        let is_s3_pointer_payload = array
            .first()
            .and_then(|v| v.as_str())
            .map(|v| v == "software.amazon.payloadoffloading.PayloadS3Pointer")
            .unwrap_or(false);
        if !is_s3_pointer_payload {
            return Err(deserializer_error());
        }

        let s3_pointer: PayloadS3Pointer = serde_json::from_value(array[1].clone())
            .map_err(|e| OffloadInterceptorError::DeserialisationError(e.to_string()))?;
        return Ok(s3_pointer);
    }

    Err(deserializer_error())
}

fn deserializer_error() -> OffloadInterceptorError {
    OffloadInterceptorError::DeserialisationError("Invalid Format".to_string())
}

pub fn s3_client(aws_config: &SdkConfig) -> S3Client {
    S3Client::from_conf(
        aws_sdk_s3::Config::from(aws_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

pub fn download_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_pointer: &PayloadS3Pointer,
) -> Result<String, OffloadInterceptorError> {
    let downloaded = executor::block_on(
        async {
            s3_client
                .get_object()
                .bucket(&s3_pointer.s3_bucket_name)
                .key(&s3_pointer.s3_key)
                .send()
                .await
                .map_err(|e| OffloadInterceptorError::FailedToLoadFromS3(e.to_string()))
        }
        .and_then(|r| {
            r.body
                .collect()
                .map_err(|e| OffloadInterceptorError::ByteStreamError(e.to_string()))
        }),
    )?;

    let data_string = String::from_utf8(downloaded.into_bytes().to_vec())
        .map_err(|e| OffloadInterceptorError::ByteStreamError(e.to_string()))?;

    Ok(data_string)
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PayloadS3Pointer {
    pub s3_bucket_name: String,
    pub s3_key: String,
}

#[cfg(test)]
mod tests {
    use crate::offload::offloading::{deserialize_s3_pointer, PayloadS3Pointer};

    #[test]
    fn deserializes_s3_pointer() {
        let offloaded_payload = r#"[
            "software.amazon.payloadoffloading.PayloadS3Pointer",
            {"s3BucketName": "offload-test", "s3Key": "42ced2b1-b2f7-4b59-b1cc-c1a4b5349edf"}
        ]"#;

        let deserialized_ptr = deserialize_s3_pointer(offloaded_payload).unwrap();

        assert_eq!(
            PayloadS3Pointer {
                s3_bucket_name: "offload-test".to_owned(),
                s3_key: "42ced2b1-b2f7-4b59-b1cc-c1a4b5349edf".to_owned(),
            },
            deserialized_ptr
        )
    }
}
