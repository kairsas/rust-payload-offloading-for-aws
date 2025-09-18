# payload-offloading-for-aws

[![Crates.io][crates-badge]][crates-url]
[![Build Status][ci-badge]][ci-url]

[crates-badge]: https://img.shields.io/crates/v/payload-offloading-for-aws.svg
[crates-url]: https://crates.io/crates/payload-offloading-for-aws
[ci-badge]: https://github.com/kairsas/rust-payload-offloading-for-aws/workflows/CI/badge.svg?branch=main
[ci-url]: https://github.com/kairsas/rust-payload-offloading-for-aws/actions?query=workflow%3ACI+branch%3Amain

Large Payload offloading to S3, compatible with existing AWS Java libraries.

When sending or receiving via SNS/SQS, large message payloads will be offloaded to or download from S3 in this format:

```json
[
    "software.amazon.payloadoffloading.PayloadS3Pointer",
    {
        "s3BucketName": "offloading-bucket",
        "s3Key": "9cc30888-0e2e-40da-b594-c9ca2cd58722"
    }
]
```

It's compatible with the [common offloading Java library](https://github.com/awslabs/payload-offloading-java-common-lib-for-aws/), which is used by the following AWS Java libraries:
* [amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib)
* [amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib)

### Usage

Add dependency to Cargo.toml:
```toml
rust-payload-offloading-for-aws = { version = "0.1", features = ["sns", "sqs"] }
```

Setup AWS SDK interceptors and use the SDK as usual:
```rust
use payload_offloading_for_aws::offload;
use aws_sdk_sns::{config::{Config as SnsConfig}};

async fn send_sns_and_receive_sqs() {
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;

    let sns_s3_offload_interceptor = offload::sns::S3OffloadInterceptor::new(
        &aws_config,
        offload::id_provider::RandomUuidProvider::default(), // bucket key generator
        "my-bucket".to_owned(), // offloading bucket
        255000, // max non-offload body size
    );

    // or use a helper function `offloading_client(&aws_config, "my-bucket".to_owned(), 25000)`
    let sns_client = aws_sdk_sns::Client::from_conf(
        SnsConfig::new(&aws_config)
            .to_builder()
            // Set offloading interceptor
            .interceptor(sns_s3_offload_interceptor)
            .build(),
    );

    // Send SNS using AWS SDK as usual
    let _ = sns_client
        .publish()
        .topic_arn("TEST-TOPIC".to_owned())
        .set_message(Some("TEST BODY".to_owned()))
        .send()
        .await
        .unwrap();

    // Another option to setup AWS client
    let sqs_client = offload::sqs::offloading_client(&aws_config, "my-bucket", 25000);

    // Receive message from SQS using the AWS SDK as usual.
    // If the message body exceeds the offloading size limit,
    // it will be automatically downloaded from S3 behind the scenes.
    let _ = sqs_client
        .receive_message()
        .queue_url("TEST-QUEUE")
        .send()
        .await
        .unwrap();
}
```

Alternatively you can download offloaded bodies manually like this:
```rust
use payload_offloading_for_aws::offload;

async fn download_offloaded_payload_explicitly() -> Result<(), String> {
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;

    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::from(&aws_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    );

    let raw_payload = r#"[
        "software.amazon.payloadoffloading.PayloadS3Pointer",
        {
            "s3BucketName": "offloading-bucket",
            "s3Key": "9cc30888-0e2e-40da-b594-c9ca2cd58722"
        }
    ]"#;

    // It's only `Some` when download was needed, failover to original payload
    let res = offload::sqs::try_downloading_body(&s3_client, raw_payload)
        .map_err(|e| e.to_string())?
        .unwrap_or(raw_payload.to_owned());

    Ok(())
}
```

### Additional References

[AWS SDK Interceptors discussion](https://github.com/awslabs/aws-sdk-rust/discussions/853)

AWS SDK interceptor examples:
  * [Replaying body](https://github.com/awslabs/aws-sdk-rust/blob/505dab66bf0801ca743212678d47d6490d2beba9/sdk/aws-smithy-runtime/src/client/http/test_util/dvr/replay.rs#L338)
  * [Compressing body](https://github.com/awslabs/aws-sdk-rust/blob/505dab66bf0801ca743212678d47d6490d2beba9/sdk/cloudwatch/src/client_request_compression.rs#L138)

### License

Licensed under either of

 - MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)
 - Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)

at your option.

#### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
time by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.