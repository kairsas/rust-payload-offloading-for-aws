# payload-offloading-rust-common-lib-for-aws

Large Payload offloading to S3, compatible with existing java libs.

When sending / receiving on sns/sqs, large message payloads will offload/download to/from S3 in this format:

```json
[
    "software.amazon.payloadoffloading.PayloadS3Pointer",
    {
        "s3BucketName": "offloading-bucket",
        "s3Key": "1a422d25-b38d-4801-b239-49689156f33f"
    }
]
```
It's compatible with common offloading Java lib: https://github.com/awslabs/payload-offloading-java-common-lib-for-aws/, which is used by sqs & sns Java libs:

* https://github.com/awslabs/amazon-sqs-java-extended-client-lib
* https://github.com/awslabs/amazon-sns-java-extended-client-lib

### Usage

Add dependency to Cargo.toml:
```toml
rust-payload-offloading-for-aws = { git = "https://github.com/kairsas/payload-offloading-rust-common-lib-for-aws.git", features = ["sns"] }
```

Setup AWS SDK client:
```rust
use payload_offloading_for_aws::offload;

let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .load()
    .await;

let sns_s3_offload_interceptor = offload::sns::S3OffloadInterceptor::new(
    &aws_config,
    RandomUuidProvider::default(), // bucket key generator
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

// Use AWS SDK as usual
let _publish_res = sns_client
    .publish()
    .topic_arn("TEST-TOPIC".to_owned())
    .set_message(Some("TEST BODY".to_owned()))
    .send()
    .await
    .unwrap();

...

let sqs_offloading_client =
    offload::sqs::offloading_client(&aws_config, "my-bucket".to_owned(), 25000);

let r = sqs_offloading_client
    .receive_message()
    .queue_url(&test_queue_url)
    .send()
    .await
    .unwrap();
```

Alternatively you can download offloaded body manually like this:
```rust
use rust_payload_offloading_for_aws::offload;

let payload = "...";

// It's only `Some` when download was needed, failover to original payload
let res = offload::sqs::try_downloading_body(s3_client, payload)?
  .unwrap_or(payload);
```

### Additional info & references

AWS SDK Interceptors info:
  https://github.com/awslabs/aws-sdk-rust/discussions/853

Useful AWS SDK interceptor examples:
  * [Replaying body](https://github.com/awslabs/aws-sdk-rust/blob/505dab66bf0801ca743212678d47d6490d2beba9/sdk/aws-smithy-runtime/src/client/http/test_util/dvr/replay.rs#L338)


  * [Compressing body](https://github.com/awslabs/aws-sdk-rust/blob/505dab66bf0801ca743212678d47d6490d2beba9/sdk/cloudwatch/src/client_request_compression.rs#L138)