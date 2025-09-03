#[cfg(all(feature = "sqs", feature = "it"))]
mod common;
#[cfg(all(feature = "sqs", feature = "it"))]
mod localstack;

#[cfg(all(feature = "sqs", feature = "it"))]
mod sqs_it {
    use ctor::ctor;
    use rust_payload_offloading_for_aws::offload::{
        offloading::OFFLOADED_MARKER_ATTRIBUTE, sqs::offloading_client,
    };
    use tracing::info;

    use crate::{
        common,
        localstack::{create_bucket, create_queue, start_aws_stack},
    };

    #[ctor]
    fn before_all() {
        common::init();
    }

    // By default test runs on 1 thread and blocks, need to use multi_thread for s3 offloads not to block
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sqs_send_receive_offloaded_message(
    ) -> Result<(), Box<dyn std::error::Error + 'static>> {
        let (_container, aws_config) = start_aws_stack().await?;

        const TEST_BUCKET: &str = "my-bucket";
        let bucket = create_bucket(&aws_config, TEST_BUCKET).await?;
        info!("Bucket created: {:?}", bucket.location);

        const TEST_QUEUE: &str = "my-queue.fifo";
        let test_queue_url = create_queue(&aws_config, TEST_QUEUE)
            .await?
            .queue_url
            .expect("queue created");
        info!("Queue created: {}", test_queue_url);

        const TEST_BODY: &str = "<my/><body/>";

        let sqs_offloading_client =
            offloading_client(&aws_config, TEST_BUCKET, TEST_BODY.len() - 1);

        // Ensure ofloaded and downloaded has the same body

        sqs_offloading_client
            .send_message()
            .queue_url(&test_queue_url)
            .message_body(TEST_BODY)
            .message_group_id("tst-grp-1")
            .send()
            .await
            .unwrap();

        let r = sqs_offloading_client
            .receive_message()
            .queue_url(&test_queue_url)
            .send()
            .await
            .unwrap();

        assert_eq!(r.messages()[0].body.clone().unwrap(), TEST_BODY.to_owned());

        // Ensure message was really offloaded

        sqs_offloading_client
            .send_message()
            .queue_url(&test_queue_url)
            .message_body(TEST_BODY)
            .message_group_id("tst-grp-1")
            .send()
            .await
            .unwrap();

        let non_offloading_client = aws_sdk_sqs::Client::new(&aws_config);

        let raw_offloaded = non_offloading_client
            .receive_message()
            .queue_url(&test_queue_url)
            .set_message_attribute_names(Some(vec!["All".to_owned()]))
            .send()
            .await
            .unwrap();

        let raw_message = raw_offloaded.messages()[0].clone();
        info!("Got raw message: {:?}", &raw_message);
        assert!(
            &raw_message
                .body
                .clone()
                .unwrap()
                .contains("software.amazon.payloadoffloading.PayloadS3Pointer"),
            "Body was not offloaded to S3"
        );
        assert!(
            &raw_message
                .message_attributes
                .unwrap()
                .get(OFFLOADED_MARKER_ATTRIBUTE)
                .is_some(),
            "Offload marker attribute is empty"
        );

        Ok(())
    }
}
