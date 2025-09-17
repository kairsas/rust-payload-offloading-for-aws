use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, SdkConfig};
use aws_sdk_s3::{operation::create_bucket::CreateBucketOutput, types::CreateBucketConfiguration};
use aws_sdk_sqs::operation::create_queue::CreateQueueOutput;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::localstack::LocalStack;
use tracing::info;

pub async fn start_aws_stack(
) -> Result<(ContainerAsync<LocalStack>, SdkConfig), Box<dyn std::error::Error + 'static>> {
    let node = LocalStack::default()
        .with_env_var("SERVICES", "s3,sqs,sns")
        .start()
        .await?;
    let host_ip = node.get_host().await?;
    let host_port = node.get_host_port_ipv4(4566).await?;
    info!("Got port: {}", host_port);

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let creds = aws_sdk_sqs::config::Credentials::new("fake", "fake", None, None, "test");
    Ok((
        node,
        aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .credentials_provider(creds)
            .endpoint_url(format!("http://{host_ip}:{host_port}"))
            .load()
            .await,
    ))
}

pub async fn create_bucket(
    aws_config: &SdkConfig,
    bucket: &str,
) -> Result<CreateBucketOutput, Box<dyn std::error::Error + 'static>> {
    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::from(aws_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    );
    Ok(s3_client
        .create_bucket()
        .create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::EuWest2)
                .build(),
        )
        .bucket(bucket)
        .send()
        .await?)
}

pub async fn create_queue(
    aws_config: &SdkConfig,
    queue_name: &str,
) -> Result<CreateQueueOutput, Box<dyn std::error::Error + 'static>> {
    let sqs_client = aws_sdk_sqs::Client::new(aws_config);
    Ok(sqs_client
        .create_queue()
        .queue_name(queue_name)
        .attributes(aws_sdk_sqs::types::QueueAttributeName::FifoQueue, "true")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::ContentBasedDeduplication,
            "true",
        )
        .send()
        .await?)
}
