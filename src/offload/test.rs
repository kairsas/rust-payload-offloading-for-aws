use core::str;
use std::sync::Once;

use aws_config::{ConfigLoader, Region};
use aws_sdk_s3::config::Credentials as S3Credentials;
use tracing::info;
use tracing_subscriber::EnvFilter;
use wiremock::{matchers::method, Mock, MockServer, Request, ResponseTemplate};

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(setup_tracing);
}

pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_thread_names(true)
        .with_ansi(true)
        .init();
}

pub async fn s3_config(base_endpoint_url: &str) -> aws_config::SdkConfig {
    mock_aws_endpoint_config(base_endpoint_url, "s3/")
        .credentials_provider(S3Credentials::for_tests())
        .load()
        .await
}

pub fn mock_aws_endpoint_config(base_endpoint_url: &str, aws_service_path: &str) -> ConfigLoader {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(format!("{base_endpoint_url}/{aws_service_path}"))
        .region(Region::from_static("eu-west-2"))
}

pub async fn expect_no_s3_put_calls(mock_server: &MockServer) {
    Mock::given(method("PUT"))
        .respond_with(move |r: &Request| {
            info!("S3 Request: {:?}", r);
            let body = str::from_utf8(&r.body).unwrap();
            info!("S3 Body: {:?}", body);

            ResponseTemplate::new(400)
                .set_body_raw(format!("Unexpected call: {body}"), "text/plain")
        })
        .expect(0)
        .named("No S3 PUT calls")
        .mount(mock_server)
        .await;
}

pub async fn given_s3_put_fails(mock_server: &MockServer) {
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(400))
        .expect(1)
        .named("S3 PUT failure")
        .mount(mock_server)
        .await;
}

pub async fn given_s3_put_succeeds(mock_server: &MockServer) {
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .named("S3 PUT success")
        .mount(mock_server)
        .await;
}
