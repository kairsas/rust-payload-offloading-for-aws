use thiserror::Error;

#[derive(Error, Debug)]
pub enum OffloadInterceptorError {
    #[error("Failure to offload to s3: {_0}")]
    FailedToOffloadToS3(String),
    #[error("Failed to put S3 Object: {_0}")]
    FailedToPutS3Object(String),
    #[error("Failed to build builder: {_0}")]
    FailedToBuildType(String),
    #[error("Failed to load object from s3: {_0}")]
    FailedToLoadFromS3(String),
    #[error("Failed to generate S3 key: {_0}")]
    FailedToGenerateS3Key(String),
    #[error("Failed to rewrite contents: {_0}")]
    FailedToRewriteContents(String),
    #[error("There was an error handling a bytestream: {_0}")]
    ByteStreamError(String),
    #[error("{_0}")]
    DeserialisationError(String),
}
