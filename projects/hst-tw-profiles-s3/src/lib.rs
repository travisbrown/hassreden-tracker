use awscreds::{error::CredentialsError, Credentials};
use awsregion::Region;
use s3::bucket::Bucket;
use serde_derive::Deserialize;

pub mod json;
pub mod state;

#[derive(Debug, Deserialize)]
pub struct Config {
    access_key: String,
    secret_key: String,
    region: String,
    bucket: String,
    pub prefix: String,
}

impl Config {
    pub fn credentials(&self) -> Result<Credentials, CredentialsError> {
        Credentials::new(
            Some(&self.access_key),
            Some(&self.secret_key),
            None,
            None,
            None,
        )
    }

    pub fn region(&self) -> Result<Region, std::str::Utf8Error> {
        self.region.parse()
    }

    pub fn bucket(&self) -> Result<Bucket, Error> {
        Ok(Bucket::new(
            &self.bucket,
            self.region()?,
            self.credentials()?,
        )?)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("S3 error")]
    S3(#[from] s3::error::S3Error),
    #[error("AWS credentials error")]
    AwsCredentials(#[from] CredentialsError),
    #[error("UTF-8 error")]
    Utf8(#[from] std::str::Utf8Error),
}
