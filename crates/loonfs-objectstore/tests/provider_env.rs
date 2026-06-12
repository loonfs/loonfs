#![allow(dead_code)]
// This file is imported as a helper module by provider tests and also compiled as its own test target.

use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const PROVIDER_ENV_EXAMPLE_RELATIVE_PATH: &str =
    "tests/provider-conformance.env.example";

pub(crate) const AWS_S3_REQUIRED_VARS: &[&str] = &[
    "LOON_TEST_S3_BUCKET",
    "LOON_TEST_S3_REGION",
    "LOON_TEST_S3_ACCESS_KEY_ID",
    "LOON_TEST_S3_SECRET_ACCESS_KEY",
];

pub(crate) const AWS_S3_OPTIONAL_VARS: &[&str] = &[
    "LOON_TEST_S3_ENDPOINT",
    "LOON_TEST_S3_SESSION_TOKEN",
    "LOON_TEST_S3_PREFIX",
];

pub(crate) const CLOUDFLARE_R2_REQUIRED_VARS: &[&str] = &[
    "LOON_TEST_R2_BUCKET",
    "LOON_TEST_R2_ACCOUNT_ID",
    "LOON_TEST_R2_ENDPOINT",
    "LOON_TEST_R2_ACCESS_KEY_ID",
    "LOON_TEST_R2_SECRET_ACCESS_KEY",
];

pub(crate) const CLOUDFLARE_R2_OPTIONAL_VARS: &[&str] = &["LOON_TEST_R2_PREFIX"];

pub(crate) const GCP_GCS_REQUIRED_VARS: &[&str] = &["LOON_TEST_GCS_BUCKET"];

pub(crate) const GCP_GCS_OPTIONAL_VARS: &[&str] = &[
    "LOON_TEST_GCS_SERVICE_ACCOUNT_KEY_PATH",
    "LOON_TEST_GCS_APPLICATION_CREDENTIALS",
    "LOON_TEST_GCS_PREFIX",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AwsS3ConformanceConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CloudflareR2ConformanceConfig {
    pub bucket: String,
    pub account_id: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcpGcsConformanceConfig {
    pub bucket: String,
    pub service_account_key_path: Option<String>,
    pub application_credentials_path: Option<String>,
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProviderEnvError {
    MissingVar { name: &'static str },
    EmptyVar { name: &'static str },
    NonUnicodeVar { name: &'static str },
    ExampleFileRead(String),
}

impl AwsS3ConformanceConfig {
    pub(crate) fn from_env() -> Result<Self, ProviderEnvError> {
        Ok(Self {
            bucket: required_env("LOON_TEST_S3_BUCKET")?,
            region: required_env("LOON_TEST_S3_REGION")?,
            endpoint: optional_env("LOON_TEST_S3_ENDPOINT"),
            access_key_id: required_env("LOON_TEST_S3_ACCESS_KEY_ID")?,
            secret_access_key: required_env("LOON_TEST_S3_SECRET_ACCESS_KEY")?,
            session_token: optional_env("LOON_TEST_S3_SESSION_TOKEN"),
            prefix: optional_env("LOON_TEST_S3_PREFIX").unwrap_or_else(|| default_prefix("aws-s3")),
        })
    }
}

impl CloudflareR2ConformanceConfig {
    pub(crate) fn from_env() -> Result<Self, ProviderEnvError> {
        Ok(Self {
            bucket: required_env("LOON_TEST_R2_BUCKET")?,
            account_id: required_env("LOON_TEST_R2_ACCOUNT_ID")?,
            endpoint: required_env("LOON_TEST_R2_ENDPOINT")?,
            access_key_id: required_env("LOON_TEST_R2_ACCESS_KEY_ID")?,
            secret_access_key: required_env("LOON_TEST_R2_SECRET_ACCESS_KEY")?,
            prefix: optional_env("LOON_TEST_R2_PREFIX").unwrap_or_else(|| default_prefix("r2")),
        })
    }
}

impl GcpGcsConformanceConfig {
    pub(crate) fn from_env() -> Result<Self, ProviderEnvError> {
        Ok(Self {
            bucket: required_env("LOON_TEST_GCS_BUCKET")?,
            service_account_key_path: optional_env("LOON_TEST_GCS_SERVICE_ACCOUNT_KEY_PATH"),
            application_credentials_path: optional_env("LOON_TEST_GCS_APPLICATION_CREDENTIALS"),
            prefix: optional_env("LOON_TEST_GCS_PREFIX")
                .unwrap_or_else(|| default_prefix("gcp-gcs")),
        })
    }
}

impl fmt::Display for ProviderEnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingVar { name } => write!(f, "missing environment variable {name}"),
            Self::EmptyVar { name } => write!(f, "environment variable {name} must not be empty"),
            Self::NonUnicodeVar { name } => {
                write!(f, "environment variable {name} must be valid Unicode")
            }
            Self::ExampleFileRead(message) => {
                write!(f, "failed to read provider env example file: {message}")
            }
        }
    }
}

pub(crate) fn provider_env_example_contents() -> Result<String, ProviderEnvError> {
    fs::read_to_string(provider_env_example_path())
        .map_err(|err| ProviderEnvError::ExampleFileRead(err.to_string()))
}

fn provider_env_example_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(PROVIDER_ENV_EXAMPLE_RELATIVE_PATH)
}

fn required_env(name: &'static str) -> Result<String, ProviderEnvError> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Err(ProviderEnvError::EmptyVar { name }),
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Err(ProviderEnvError::MissingVar { name }),
        Err(std::env::VarError::NotUnicode(_)) => Err(ProviderEnvError::NonUnicodeVar { name }),
    }
}

fn optional_env(name: &'static str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

#[allow(clippy::disallowed_methods)]
fn default_prefix(provider: &str) -> String {
    // Provider conformance prefixes need to be unique per ad hoc test run.
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("conformance/{provider}/{}-{stamp}", std::process::id())
}
