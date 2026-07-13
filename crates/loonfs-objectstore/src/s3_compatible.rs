//! The shared S3-compatible store implementation behind the AWS S3 and
//! Cloudflare R2 providers.

use crate::secret::SecretString;
use crate::store_io_runtime::StoreIoRuntime;
use crate::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, ProviderObjectStore,
    ProviderObjectStoreConfig, PutMode,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::aws::{AmazonS3Builder, Checksum};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3CompatibleConfig {
    pub provider_name: &'static str,
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub access_key_id: SecretString,
    pub secret_access_key: SecretString,
    pub session_token: Option<SecretString>,
    pub key_prefix: Option<String>,
    pub force_path_style: bool,
    pub sha256_checksum_metadata: bool,
}

#[derive(Clone)]
pub(crate) struct S3CompatibleStore {
    provider_name: &'static str,
    inner: ProviderObjectStore,
    /// Keeps the HTTP IO runtime alive for the provider client's lifetime;
    /// the connector inside the client holds only a handle onto it.
    _io_runtime: StoreIoRuntime,
}

impl fmt::Debug for S3CompatibleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3CompatibleStore")
            .field("provider_name", &self.provider_name)
            .finish_non_exhaustive()
    }
}

impl S3CompatibleStore {
    pub(crate) fn new(config: S3CompatibleConfig) -> Result<Self, ObjectStoreError> {
        validate_config(&config)?;
        let endpoint_url = config
            .endpoint_url
            .as_deref()
            .map(|endpoint| {
                object_store_endpoint_url(&config.bucket, endpoint, config.force_path_style)
            })
            .transpose()?;

        let io_runtime = StoreIoRuntime::new()?;
        let mut builder = AmazonS3Builder::new()
            .with_http_connector(io_runtime.connector())
            .with_client_options(crate::provider_object_store::provider_client_options())
            .with_retry(crate::provider_object_store::provider_retry_config())
            .with_bucket_name(config.bucket)
            .with_region(config.region)
            .with_access_key_id(config.access_key_id.expose())
            .with_secret_access_key(config.secret_access_key.expose())
            .with_virtual_hosted_style_request(!config.force_path_style);

        if let Some(endpoint_url) = endpoint_url {
            let allow_http = endpoint_url.starts_with("http://");
            builder = builder
                .with_endpoint(endpoint_url)
                .with_allow_http(allow_http);
        }
        if let Some(session_token) = &config.session_token {
            builder = builder.with_token(session_token.expose());
        }
        if config.sha256_checksum_metadata {
            builder = builder.with_checksum_algorithm(Checksum::SHA256);
        }

        let provider = builder
            .build()
            .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?;
        let inner = ProviderObjectStore::new(
            Arc::new(provider),
            ProviderObjectStoreConfig {
                key_prefix: config.key_prefix,
                sha256_checksum_metadata: config.sha256_checksum_metadata,
            },
        )?;

        Ok(Self {
            provider_name: config.provider_name,
            inner,
            _io_runtime: io_runtime,
        })
    }
}

#[async_trait]
impl ObjectStore for S3CompatibleStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn validate_config(config: &S3CompatibleConfig) -> Result<(), ObjectStoreError> {
    if config.bucket.trim().is_empty() {
        return Err(ObjectStoreError::Configuration(
            "bucket must not be empty".to_owned(),
        ));
    }
    if config.region.trim().is_empty() {
        return Err(ObjectStoreError::Configuration(
            "region must not be empty".to_owned(),
        ));
    }
    if config.access_key_id.expose().trim().is_empty() {
        return Err(ObjectStoreError::Configuration(
            "access key id must not be empty".to_owned(),
        ));
    }
    if config.secret_access_key.expose().trim().is_empty() {
        return Err(ObjectStoreError::Configuration(
            "secret access key must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn object_store_endpoint_url(
    bucket: &str,
    endpoint_url: &str,
    force_path_style: bool,
) -> Result<String, ObjectStoreError> {
    if force_path_style {
        return Ok(endpoint_url.to_owned());
    }

    let parsed = parse_endpoint_url(endpoint_url)?;
    let bucket_prefix = format!("{}.", bucket.trim());
    if parsed.authority.starts_with(&bucket_prefix) {
        return Ok(endpoint_url.to_owned());
    }

    Ok(format!(
        "{}://{}.{}/{}",
        parsed.scheme, bucket, parsed.authority, parsed.path
    )
    .trim_end_matches('/')
    .to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedEndpoint<'a> {
    scheme: &'a str,
    authority: &'a str,
    path: &'a str,
}

fn parse_endpoint_url(value: &str) -> Result<ParsedEndpoint<'_>, ObjectStoreError> {
    let (scheme, rest) = value
        .strip_prefix("https://")
        .map(|rest| ("https", rest))
        .or_else(|| value.strip_prefix("http://").map(|rest| ("http", rest)))
        .ok_or_else(|| {
            ObjectStoreError::Configuration(
                "endpoint url must start with http:// or https://".to_owned(),
            )
        })?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        return Err(ObjectStoreError::Configuration(
            "endpoint url must include authority".to_owned(),
        ));
    }
    Ok(ParsedEndpoint {
        scheme,
        authority,
        path: path.trim_end_matches('/'),
    })
}

#[cfg(test)]
mod tests {
    use super::{object_store_endpoint_url, S3CompatibleConfig, S3CompatibleStore};

    fn test_config() -> S3CompatibleConfig {
        S3CompatibleConfig {
            provider_name: "test-s3",
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: Some("http://127.0.0.1:9000".to_owned()),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: true,
            sha256_checksum_metadata: true,
        }
    }

    #[test]
    fn s3_compatible_store_builds_without_hidden_runtime() {
        let store = S3CompatibleStore::new(test_config()).expect("construct store");
        let debug = format!("{store:?}");
        assert!(debug.contains("test-s3"));
    }

    #[test]
    fn s3_compatible_store_rejects_blank_credentials() {
        let mut config = test_config();
        config.access_key_id = "".into();
        assert!(S3CompatibleStore::new(config).is_err());
    }

    #[test]
    fn virtual_hosted_endpoint_inserts_bucket_when_endpoint_is_bucketless() {
        let endpoint =
            object_store_endpoint_url("bucket", "https://s3.us-east-2.amazonaws.com", false)
                .expect("endpoint");

        assert_eq!(endpoint, "https://bucket.s3.us-east-2.amazonaws.com");
    }

    #[test]
    fn virtual_hosted_endpoint_preserves_bucket_specific_endpoint() {
        let endpoint =
            object_store_endpoint_url("bucket", "https://bucket.s3.us-east-2.amazonaws.com", false)
                .expect("endpoint");

        assert_eq!(endpoint, "https://bucket.s3.us-east-2.amazonaws.com");
    }

    #[test]
    fn path_style_endpoint_stays_bucketless() {
        let endpoint =
            object_store_endpoint_url("bucket", "https://s3.us-east-2.amazonaws.com", true)
                .expect("endpoint");

        assert_eq!(endpoint, "https://s3.us-east-2.amazonaws.com");
    }
}
