//! [`S3CompatibleStore`]: the S3-API store, constructed per provider.
//!
//! AWS S3 and Cloudflare R2 differ by addressing, credentials, and whether
//! uploads carry a client-computed checksum -- not by behaviour worth a type
//! each, so both are constructors here.

use crate::keyspace::parse_endpoint_url;
use crate::object_store::Result;
use crate::presign::{S3CompatiblePresigner, S3PresignerConfig};
use crate::secret::SecretString;
use crate::store_io_runtime::StoreIoRuntime;
use crate::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, ProviderObjectStore,
    ProviderObjectStoreConfig, PutMode, StoredObjectChecksum,
};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::wire::hex::hex_encode_bytes;
use loonfs_api::{ChecksumAlgorithm, StorageChecksum};
use object_store::aws::{AmazonS3Builder, Checksum};
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Lifetime of the internally signed `HeadObject` used for checksum
/// readback. The request is issued immediately and never handed out, so it
/// only needs to outlive one round trip and its clock skew.
const CHECKSUM_HEAD_TTL: Duration = Duration::from_secs(60);

/// Provider checksum headers this adapter understands, in the order it
/// prefers them, paired with the durable algorithm each one names.
const S3_CHECKSUM_HEADERS: &[(&str, ChecksumAlgorithm)] = &[
    ("x-amz-checksum-sha256", ChecksumAlgorithm::Sha256),
    ("x-amz-checksum-crc64nvme", ChecksumAlgorithm::Crc64nvme),
    ("x-amz-checksum-crc32c", ChecksumAlgorithm::Crc32c),
];

/// Supplies explicit credentials, addressing, and key scoping for AWS S3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsS3StoreConfig {
    /// Bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Signing region passed to the S3 client and presigner.
    pub region: String,
    /// S3-compatible endpoint override, or `None` for the regional AWS endpoint.
    pub endpoint_url: Option<String>,
    /// Access-key id used for SigV4 request signing.
    pub access_key_id: SecretString,
    /// Secret access key used for SigV4 request signing.
    pub secret_access_key: SecretString,
    /// Temporary credential token, or `None` for long-lived credentials.
    pub session_token: Option<SecretString>,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    pub key_prefix: Option<String>,
    /// Selects path-style bucket addressing for compatible endpoints that require it.
    pub force_path_style: bool,
}

/// Supplies explicit S3 credentials and account addressing for Cloudflare R2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudflareR2StoreConfig {
    /// R2 bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Cloudflare account identity, required even though requests use the explicit endpoint.
    pub account_id: String,
    /// Account-level R2 S3 endpoint; this adapter always uses path-style bucket addressing.
    pub endpoint_url: String,
    /// S3-compatible access-key id used for request signing.
    pub access_key_id: SecretString,
    /// S3-compatible secret used for request signing.
    pub secret_access_key: SecretString,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    pub key_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3CompatibleConfig {
    provider_name: &'static str,
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    access_key_id: SecretString,
    secret_access_key: SecretString,
    session_token: Option<SecretString>,
    key_prefix: Option<String>,
    force_path_style: bool,
    /// Attach a client-computed SHA-256 to every upload so the provider
    /// verifies the bytes on PUT (`x-amz-checksum-sha256`). Enabling it also
    /// gives the provider a stored full-object checksum that
    /// [`ObjectStore::head_stored_checksum`] can read back.
    sha256_upload_checksum: bool,
}

/// Implements the LoonFS storage contract on an S3-API endpoint.
#[derive(Clone)]
pub struct S3CompatibleStore {
    provider_name: &'static str,
    inner: ProviderObjectStore,
    /// Signs the `HeadObject` that reads a stored checksum back. The
    /// provider client cannot express that request, and the SigV4 signer
    /// this crate already owns for direct-put URLs can.
    checksum_head_signer: S3CompatiblePresigner,
    /// Sends that one signed request over the store's own IO runtime and
    /// timeout scheme, exactly like every provider-client request.
    http: HttpClient,
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
    /// Builds an AWS S3 store with bounded retries and SHA-256 upload checksums.
    ///
    /// Construction fails for invalid credentials, bucket, region, endpoint,
    /// key prefix, runtime initialization, or provider-client configuration.
    pub fn aws_s3(config: AwsS3StoreConfig) -> Result<Self> {
        Self::new(S3CompatibleConfig {
            provider_name: "aws-s3",
            bucket: config.bucket,
            region: config.region,
            endpoint_url: config.endpoint_url,
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: config.session_token,
            key_prefix: config.key_prefix,
            force_path_style: config.force_path_style,
            sha256_upload_checksum: true,
        })
    }

    /// Builds a Cloudflare R2 store with path-style addressing.
    ///
    /// Construction fails for a blank account id or any invalid shared
    /// configuration, including credentials, endpoint, and key prefix.
    pub fn cloudflare_r2(config: CloudflareR2StoreConfig) -> Result<Self> {
        if config.account_id.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "account id must not be empty".to_owned(),
            ));
        }
        Self::new(S3CompatibleConfig {
            provider_name: "cloudflare-r2",
            bucket: config.bucket,
            region: "auto".to_owned(),
            endpoint_url: Some(config.endpoint_url),
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: None,
            key_prefix: config.key_prefix,
            // The configured endpoint is the bucket-less account host; path
            // style makes the client append the bucket. Virtual hosting would
            // use the endpoint verbatim and address keys as buckets.
            force_path_style: true,
            // Left off pending a live verification: R2 historically rejected
            // aws-chunked checksummed uploads, yet its presigner requires
            // `x-amz-checksum-sha256` on direct puts -- so R2 provably accepts
            // the checksum and this is likely stale caution. Verify
            // single-part and multipart against live R2 before enabling.
            sha256_upload_checksum: false,
        })
    }

    fn new(config: S3CompatibleConfig) -> Result<Self> {
        validate_config(&config)?;
        let endpoint_url = config
            .endpoint_url
            .as_deref()
            .map(|endpoint| {
                object_store_endpoint_url(&config.bucket, endpoint, config.force_path_style)
            })
            .transpose()?;
        let checksum_head_signer = S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            endpoint_url: config.endpoint_url.clone(),
            access_key_id: config.access_key_id.clone(),
            secret_access_key: config.secret_access_key.clone(),
            session_token: config.session_token.clone(),
            key_prefix: config.key_prefix.clone(),
            force_path_style: config.force_path_style,
        })?;

        let io_runtime = StoreIoRuntime::new()?;
        let http = io_runtime
            .connector()
            .connect(&crate::provider_object_store::provider_client_options())
            .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?;
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
        if config.sha256_upload_checksum {
            builder = builder.with_checksum_algorithm(Checksum::SHA256);
        }

        let provider = Arc::new(
            builder
                .build()
                .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?,
        );
        let inner = ProviderObjectStore::new(
            Arc::clone(&provider) as Arc<dyn object_store::ObjectStore>,
            Some(provider),
            ProviderObjectStoreConfig {
                key_prefix: config.key_prefix,
            },
        )?;

        Ok(Self {
            provider_name: config.provider_name,
            inner,
            checksum_head_signer,
            http,
            _io_runtime: io_runtime,
        })
    }

    #[allow(clippy::disallowed_methods)]
    fn checksum_head_signing_time() -> SystemTime {
        // A SigV4 signature is dated, so this one request enters wall time
        // here. Nothing durable is derived from it.
        SystemTime::now()
    }
}

#[async_trait]
impl ObjectStore for S3CompatibleStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.inner.head(key).await
    }

    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        let signed = self.checksum_head_signer.presign_head_stored_checksum(
            key,
            CHECKSUM_HEAD_TTL,
            Self::checksum_head_signing_time(),
        )?;
        let mut builder = http::Request::builder().method("HEAD").uri(&signed.url);
        for (name, value) in &signed.headers {
            builder = builder.header(name, value);
        }
        let request = builder
            .body(HttpRequestBody::empty())
            .map_err(|err| ObjectStoreError::transport(key, err.to_string()))?;
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|err| ObjectStoreError::transport(key, err.to_string()))?;

        let status = response.status();
        if status == http::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status == http::StatusCode::FORBIDDEN || status == http::StatusCode::UNAUTHORIZED {
            return Err(ObjectStoreError::PermissionDenied {
                object_key: key.to_owned(),
                message: format!("provider refused the checksum head with {status}"),
            });
        }
        if !status.is_success() {
            // A HEAD carries no body to quote, so the status is the whole
            // diagnostic the provider gives us.
            return Err(ObjectStoreError::transport(
                key,
                format!("checksum head failed with {status}"),
            ));
        }

        let headers = response.headers();
        let Some(storage_checksum) = s3_stored_checksum(headers) else {
            return Err(ObjectStoreError::transport(
                key,
                "provider reported no full-object checksum for this object".to_owned(),
            ));
        };
        let size_bytes = headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                ObjectStoreError::transport(key, "checksum head reported no content length")
            })?;

        Ok(Some(StoredObjectChecksum {
            size_bytes,
            storage_checksum,
        }))
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        self.inner.get(key, range).await
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        self.inner.list_prefix_stream(prefix)
    }
}

/// Reads whichever full-object checksum the provider stored.
///
/// The checksum *type* header is deliberately not consulted: R2 never sends
/// one, and full-object coverage is established when LoonFS writes the
/// object, not discovered when it reads the metadata back.
fn s3_stored_checksum(headers: &http::HeaderMap) -> Option<StorageChecksum> {
    for (header, algorithm) in S3_CHECKSUM_HEADERS {
        let Some(value) = headers.get(*header).and_then(|value| value.to_str().ok()) else {
            continue;
        };
        let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(value) else {
            continue;
        };
        if raw.len() != algorithm.value_bytes() {
            continue;
        }
        return Some(StorageChecksum {
            algorithm: *algorithm,
            value: hex_encode_bytes(&raw),
        });
    }
    None
}

fn validate_config(config: &S3CompatibleConfig) -> Result<()> {
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
) -> Result<String> {
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
            sha256_upload_checksum: true,
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
