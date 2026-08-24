//! [`S3CompatibleStore`]: the S3-API store, constructed per provider.
//!
//! AWS S3 and Cloudflare R2 differ by addressing, credentials, and whether
//! uploads carry a client-computed checksum -- not by behaviour worth a type
//! each, so both are constructors here.

use crate::aws_credentials::{
    aws_credentials_source, static_aws_credentials_source, ObjectStoreAwsCredentialProvider,
    SharedAwsCredentialsSource,
};
use crate::keyspace::parse_endpoint_url;
use crate::object_store::Result;
use crate::presign::PresignedUrl;
use crate::presign::{
    S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES, CHECKSUM_HEAD_TTL,
    CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
};
use crate::store_io_runtime::StoreIoRuntime;
use crate::{
    ByteRange, ByteStream, MultipartCompletion, MultipartPart, ObjectBody, ObjectMetadata,
    ObjectStore, ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig, PutMode,
    StoredObjectChecksum,
};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::{wire::hex::hex_encode_bytes, SecretString};
use loonfs_api::{Checksum, ChecksumAlgorithm};
use object_store::aws::{AmazonS3Builder, Checksum as ProviderChecksum};
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Lifetime of the internally signed multipart control requests. Like the
/// checksum head, each is issued immediately and never handed out.
const MULTIPART_CONTROL_TTL: Duration = Duration::from_secs(60);

/// Provider checksum headers this adapter understands, in the order it
/// prefers them, paired with the durable algorithm each one names.
const S3_CHECKSUM_HEADERS: &[(&str, ChecksumAlgorithm)] = &[
    ("x-amz-checksum-sha256", ChecksumAlgorithm::Sha256),
    ("x-amz-checksum-crc64nvme", ChecksumAlgorithm::Crc64nvme),
    ("x-amz-checksum-crc32c", ChecksumAlgorithm::Crc32c),
];

/// Supplies credentials, addressing, and key scoping for AWS S3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsS3StoreConfig {
    /// Bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Signing region passed to the S3 client and presigner.
    pub region: String,
    /// S3-compatible endpoint override, or `None` for the regional AWS endpoint.
    pub endpoint_url: Option<String>,
    /// Credential source shared by provider requests and presigned URLs.
    pub credentials: crate::AwsS3Credentials,
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

#[derive(Debug, Clone)]
struct S3CompatibleConfig {
    provider_name: &'static str,
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    credentials: SharedAwsCredentialsSource,
    key_prefix: Option<String>,
    force_path_style: bool,
    /// Attach a client-computed SHA-256 to every upload so the provider
    /// verifies the bytes on PUT (`x-amz-checksum-sha256`). Enabling it also
    /// gives the provider a stored full-object checksum that
    /// [`ObjectStore::head_stored_checksum`] can read back.
    sha256_upload_checksum: bool,
    /// This provider's documented maximum for a single PUT, carried through
    /// to the signer so a direct-put issuer can advertise it.
    direct_put_max_content_bytes: u64,
}

/// Implements the LoonFS storage contract on an S3-API endpoint.
#[derive(Clone)]
pub struct S3CompatibleStore {
    provider_name: &'static str,
    inner: ProviderObjectStore,
    /// Signs the requests the provider client cannot express: the
    /// `HeadObject` that reads a stored checksum back, and the multipart
    /// control calls that have to carry checksum headers. The SigV4 signer
    /// this crate already owns for direct-put URLs can express all of them.
    request_signer: S3CompatiblePresigner,
    /// Sends those signed requests over the store's own IO runtime and
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
    /// Construction fails for invalid static credentials, bucket, region,
    /// endpoint, key prefix, runtime initialization, or provider-client
    /// configuration. Ambient credential resolution remains lazy.
    pub fn aws_s3(config: AwsS3StoreConfig) -> Result<Self> {
        let credentials = aws_credentials_source(&config.credentials, &config.region)?;
        Self::aws_s3_with_credentials(config, credentials)
    }

    pub(crate) fn aws_s3_with_credentials(
        config: AwsS3StoreConfig,
        credentials: SharedAwsCredentialsSource,
    ) -> Result<Self> {
        Self::new(S3CompatibleConfig {
            provider_name: "aws-s3",
            bucket: config.bucket,
            region: config.region,
            endpoint_url: config.endpoint_url,
            credentials,
            key_prefix: config.key_prefix,
            force_path_style: config.force_path_style,
            sha256_upload_checksum: true,
            direct_put_max_content_bytes: AWS_S3_MAX_DIRECT_PUT_BYTES,
        })
    }

    /// Builds a Cloudflare R2 store with path-style addressing.
    ///
    /// Construction fails for a blank account id or any invalid shared
    /// configuration, including credentials, endpoint, and key prefix.
    pub fn cloudflare_r2(config: CloudflareR2StoreConfig) -> Result<Self> {
        let credentials = static_aws_credentials_source(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
        );
        Self::cloudflare_r2_with_credentials(config, credentials)
    }

    pub(crate) fn cloudflare_r2_with_credentials(
        config: CloudflareR2StoreConfig,
        credentials: SharedAwsCredentialsSource,
    ) -> Result<Self> {
        if config.account_id.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "account id must not be empty".to_owned(),
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
        Self::new(S3CompatibleConfig {
            provider_name: "cloudflare-r2",
            bucket: config.bucket,
            region: "auto".to_owned(),
            endpoint_url: Some(config.endpoint_url),
            credentials,
            key_prefix: config.key_prefix,
            // The configured endpoint is the bucket-less account host; path
            // style makes the client append the bucket. Virtual hosting would
            // use the endpoint verbatim and address keys as buckets.
            force_path_style: true,
            // Measured live 2026-07-31: must stay off. With a checksum
            // algorithm configured, the provider client signs multipart part
            // uploads with the aws-chunked streaming-trailer encoding, and R2
            // answers 501 Not Implemented to every part PUT. Plain PUTs with
            // the header succeed (the first nine conformance assertions pass;
            // the multipart-overwrite and streamed-write assertions fail), but
            // one upstream knob configures both shapes, so it stays off
            // wholesale. Nothing is lost: R2 stores its self-computed
            // CRC-64/NVME, which `head_stored_checksum` reads back, and the
            // presigned direct paths carry their own enforced checksum
            // headers independent of this flag.
            sha256_upload_checksum: false,
            direct_put_max_content_bytes: CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
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
        let request_signer = S3CompatiblePresigner::with_credentials(
            S3PresignerConfig {
                bucket: config.bucket.clone(),
                region: config.region.clone(),
                endpoint_url: config.endpoint_url.clone(),
                key_prefix: config.key_prefix.clone(),
                force_path_style: config.force_path_style,
                direct_put_max_content_bytes: config.direct_put_max_content_bytes,
            },
            Arc::clone(&config.credentials),
        )?;

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
            .with_credentials(Arc::new(ObjectStoreAwsCredentialProvider::new(
                config.credentials,
            )))
            .with_virtual_hosted_style_request(!config.force_path_style);

        if let Some(endpoint_url) = endpoint_url {
            let allow_http = endpoint_url.starts_with("http://");
            builder = builder
                .with_endpoint(endpoint_url)
                .with_allow_http(allow_http);
        }
        if config.sha256_upload_checksum {
            builder = builder.with_checksum_algorithm(ProviderChecksum::SHA256);
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
            request_signer,
            http,
            _io_runtime: io_runtime,
        })
    }

    #[allow(clippy::disallowed_methods)]
    fn signing_time() -> SystemTime {
        // A SigV4 signature is dated, so these internally issued requests
        // enter wall time here. Nothing durable is derived from it.
        SystemTime::now()
    }

    /// Issues one internally signed request and returns its status, headers,
    /// and body.
    ///
    /// Every caller here signs a request the provider client cannot build,
    /// so they all land on the same transport: the store's own HTTP client,
    /// runtime, and timeouts.
    async fn execute_signed(
        &self,
        key: &str,
        signed: PresignedUrl,
        body: HttpRequestBody,
    ) -> Result<SignedResponse> {
        let mut builder = http::Request::builder()
            .method(signed.method.as_str())
            .uri(&signed.url);
        for (name, value) in &signed.headers {
            builder = builder.header(name, value);
        }
        let request = builder
            .body(body)
            .map_err(|err| ObjectStoreError::transport(key, err.to_string()))?;
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|err| ObjectStoreError::retryable_transport(key, err.to_string()))?;

        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .bytes()
            .await
            .map_err(|err| ObjectStoreError::retryable_transport(key, err.to_string()))?;
        Ok(SignedResponse {
            status,
            headers,
            body,
        })
    }
}

/// One internally signed response, read whole.
///
/// The bodies here are small provider control documents — an upload id, or
/// an error — so reading them fully is what makes the S3 family's habit of
/// reporting failures inside a 200 response detectable at all.
struct SignedResponse {
    status: http::StatusCode,
    headers: http::HeaderMap,
    body: bytes::Bytes,
}

impl SignedResponse {
    fn text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    /// Reports the provider's own error code for this response, treating a
    /// success status carrying an error document as the failure it is.
    ///
    /// `CompleteMultipartUpload` may answer 200 and then report a failure in
    /// the body, because the provider holds the connection open while it
    /// assembles the object. Reading only the status would take that for
    /// success.
    fn provider_error_code(&self) -> Option<String> {
        let text = self.text();
        if self.status.is_success() && !text.contains("<Error") {
            return None;
        }
        let code = xml_element(&text, "Code").unwrap_or_else(|| self.status.to_string());
        tracing::debug!(
            provider_status = self.status.as_u16(),
            provider_error_code = code,
            provider_request_id = self
                .headers
                .get("x-amz-request-id")
                .and_then(|value| value.to_str().ok()),
            "S3-compatible provider request failed"
        );
        Some(code)
    }
}

#[async_trait]
impl ObjectStore for S3CompatibleStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.inner.head(key).await
    }

    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        let signed = self
            .request_signer
            .presign_head_stored_checksum(key, CHECKSUM_HEAD_TTL, Self::signing_time())
            .await?;
        let response = self
            .execute_signed(key, signed, HttpRequestBody::empty())
            .await?;

        let status = response.status;
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
            return Err(transport_for_status(
                key,
                status,
                format!("checksum head failed with {status}"),
            ));
        }

        let headers = &response.headers;
        let Some(checksum) = s3_stored_checksum(headers) else {
            return Err(ObjectStoreError::StoredChecksumMissing {
                object_key: key.to_owned(),
            });
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
            checksum,
        }))
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        let signed = self
            .request_signer
            .presign_create_multipart(key, MULTIPART_CONTROL_TTL, Self::signing_time())
            .await?;
        let response = self
            .execute_signed(key, signed, HttpRequestBody::empty())
            .await?;
        if let Some(code) = response.provider_error_code() {
            return Err(multipart_error(key, "create", &code));
        }
        xml_element(&response.text(), "UploadId").ok_or_else(|| {
            ObjectStoreError::transport(key, "multipart create returned no upload id")
        })
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<MultipartCompletion> {
        let signed = self
            .request_signer
            .presign_complete_multipart(
                key,
                provider_upload_id,
                checksum,
                MULTIPART_CONTROL_TTL,
                Self::signing_time(),
            )
            .await?;
        let response = self
            .execute_signed(key, signed, complete_multipart_body(parts)?.into())
            .await?;
        match response.provider_error_code().as_deref() {
            None => Ok(MultipartCompletion::Assembled),
            // The upload is gone, which says nothing about the object: an
            // earlier completion may have consumed it and assembled exactly
            // what was asked for. The caller decides from the object.
            Some("NoSuchUpload") => Ok(MultipartCompletion::UnknownUpload),
            Some(code) => Err(multipart_error(key, "complete", code)),
        }
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        let signed = self
            .request_signer
            .presign_abort_multipart(
                key,
                provider_upload_id,
                MULTIPART_CONTROL_TTL,
                Self::signing_time(),
            )
            .await?;
        let response = self
            .execute_signed(key, signed, HttpRequestBody::empty())
            .await?;
        match response.provider_error_code().as_deref() {
            // Nothing to abandon is the state an abort is trying to reach.
            None | Some("NoSuchUpload") => Ok(()),
            Some(code) => Err(multipart_error(key, "abort", code)),
        }
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

    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        self.inner.put_streamed(key, body, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

/// Reads whichever full-object checksum the provider stored.
///
/// The checksum *type* header is deliberately not consulted: R2 never sends
/// one, and full-object coverage is established when LoonFS writes the
/// object, not discovered when it reads the metadata back.
fn s3_stored_checksum(headers: &http::HeaderMap) -> Option<Checksum> {
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
        return Some(Checksum {
            algorithm: *algorithm,
            value: hex_encode_bytes(&raw),
        });
    }
    None
}

/// Reads the text of the first `<name>` element in a provider document.
///
/// The S3 control documents this crate reads are a handful of flat elements
/// — an upload id, an error code — so a scan finds them without an XML
/// parser, and anything it cannot find is reported as absent rather than
/// guessed at.
fn xml_element(document: &str, name: &str) -> Option<String> {
    let opening = format!("<{name}>");
    let closing = format!("</{name}>");
    let start = document.find(&opening)? + opening.len();
    let end = document[start..].find(&closing)? + start;
    Some(document[start..end].trim().to_owned())
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Builds the part manifest `CompleteMultipartUpload` assembles from.
///
/// The parts are the client's bookkeeping: LoonFS never recorded them, so
/// this document is the only place they exist on the server side, for
/// exactly as long as one request.
fn complete_multipart_body(parts: &[MultipartPart]) -> Result<String> {
    if parts.is_empty() {
        return Err(ObjectStoreError::InvalidContentRef(
            "a multipart upload completes with at least one part".to_owned(),
        ));
    }
    let mut body = String::from("<CompleteMultipartUpload>");
    let mut previous = 0;
    for part in parts {
        if part.part_number <= previous {
            return Err(ObjectStoreError::InvalidContentRef(
                "multipart parts must be listed once each, in ascending part order".to_owned(),
            ));
        }
        previous = part.part_number;
        body.push_str("<Part><PartNumber>");
        body.push_str(&part.part_number.to_string());
        body.push_str("</PartNumber><ETag>");
        body.push_str(&xml_escape(&part.etag));
        body.push_str("</ETag><ChecksumCRC64NVME>");
        body.push_str(&xml_escape(&base64_checksum(&part.checksum)?));
        body.push_str("</ChecksumCRC64NVME></Part>");
    }
    body.push_str("</CompleteMultipartUpload>");
    Ok(body)
}

fn base64_checksum(checksum: &Checksum) -> Result<String> {
    checksum
        .validate()
        .map_err(|error| ObjectStoreError::InvalidContentRef(error.to_string()))?;
    let raw = loonfs_api::wire::hex::hex_decode_bytes(&checksum.value)
        .map_err(|error| ObjectStoreError::InvalidContentRef(error.to_string()))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(raw))
}

fn multipart_error(key: &str, operation: &str, code: &str) -> ObjectStoreError {
    match code {
        "AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch" => {
            ObjectStoreError::PermissionDenied {
                object_key: key.to_owned(),
                message: format!("provider refused multipart {operation}: {code}"),
            }
        }
        "NoSuchBucket" | "NoSuchKey" => ObjectStoreError::NotFound {
            object_key: key.to_owned(),
        },
        "ConditionalRequestConflict" | "PreconditionFailed" => {
            ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            }
        }
        "InternalError" | "RequestTimeout" | "ServiceUnavailable" | "SlowDown" => {
            ObjectStoreError::retryable_transport(
                key,
                format!("multipart {operation} failed: {code}"),
            )
        }
        code => ObjectStoreError::transport(key, format!("multipart {operation} failed: {code}")),
    }
}

fn transport_for_status(
    key: &str,
    status: http::StatusCode,
    message: impl Into<String>,
) -> ObjectStoreError {
    if status == http::StatusCode::REQUEST_TIMEOUT
        || status == http::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        ObjectStoreError::retryable_transport(key, message)
    } else {
        ObjectStoreError::transport(key, message)
    }
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
    use super::{multipart_error, object_store_endpoint_url, AwsS3StoreConfig, S3CompatibleStore};
    use crate::test_support::{aws_environment_lock, isolated_aws_environment};
    use crate::{AwsS3Credentials, ObjectStoreErrorClass};

    #[tokio::test(flavor = "current_thread")]
    async fn ambient_aws_store_construction_does_not_resolve_credentials() {
        let _lock = aws_environment_lock().await;
        let tempdir = tempfile::tempdir().expect("create temporary AWS config directory");
        let _environment = isolated_aws_environment(&tempdir, None);

        S3CompatibleStore::aws_s3(AwsS3StoreConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: Some("http://127.0.0.1:9000".to_owned()),
            credentials: AwsS3Credentials::Ambient {},
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: true,
        })
        .expect("construct ambient AWS store lazily");
    }

    #[test]
    fn multipart_provider_codes_are_classified_at_the_adapter_boundary() {
        for (code, expected) in [
            ("AccessDenied", ObjectStoreErrorClass::PermissionDenied),
            ("NoSuchKey", ObjectStoreErrorClass::NotFound),
            (
                "PreconditionFailed",
                ObjectStoreErrorClass::PreconditionFailed,
            ),
            ("SlowDown", ObjectStoreErrorClass::RetryableTransport),
            ("MalformedXML", ObjectStoreErrorClass::Other),
        ] {
            assert_eq!(
                multipart_error("private-key", "complete", code).class(),
                expected,
                "wrong class for {code}"
            );
        }
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
