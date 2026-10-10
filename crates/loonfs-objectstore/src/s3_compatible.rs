//! S3-compatible provider constructors, and the requests this crate signs
//! for them.
//!
//! AWS S3 and Cloudflare R2 differ by addressing, credentials,
//! assembly part sizes, and copy preconditions.

#[path = "s3_assembly.rs"]
mod assembly;

use crate::aws_credentials::{
    aws_credentials_source, static_aws_credentials_source, ObjectStoreAwsCredentialProvider,
    SharedAwsCredentialsSource,
};
use crate::configured::ConfiguredObjectStoreKind;
use crate::endpoint::{parse_endpoint_url, virtual_hosted_authority};
use crate::object_store::Result;
use crate::presign::{
    base64_crc64nvme, DirectGetIssuer, DirectMultipartIssuer, DirectTransferIssuers,
    PresignedGetRequest, PresignedPartRequest, PresignedUrl, S3CompatiblePresigner,
    S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES, CHECKSUM_HEAD_TTL,
    CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
};
use crate::provider_object_store::{
    AbortUploadOnDrop, CompareToken, MultipartController, PartReader, StoredChecksumReader,
    MAX_PROVIDER_MULTIPART_PARTS,
};
use crate::retry::{with_transport_retry, DEFAULT};
use crate::signed_request::{
    classify_signed_response, object_length_from_signed_head, send_signed, SignedResponse,
};
use crate::store_io_runtime::StoreIoRuntime;
use crate::timing::StdMonotonicTimer;
use crate::{
    AssemblySource, ByteRange, MultipartPart, ObjectMetadata, ObjectStoreError,
    ObjectStoreErrorClass, ProviderObjectStore, ProviderObjectStoreConfig, PutMode,
};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use loonfs_types::{format::hex::hex_encode_bytes, SecretString};
use loonfs_types::{Checksum, ChecksumAlgorithm, OperationDeadline, StreamingChecksum};
use object_store::aws::AmazonS3Builder;
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Lifetime of the internally signed multipart control requests. Like the
/// checksum head, each is issued immediately and never handed out.
const MULTIPART_CONTROL_TTL: Duration = Duration::from_secs(60);

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
    kind: ConfiguredObjectStoreKind,
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    credentials: SharedAwsCredentialsSource,
    key_prefix: Option<String>,
    force_path_style: bool,
    /// This provider's documented maximum for a single PUT, carried through
    /// to the signer so a direct-put issuer can advertise it.
    direct_put_max_content_bytes: u64,
}

#[derive(Clone)]
struct S3RequestSigner {
    request_signer: Arc<S3CompatiblePresigner>,
    http: HttpClient,
    kind: ConfiguredObjectStoreKind,
}

/// Builds an AWS S3 store with bounded retries and native CRC-64/NVME checksums.
pub fn aws_s3(config: AwsS3StoreConfig) -> Result<ProviderObjectStore> {
    let credentials = aws_credentials_source(&config.credentials, &config.region)?;
    aws_s3_with_credentials(config, credentials).map(|(store, _)| store)
}

/// Returns the store and the issuers that sign direct transfers against it.
pub(crate) fn aws_s3_with_credentials(
    config: AwsS3StoreConfig,
    credentials: SharedAwsCredentialsSource,
) -> Result<(ProviderObjectStore, DirectTransferIssuers)> {
    new(S3CompatibleConfig {
        kind: ConfiguredObjectStoreKind::AwsS3,
        bucket: config.bucket,
        region: config.region,
        endpoint_url: config.endpoint_url,
        credentials,
        key_prefix: config.key_prefix,
        force_path_style: config.force_path_style,
        direct_put_max_content_bytes: AWS_S3_MAX_DIRECT_PUT_BYTES,
    })
}

/// Builds a Cloudflare R2 store with path-style addressing.
pub fn cloudflare_r2(config: CloudflareR2StoreConfig) -> Result<ProviderObjectStore> {
    let credentials = static_aws_credentials_source(
        config.access_key_id.clone(),
        config.secret_access_key.clone(),
        None,
    );
    cloudflare_r2_with_credentials(config, credentials).map(|(store, _)| store)
}

/// Returns the store and the issuers that sign direct transfers against it.
pub(crate) fn cloudflare_r2_with_credentials(
    config: CloudflareR2StoreConfig,
    credentials: SharedAwsCredentialsSource,
) -> Result<(ProviderObjectStore, DirectTransferIssuers)> {
    new(S3CompatibleConfig {
        kind: ConfiguredObjectStoreKind::CloudflareR2,
        bucket: config.bucket,
        region: "auto".to_owned(),
        endpoint_url: Some(config.endpoint_url),
        credentials,
        key_prefix: config.key_prefix,
        // The configured endpoint is the bucket-less account host; path
        // style makes the client append the bucket. Virtual hosting would
        // use the endpoint verbatim and address keys as buckets.
        force_path_style: true,
        direct_put_max_content_bytes: CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
    })
}

fn new(config: S3CompatibleConfig) -> Result<(ProviderObjectStore, DirectTransferIssuers)> {
    let endpoint_url = config
        .endpoint_url
        .as_deref()
        .map(|endpoint| {
            object_store_endpoint_url(&config.bucket, endpoint, config.force_path_style)
        })
        .transpose()?;
    let request_signer = Arc::new(S3CompatiblePresigner::with_credentials(
        S3PresignerConfig {
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            endpoint_url: config.endpoint_url.clone(),
            key_prefix: config.key_prefix.clone(),
            force_path_style: config.force_path_style,
            direct_put_max_content_bytes: config.direct_put_max_content_bytes,
        },
        Arc::clone(&config.credentials),
    )?);

    let io_runtime = StoreIoRuntime::new()?;
    let http = io_runtime
        .connector()
        .connect(&crate::provider_object_store::provider_client_options())
        .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?;
    let mut builder = AmazonS3Builder::new()
        .with_checksum_algorithm(object_store::aws::Checksum::CRC64NVME)
        .with_http_connector(io_runtime.connector())
        .with_client_options(crate::provider_object_store::provider_client_options())
        .with_retry(crate::provider_object_store::provider_retry_config())
        .with_disable_bulk_delete(true)
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

    let provider = Arc::new(
        builder
            .clone()
            .build()
            .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?,
    );
    let one_attempt = builder
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..crate::provider_object_store::provider_retry_config()
        })
        .build()
        .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?;
    let signer = Arc::new(S3RequestSigner {
        request_signer: Arc::clone(&request_signer),
        http,
        kind: config.kind,
    });
    let store = ProviderObjectStore::new(
        Arc::clone(&provider) as Arc<dyn object_store::ObjectStore>,
        Arc::new(one_attempt),
        Arc::clone(&provider) as Arc<dyn object_store::multipart::MultipartStore>,
        provider,
        ProviderObjectStoreConfig {
            key_prefix: config.key_prefix,
        },
        config.kind,
        io_runtime,
        signer.clone(),
    )?;
    let direct_transfers = DirectTransferIssuers {
        get: request_signer.clone(),
        put: Some(request_signer.clone()),
        multipart: Some(request_signer),
    };

    let store = store
        .compare_token(CompareToken::Etag)
        .multipart_controller(signer);
    Ok((store, direct_transfers))
}

impl S3RequestSigner {
    #[allow(clippy::disallowed_methods)]
    fn signing_time() -> SystemTime {
        // A SigV4 signature is dated, so these internally issued requests
        // enter wall time here. Nothing durable is derived from it.
        SystemTime::now()
    }
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
impl StoredChecksumReader for S3RequestSigner {
    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let signed = self
            .request_signer
            .presign_head(key, CHECKSUM_HEAD_TTL, Self::signing_time())
            .await?;
        let response = send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
        crate::signed_request::metadata_from_signed_head(
            key,
            &response,
            "x-amz-version-id",
            s3_stored_checksum,
        )
    }
}

#[async_trait]
impl MultipartController for S3RequestSigner {
    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        self.create_upload(key).await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<()> {
        self.complete_upload(
            key,
            provider_upload_id,
            parts,
            Some(checksum),
            &PutMode::Overwrite,
        )
        .await
        .map(|_| ())
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
        let response = send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
        let code = response.provider_error_code();
        if code.as_deref() == Some("NoSuchUpload") {
            return Ok(());
        }
        match classify_signed_response(key, response.status, code.as_deref()) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Uploads the parts with up to `part_window` in flight and completes
    /// with `If-None-Match: *`, claiming the CRC-64/NVME of every byte sent.
    async fn put_if_absent(
        &self,
        key: &str,
        head: Bytes,
        rest: PartReader<'_>,
        part_window: usize,
    ) -> Result<ObjectMetadata> {
        let upload_id = self.create_upload(key).await?;
        let mut abort_on_drop = self.abort_on_drop(key, &upload_id);
        let mut crc = StreamingChecksum::for_algorithm(ChecksumAlgorithm::Crc64nvme);
        let mut size_bytes = 0;
        let payloads = stream::once(async { Ok(head) })
            .chain(stream::try_unfold(rest, |mut rest| async move {
                Ok::<_, ObjectStoreError>(rest.next_part().await?.map(|payload| (payload, rest)))
            }))
            .boxed();
        // Parts are cut, counted, and checksummed in order, and the next one
        // is cut only while the window has room, so the window bounds the
        // parts held in memory.
        let mut parts: Vec<MultipartPart> = payloads
            .enumerate()
            .map(|(index, payload)| {
                let payload = payload?;
                if index >= MAX_PROVIDER_MULTIPART_PARTS {
                    return Err(ObjectStoreError::transport(
                        key,
                        format!(
                            "payload needs more than the provider's \
                             {MAX_PROVIDER_MULTIPART_PARTS}-part limit at this part size"
                        ),
                    ));
                }
                crc.update(&payload);
                size_bytes += payload.len() as u64;
                Ok(self.upload_part(key, &upload_id, index as u32 + 1, payload))
            })
            .try_buffer_unordered(part_window)
            .try_collect()
            .await?;
        parts.sort_unstable_by_key(|part| part.part_number);
        let crc = crc.finish();
        let etag = self
            .finish_upload(
                key,
                &upload_id,
                &parts,
                Some(&crc),
                &PutMode::CreateIfAbsent,
            )
            .await?;
        abort_on_drop.disarm();
        Ok(ObjectMetadata {
            etag: Some(etag),
            version: None,
            size_bytes,
            last_modified_ms: None,
            checksum: Some(crc),
        })
    }

    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let assembled = self.assemble_parts(key, sources, tail, expected).await;
        assembled.map_err(|error| match error {
            ObjectStoreError::NotFound { object_key } => {
                ObjectStoreError::PreconditionFailed { object_key }
            }
            error => error,
        })
    }
}

impl S3RequestSigner {
    async fn create_upload(&self, key: &str) -> Result<String> {
        let signed = self
            .request_signer
            .presign_create_multipart(key, MULTIPART_CONTROL_TTL, Self::signing_time())
            .await?;
        let response = self
            .send_checked(key, signed, HttpRequestBody::empty())
            .await?;
        xml_element(&response.text(), "UploadId").ok_or_else(|| {
            ObjectStoreError::transport(key, "multipart create returned no upload id")
        })
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        payload: Bytes,
    ) -> Result<MultipartPart> {
        let checksum = Checksum::crc64nvme(&payload);
        let etag = retry_part(key, "put_part", payload.len() as u64, || async {
            let signed = self
                .request_signer
                .presign_multipart_part(
                    PresignedPartRequest {
                        object_key: key,
                        provider_upload_id: upload_id,
                        part_number,
                        checksum: &checksum,
                        expires_in: MULTIPART_CONTROL_TTL,
                    },
                    Self::signing_time(),
                )
                .await?;
            let response = self
                .send_checked(key, signed, payload.clone().into())
                .await?;
            response
                .headers
                .get(http::header::ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
                .ok_or_else(|| ObjectStoreError::transport(key, "part upload returned no etag"))
        })
        .await?;
        Ok(MultipartPart {
            part_number,
            etag,
            checksum,
        })
    }

    async fn copy_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        source_key: &str,
        source: &ByteRange,
        source_etag: &str,
    ) -> Result<MultipartPart> {
        let source_etag =
            (self.kind != ConfiguredObjectStoreKind::CloudflareR2).then_some(source_etag);
        let response = retry_part(key, "copy_part", 0, || async {
            let signed = self
                .request_signer
                .presign_copy_part(
                    key,
                    upload_id,
                    part_number,
                    source_key,
                    source,
                    source_etag,
                    MULTIPART_CONTROL_TTL,
                    Self::signing_time(),
                )
                .await?;
            self.send_checked(key, signed, HttpRequestBody::empty())
                .await
        })
        .await?;
        let text = response.text();
        let etag = xml_etag(&text)
            .ok_or_else(|| ObjectStoreError::transport(key, "part copy returned no etag"))?;
        let checksum = xml_element(&text, "ChecksumCRC64NVME")
            .and_then(|value| base64_checksum(ChecksumAlgorithm::Crc64nvme, &value))
            .ok_or_else(|| ObjectStoreError::transport(key, "part copy returned no crc64nvme"))?;
        Ok(MultipartPart {
            part_number,
            etag,
            checksum,
        })
    }

    async fn get_range(&self, key: &str, range: &ByteRange, etag: &str) -> Result<Bytes> {
        let mut signed = self
            .request_signer
            .presign_get(
                PresignedGetRequest {
                    object_key: key,
                    range: range.clone(),
                    expires_in: MULTIPART_CONTROL_TTL,
                },
                Self::signing_time(),
            )
            .await?;
        signed
            .headers
            .insert("if-match".to_owned(), etag.to_owned());
        let response = send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
        checked_data(key, response).map(|response| response.body)
    }

    /// The length and ETag of the object at `key`, or `None` when absent.
    async fn head_object(&self, key: &str) -> Result<Option<(u64, String)>> {
        let signed = self
            .request_signer
            .presign_head(key, CHECKSUM_HEAD_TTL, Self::signing_time())
            .await?;
        let response = send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
        object_length_from_signed_head(key, &response)
    }

    /// Sends one signed request and fails on any provider error, including
    /// one reported in the body of a success response.
    async fn send_checked(
        &self,
        key: &str,
        signed: PresignedUrl,
        body: HttpRequestBody,
    ) -> Result<SignedResponse> {
        let response = send_signed(&self.http, key, signed, body).await?;
        let code = response.provider_error_code();
        match classify_signed_response(key, response.status, code.as_deref()) {
            Some(error) => Err(error),
            None => Ok(response),
        }
    }

    /// Sends `CompleteMultipartUpload` under `mode`'s condition, answering
    /// `None` when the provider no longer knows the upload.
    async fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[MultipartPart],
        checksum: Option<&Checksum>,
        mode: &PutMode,
    ) -> Result<Option<SignedResponse>> {
        let signed = self
            .request_signer
            .presign_complete_multipart(
                key,
                upload_id,
                checksum,
                mode,
                MULTIPART_CONTROL_TTL,
                Self::signing_time(),
            )
            .await?;
        let response = send_signed(
            &self.http,
            key,
            signed,
            complete_multipart_body(parts)?.into(),
        )
        .await?;
        let code = response.provider_error_code();
        if code.as_deref() == Some("NoSuchUpload") {
            return Ok(None);
        }
        match classify_signed_response(key, response.status, code.as_deref()) {
            Some(error) => Err(error),
            None => Ok(Some(response)),
        }
    }

    /// Completes an upload this signer started and answers the new ETag.
    async fn finish_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[MultipartPart],
        checksum: Option<&Checksum>,
        mode: &PutMode,
    ) -> Result<String> {
        let response = self
            .complete_upload(key, upload_id, parts, checksum, mode)
            .await?
            .ok_or_else(|| {
                ObjectStoreError::retryable_transport(
                    key,
                    "the provider no longer knows the multipart upload",
                )
            })?;
        xml_etag(&response.text()).ok_or_else(|| {
            ObjectStoreError::transport(key, "multipart completion returned no etag")
        })
    }

    fn abort_on_drop(&self, key: &str, upload_id: &str) -> AbortUploadOnDrop {
        let signer = self.clone();
        let (aborted_key, aborted_id) = (key.to_owned(), upload_id.to_owned());
        let abort = async move {
            signer
                .abort_multipart_upload(&aborted_key, &aborted_id)
                .await
        };
        AbortUploadOnDrop::new(key, abort.boxed())
    }
}

/// Accepts a data response by its status alone. A success body is the bytes
/// themselves, which may hold anything, so only a failure body is read as
/// the provider's error document. Control responses go through
/// `send_checked`, because the provider can answer those with a success
/// status and an error in the body.
fn checked_data(key: &str, response: SignedResponse) -> Result<SignedResponse> {
    let code = (!response.status.is_success())
        .then(|| response.provider_error_code())
        .flatten();
    match classify_signed_response(key, response.status, code.as_deref()) {
        Some(error) => Err(error),
        None => Ok(response),
    }
}

/// Retries one replay-safe part request under its own operation deadline.
async fn retry_part<T, Fut>(
    key: &str,
    operation: &'static str,
    payload_bytes: u64,
    attempt: impl FnMut() -> Fut,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    let timer = StdMonotonicTimer::default();
    let deadline = OperationDeadline::start(&timer, DEFAULT.operation_deadline);
    with_transport_retry(
        &DEFAULT,
        key,
        operation,
        payload_bytes,
        Some(&deadline),
        |error: &ObjectStoreError| error.class() == ObjectStoreErrorClass::RetryableTransport,
        attempt,
    )
    .await
}

/// The CRC-64/NVME of the object that parts assemble to, combined in part
/// order from each part's own checksum and length. `None` when a part's
/// checksum is not a CRC-64/NVME.
fn assembled_crc64nvme<'a>(
    mut parts: impl Iterator<Item = (&'a Checksum, u64)>,
) -> Option<Checksum> {
    let (first, _) = parts.next()?;
    parts.try_fold(first.clone(), |assembled, (next, next_bytes)| {
        assembled.crc_combine(next, next_bytes)
    })
}

fn s3_stored_checksum(headers: &http::HeaderMap) -> Option<Checksum> {
    let value = headers.get("x-amz-checksum-crc64nvme")?.to_str().ok()?;
    base64_checksum(ChecksumAlgorithm::Crc64nvme, value)
}

/// Decodes the base64 spelling the S3 family uses for a checksum.
fn base64_checksum(algorithm: ChecksumAlgorithm, value: &str) -> Option<Checksum> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    (raw.len() == algorithm.value_bytes()).then(|| Checksum {
        algorithm,
        value: hex_encode_bytes(&raw),
    })
}

/// Reads the ETag of a part copy or a completion, whose XML escapes the
/// quotes that the same ETag carries in a response header.
fn xml_etag(document: &str) -> Option<String> {
    xml_element(document, "ETag").map(|etag| etag.replace("&quot;", "\""))
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
        body.push_str(&xml_escape(&base64_crc64nvme(&part.checksum)?));
        body.push_str("</ChecksumCRC64NVME></Part>");
    }
    body.push_str("</CompleteMultipartUpload>");
    Ok(body)
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
    Ok(format!(
        "{}://{}/{}",
        parsed.scheme,
        virtual_hosted_authority(bucket, parsed.authority),
        parsed.path
    )
    .trim_end_matches('/')
    .to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        aws_s3, checked_data, object_store_endpoint_url, static_aws_credentials_source,
        AwsS3StoreConfig, S3RequestSigner,
    };
    use crate::presign::{
        base64_crc64nvme, S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES,
    };
    use crate::provider_object_store::MultipartController;
    use crate::signed_request::{classify_signed_response, SignedResponse};
    use crate::test_support::{aws_environment_lock, isolated_aws_environment};
    use crate::ByteRange;
    use crate::{
        AssemblySource, AwsS3Credentials, ConfiguredObjectStoreKind, ObjectStoreError,
        ObjectStoreErrorClass,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use loonfs_types::{Checksum, SecretString};
    use object_store::client::{
        HttpClient, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    };
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    const KEY: &str = "namespaces/demo/content/con_0123456789abcdef0123456789abcdef";
    const BASE_KEY: &str = "namespaces/demo/content/con_fedcba9876543210fedcba9876543210";

    #[derive(Debug, Clone)]
    struct ScriptedProvider {
        base: Arc<Vec<u8>>,
        operations: Arc<Mutex<Vec<&'static str>>>,
        aborted: Arc<Notify>,
        objects: Arc<Mutex<std::collections::HashMap<String, Bytes>>>,
    }

    impl ScriptedProvider {
        fn operations(&self) -> Vec<&'static str> {
            self.operations.lock().expect("operations").clone()
        }
    }

    impl object_store::client::HttpConnector for ScriptedProvider {
        fn connect(&self, _: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    #[async_trait]
    impl HttpService for ScriptedProvider {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            let query = request.uri().query().unwrap_or_default().to_owned();
            let response = http::Response::builder();
            let (operation, response) = match *request.method() {
                http::Method::HEAD => {
                    assert_eq!(request.headers()["x-amz-checksum-mode"], "ENABLED");
                    let bytes = if request.uri().path().ends_with(BASE_KEY) {
                        Bytes::copy_from_slice(&self.base)
                    } else {
                        self.objects.lock().expect("objects")[request.uri().path()].clone()
                    };
                    let checksum = base64_crc64nvme(&Checksum::crc64nvme(&bytes))
                        .expect("checksum");
                    (
                        "head",
                        response
                            .header(http::header::CONTENT_LENGTH, bytes.len())
                            .header(http::header::ETAG, "\"base\"")
                            .header("x-amz-checksum-crc64nvme", checksum)
                            .body(Bytes::new()),
                    )
                }
                http::Method::GET => {
                    let (first, last) = request.headers()[http::header::RANGE]
                        .to_str().expect("range").strip_prefix("bytes=").expect("bytes")
                        .split_once('-').expect("bounds");
                    let first: usize = first.parse().expect("first");
                    let end = last.parse::<usize>().expect("last") + 1;
                    assert!(end - first <= crate::assembly::READ_BYTES as usize);
                    assert_eq!(request.headers()[http::header::IF_MATCH], "\"base\"");
                    ("get", response.body(Bytes::copy_from_slice(&self.base[first..end])))
                }
                http::Method::POST if query.contains("uploads") => (
                    "create",
                    response.body(Bytes::from_static(
                        b"<InitiateMultipartUploadResult><UploadId>scripted</UploadId></InitiateMultipartUploadResult>",
                    )),
                ),
                http::Method::PUT if request.headers().contains_key("x-amz-copy-source") => {
                    let range = request.headers()["x-amz-copy-source-range"]
                        .to_str()
                        .expect("copy range")
                        .strip_prefix("bytes=")
                        .and_then(|range| range.split_once('-'))
                        .map(|(first, last)| {
                            let first: usize = first.parse().expect("first byte");
                            let last: usize = last.parse().expect("last byte");
                            first..last + 1
                        })
                        .expect("copy range");
                    let reported = base64_crc64nvme(&Checksum::crc64nvme(&self.base[range]))
                        .expect("crc64nvme");
                    (
                        "copy",
                        response.body(Bytes::from(format!(
                            "<CopyPartResult><ETag>\"copied\"</ETag><ChecksumCRC64NVME>{reported}</ChecksumCRC64NVME></CopyPartResult>"
                        ))),
                    )
                }
                http::Method::PUT if !query.contains("partNumber") => {
                    use http_body_util::BodyExt as _;
                    let (parts, body) = request.into_parts();
                    let bytes = body.collect().await?.to_bytes();
                    let checksum = base64_crc64nvme(&Checksum::crc64nvme(&bytes)).expect("checksum");
                    assert_eq!(parts.headers["x-amz-checksum-crc64nvme"], checksum);
                    assert_eq!(parts.headers["if-none-match"], "*");
                    assert!(!parts.headers.keys().any(|name| {
                        name.as_str().starts_with("x-amz-meta-")
                    }));
                    let mut objects = self.objects.lock().expect("objects");
                    let response = if objects.contains_key(parts.uri.path()) {
                        response.status(http::StatusCode::PRECONDITION_FAILED)
                    } else {
                        objects.insert(parts.uri.path().to_owned(), bytes);
                        response.header(http::header::ETAG, "\"written\"")
                    };
                    ("put", response.body(Bytes::new()))
                }
                http::Method::PUT => (
                    "upload",
                    response
                        .header(http::header::ETAG, "\"uploaded\"")
                        .body(Bytes::new()),
                ),
                http::Method::POST => (
                    "complete",
                    response.body(Bytes::from_static(
                        b"<CompleteMultipartUploadResult><ETag>\"assembled\"</ETag></CompleteMultipartUploadResult>",
                    )),
                ),
                http::Method::DELETE => {
                    self.aborted.notify_one();
                    (
                        "abort",
                        response
                            .status(http::StatusCode::NO_CONTENT)
                            .body(Bytes::new()),
                    )
                }
                _ => (
                    "unscripted",
                    response
                        .status(http::StatusCode::NOT_IMPLEMENTED)
                        .body(Bytes::new()),
                ),
            };
            self.operations.lock().expect("operations").push(operation);
            Ok(response
                .expect("scripted response")
                .map(HttpResponseBody::from))
        }
    }

    fn scripted_signer(
        kind: ConfiguredObjectStoreKind,
        provider: &ScriptedProvider,
    ) -> S3RequestSigner {
        let request_signer = S3CompatiblePresigner::with_credentials(
            S3PresignerConfig {
                bucket: "bucket".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint_url: Some("http://provider.invalid".to_owned()),
                key_prefix: None,
                force_path_style: true,
                direct_put_max_content_bytes: AWS_S3_MAX_DIRECT_PUT_BYTES,
            },
            static_aws_credentials_source(
                SecretString::new("test-access"),
                SecretString::new("test-secret"),
                None,
            ),
        )
        .expect("presigner");
        S3RequestSigner {
            request_signer: Arc::new(request_signer),
            http: HttpClient::new(provider.clone()),
            kind,
        }
    }

    #[tokio::test]
    async fn verified_put_and_assembly_accept_the_same_stored_checksum() {
        use crate::{
            ImmutableWriteError, ObjectStore, ProviderObjectStore, ProviderObjectStoreConfig,
        };
        for kind in [
            ConfiguredObjectStoreKind::AwsS3,
            ConfiguredObjectStoreKind::CloudflareR2,
        ] {
            let provider = ScriptedProvider {
                base: Arc::new(b"source".to_vec()),
                operations: Arc::default(),
                aborted: Arc::default(),
                objects: Arc::default(),
            };
            let client = Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name("bucket")
                    .with_region("us-east-1")
                    .with_access_key_id("test-access")
                    .with_secret_access_key("test-secret")
                    .with_endpoint("http://provider.invalid")
                    .with_allow_http(true)
                    .with_checksum_algorithm(object_store::aws::Checksum::CRC64NVME)
                    .with_http_connector(provider.clone())
                    .build()
                    .expect("client"),
            );
            let signer = Arc::new(scripted_signer(kind, &provider));
            let store = ProviderObjectStore::new(
                client.clone(),
                client.clone(),
                client.clone(),
                client,
                ProviderObjectStoreConfig { key_prefix: None },
                kind,
                crate::store_io_runtime::StoreIoRuntime::new().expect("runtime"),
                signer.clone(),
            )
            .expect("store")
            .multipart_controller(signer);
            let bytes = Bytes::from_static(b"source tail");
            let expected = Checksum::crc64nvme(&bytes);
            store
                .put_immutable_verified(KEY, bytes)
                .await
                .expect("verified put");
            let sources = [AssemblySource {
                key: BASE_KEY.to_owned(),
                range: None,
                checksum: Checksum::crc64nvme(b"source"),
            }];
            let accepted = store
                .assemble(KEY, &sources, vec![Bytes::from_static(b" tail")], &expected)
                .await
                .expect("same object");
            assert_eq!(accepted.checksum, Some(expected));
            assert!(matches!(
                store
                    .assemble(
                        KEY,
                        &sources,
                        vec![Bytes::from_static(b" other")],
                        &Checksum::crc64nvme(b"source other")
                    )
                    .await,
                Err(ImmutableWriteError::DifferentObject { .. })
            ));
            assert_eq!(provider.operations(), ["put", "head", "head"]);
        }
    }

    #[tokio::test]
    async fn small_assemblies_read_each_source_once_and_create_in_one_request() {
        for kind in [
            ConfiguredObjectStoreKind::AwsS3,
            ConfiguredObjectStoreKind::CloudflareR2,
        ] {
            for correct in [true, false] {
                let base = vec![b'a'; 3 * 1024 * 1024];
                let checksum = Checksum::crc64nvme(&base);
                let provider = ScriptedProvider {
                    base: Arc::new(base),
                    operations: Arc::default(),
                    aborted: Arc::default(),
                    objects: Arc::default(),
                };
                let signer = scripted_signer(kind, &provider);
                let sources = vec![
                    AssemblySource {
                        key: BASE_KEY.to_owned(),
                        range: None,
                        checksum: if correct {
                            checksum.clone()
                        } else {
                            Checksum::crc64nvme(b"wrong")
                        },
                    };
                    2
                ];
                let tail = Bytes::from_static(b"tail");
                let expected = checksum
                    .crc_combine(&checksum, provider.base.len() as u64)
                    .expect("sources")
                    .crc_combine(&Checksum::crc64nvme(&tail), tail.len() as u64)
                    .expect("tail");
                let result = signer.assemble(KEY, &sources, vec![tail], &expected).await;
                if correct {
                    assert_eq!(result.expect("assembly").checksum, Some(expected));
                    assert_eq!(provider.operations(), ["head", "head", "get", "get", "put"]);
                } else {
                    assert!(matches!(
                        result,
                        Err(ObjectStoreError::ChecksumMismatch { .. })
                    ));
                    assert_eq!(provider.operations(), ["head", "head", "get"]);
                }
            }
        }
    }

    #[tokio::test]
    async fn assembly_checks_copied_parts_before_completing() {
        for kind in [
            ConfiguredObjectStoreKind::AwsS3,
            ConfiguredObjectStoreKind::CloudflareR2,
        ] {
            let length = if kind == ConfiguredObjectStoreKind::CloudflareR2 {
                64 * 1024 * 1024
            } else {
                crate::assembly::READ_BYTES as usize
            };
            let base = vec![b'a'; length];
            let tail = Bytes::from_static(b"tail");
            let checksum = Checksum::crc64nvme(&base);
            let expected = checksum
                .crc_combine(&Checksum::crc64nvme(&base[..3]), 3)
                .expect("small source")
                .crc_combine(&Checksum::crc64nvme(&tail), tail.len() as u64)
                .expect("combined");
            for correct in [true, false] {
                let provider = ScriptedProvider {
                    base: Arc::new(base.clone()),
                    operations: Arc::default(),
                    aborted: Arc::default(),
                    objects: Arc::default(),
                };
                let signer = scripted_signer(kind, &provider);
                let sources = [
                    AssemblySource {
                        key: BASE_KEY.to_owned(),
                        range: None,
                        checksum: checksum.clone(),
                    },
                    AssemblySource {
                        key: BASE_KEY.to_owned(),
                        range: Some(ByteRange {
                            start_inclusive: 0,
                            end_exclusive: 3,
                        }),
                        checksum: Checksum::crc64nvme(&base[..3]),
                    },
                ];
                let claim = if correct {
                    expected.clone()
                } else {
                    Checksum::crc64nvme(b"wrong")
                };
                let result = signer
                    .assemble(
                        KEY,
                        &sources,
                        vec![tail.slice(..2), tail.slice(2..)],
                        &claim,
                    )
                    .await;
                if correct {
                    assert_eq!(result.expect("assembly").checksum, Some(expected.clone()));
                    assert_eq!(
                        provider.operations(),
                        ["head", "head", "create", "copy", "get", "upload", "complete"]
                    );
                } else {
                    assert!(matches!(
                        result,
                        Err(ObjectStoreError::ChecksumMismatch { .. })
                    ));
                    provider.aborted.notified().await;
                    assert_eq!(
                        provider.operations(),
                        ["head", "head", "create", "copy", "get", "upload", "abort"]
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ambient_aws_store_construction_does_not_resolve_credentials() {
        let _lock = aws_environment_lock().await;
        let tempdir = tempfile::tempdir().expect("create temporary AWS config directory");
        let _environment = isolated_aws_environment(&tempdir, None);

        aws_s3(AwsS3StoreConfig {
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
                classify_signed_response("private-key", http::StatusCode::BAD_REQUEST, Some(code),)
                    .expect("provider error should classify")
                    .class(),
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

    #[test]
    fn a_data_body_is_never_read_as_an_error_document() {
        let body = bytes::Bytes::from_static(b"<Error><Code>AccessDenied</Code></Error>");
        let response = |status: u16| SignedResponse {
            status: http::StatusCode::from_u16(status).expect("status"),
            headers: http::HeaderMap::new(),
            body: body.clone(),
        };
        let served = checked_data("namespaces/demo/content/con_1", response(206))
            .expect("a success body is the bytes themselves");
        assert_eq!(served.body, body);
        assert!(matches!(
            checked_data("namespaces/demo/content/con_1", response(403)),
            Err(crate::ObjectStoreError::PermissionDenied { .. })
        ));
    }
}
