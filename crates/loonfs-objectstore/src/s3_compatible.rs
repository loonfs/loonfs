//! S3-compatible provider constructors, and the requests this crate signs
//! for them.
//!
//! AWS S3 and Cloudflare R2 differ by addressing, credentials,
//! assembly part sizes, and copy preconditions.

mod assembly;

use crate::assembly::{ResolvedSource, SourceReader};
use crate::aws_credentials::{
    aws_credentials_source, cloudflare_r2_credentials_source, ObjectStoreAwsCredentialProvider,
    SharedAwsCredentialsSource,
};
use crate::checksum_header::decode_base64_checksum;
use crate::clock::wall_now;
use crate::configured::ConfiguredObjectStoreKind;
use crate::endpoint::{parse_endpoint_url, virtual_hosted_authority};
use crate::keyspace::KeyScope;
use crate::object_store::{invalid_request, Result};
use crate::presign::{
    base64_crc64nvme, DirectGetIssuer, DirectMultipartIssuer, DirectTransferIssuers,
    PresignedCopyPartRequest, PresignedGetRequest, PresignedPartRequest, PresignedUrl,
    S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES,
    CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES, INTERNAL_REQUEST_TTL, S3_CRC64NVME_CHECKSUM_HEADER,
    S3_IF_MATCH_HEADER,
};
use crate::provider_limits::{provider_client_options, PROVIDER_MAX_MULTIPART_PARTS};
use crate::provider_object_store::{
    configuration_error, provider_http, ConfiguredProvider, ProviderParts,
};
use crate::retry::{one_attempt_retry_config, provider_retry_config, TransportRetry};
use crate::signed_request::{ensure_succeeded, send_signed, SignedResponse};
use crate::signed_upload::{AbortUploadOnDrop, ClientMultipart, PartReader, ProviderSigner};
use crate::store_io_runtime::StoreIoRuntime;
use crate::{
    AssemblySource, ByteRange, MultipartPart, ObjectMetadata, ObjectStoreError, ProviderObjectStore,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use loonfs_types::{Checksum, ChecksumAlgorithm, ChecksumDigest, StdMonotonicTimer};
use object_store::aws::AmazonS3Builder;
use object_store::client::{HttpClient, HttpRequestBody};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Supplies a credential source, addressing, and key scoping for AWS S3.
///
/// Config files spell this as a store table with `kind = "aws_s3"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsS3StoreConfig {
    /// Bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Signing region passed to the S3 client and presigner.
    pub region: String,
    /// S3-compatible endpoint override, or `None` for the regional AWS endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// Credential source shared by provider requests and presigned URLs.
    pub credentials: crate::AwsS3Credentials,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,
    /// Selects path-style bucket addressing for compatible endpoints that require it.
    ///
    /// The default is virtual-hosted style.
    #[serde(default)]
    pub force_path_style: bool,
}

/// Supplies a credential source and account addressing for Cloudflare R2.
///
/// Config files spell this as a store table with `kind = "cloudflare_r2"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudflareR2StoreConfig {
    /// R2 bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Cloudflare account identity, required even though requests use the explicit endpoint.
    pub account_id: String,
    /// Account-level R2 S3 endpoint. This adapter always uses path-style bucket addressing.
    pub endpoint_url: String,
    /// Credential source, resolved when the store is built.
    pub credentials: crate::CloudflareR2Credentials,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,
}

/// The precondition that a part copy puts on the version of its source
/// object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopySourcePrecondition {
    /// The copy is signed with `x-amz-copy-source-if-match`, so it fails when
    /// the source object changed.
    IfMatch,
    /// The copy is signed without a precondition, for a provider that
    /// doesn't support `x-amz-copy-source-if-match`.
    Unconditional,
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
    direct_put_max_content_bytes: u64,
}

#[derive(Clone)]
struct S3RequestSigner {
    request_signer: Arc<S3CompatiblePresigner>,
    http: HttpClient,
    io_runtime: tokio::runtime::Handle,
    kind: ConfiguredObjectStoreKind,
    retry: Arc<TransportRetry>,
}

/// Builds an AWS S3 store with bounded retries and native CRC-64/NVME checksums.
pub fn aws_s3(config: AwsS3StoreConfig) -> Result<ProviderObjectStore> {
    let credentials = aws_credentials_source(&config.credentials, &config.region)?;
    aws_s3_with_credentials(config, credentials).map(|provider| provider.store)
}

/// Returns the store and the issuers that sign direct transfers against it.
pub(crate) fn aws_s3_with_credentials(
    config: AwsS3StoreConfig,
    credentials: SharedAwsCredentialsSource,
) -> Result<ConfiguredProvider> {
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
    let credentials = cloudflare_r2_credentials_source(&config.credentials)?;
    cloudflare_r2_with_credentials(config, credentials).map(|provider| provider.store)
}

/// Returns the store and the issuers that sign direct transfers against it.
pub(crate) fn cloudflare_r2_with_credentials(
    config: CloudflareR2StoreConfig,
    credentials: SharedAwsCredentialsSource,
) -> Result<ConfiguredProvider> {
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

fn new(config: S3CompatibleConfig) -> Result<ConfiguredProvider> {
    let location = match &config.endpoint_url {
        Some(endpoint) => format!("{endpoint} {}", config.bucket),
        None => config.bucket.clone(),
    };
    let endpoint_url = config
        .endpoint_url
        .as_deref()
        .map(|endpoint| {
            object_store_endpoint_url(&config.bucket, endpoint, config.force_path_style)
        })
        .transpose()?;
    let key_scope = KeyScope::new(config.key_prefix.as_deref())?;
    let request_signer = Arc::new(S3CompatiblePresigner::with_credentials(
        S3PresignerConfig {
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            endpoint_url: config.endpoint_url.clone(),
            key_prefix: config.key_prefix,
            force_path_style: config.force_path_style,
            direct_put_max_content_bytes: config.direct_put_max_content_bytes,
        },
        key_scope.clone(),
        Arc::clone(&config.credentials),
    ));

    let io_runtime = StoreIoRuntime::new()?;
    let http = provider_http(&io_runtime)?;
    let mut builder = AmazonS3Builder::new()
        .with_checksum_algorithm(object_store::aws::Checksum::CRC64NVME)
        .with_http_connector(io_runtime.connector())
        .with_client_options(provider_client_options())
        .with_retry(provider_retry_config())
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

    let provider = Arc::new(builder.clone().build().map_err(configuration_error)?);
    let one_attempt = builder
        .with_retry(one_attempt_retry_config())
        .build()
        .map_err(configuration_error)?;
    let retry = Arc::new(TransportRetry::new(Arc::new(StdMonotonicTimer::default())));
    let signer = Arc::new(S3RequestSigner {
        request_signer: Arc::clone(&request_signer),
        http,
        io_runtime: io_runtime.handle(),
        kind: config.kind,
        retry: Arc::clone(&retry),
    });
    let store = ProviderObjectStore::new(ProviderParts {
        provider,
        one_attempt: Arc::new(one_attempt),
        signer: Arc::clone(&signer) as Arc<dyn ProviderSigner>,
        client_multipart: Some(signer),
        kind: config.kind,
        key_scope,
        location,
        io_runtime,
        retry,
    });
    let direct_transfers = DirectTransferIssuers {
        get: request_signer.clone(),
        put: Some(request_signer.clone()),
        multipart: Some(request_signer),
    };

    Ok(ConfiguredProvider {
        store,
        direct_transfers,
    })
}

impl SignedResponse {
    fn text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    /// Reports the provider's own error code for this response, treating a
    /// success status with an error document as the failure it is.
    ///
    /// `CompleteMultipartUpload` may return 200 and then report a failure in
    /// the body, because the provider keeps the connection open while it
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

struct UploadedPart {
    part_number: u32,
    etag: String,
    checksum: Option<Checksum>,
}

impl From<&MultipartPart> for UploadedPart {
    fn from(part: &MultipartPart) -> Self {
        Self {
            part_number: part.part_number,
            etag: part.etag.clone(),
            checksum: Some(part.checksum.clone()),
        }
    }
}

#[async_trait]
impl ProviderSigner for S3RequestSigner {
    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.retry
            .run(key, "head", 0, || async {
                let signed = self
                    .request_signer
                    .presign_head(key, INTERNAL_REQUEST_TTL, wall_now())
                    .await?;
                let response =
                    send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
                crate::signed_request::metadata_from_signed_head(
                    key,
                    &response,
                    "x-amz-version-id",
                    s3_stored_checksum,
                )
            })
            .await
    }

    /// Sends one create-only PUT whose CRC-64/NVME the provider verifies and
    /// stores.
    async fn create_checked(
        &self,
        key: &str,
        bytes: Bytes,
        checksum: &Checksum,
    ) -> Result<ObjectMetadata> {
        let signed = self
            .request_signer
            .presign_create_checked(key, checksum, INTERNAL_REQUEST_TTL, wall_now())
            .await?;
        let size_bytes = bytes.len() as u64;
        let response = self.send_checked(key, signed, bytes.into()).await?;
        let etag = response
            .headers
            .get(http::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        Ok(ObjectMetadata {
            etag,
            version: None,
            size_bytes,
            last_modified_at_ms: None,
            checksum: Some(checksum.clone()),
        })
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
        let mut crc = ChecksumDigest::new(ChecksumAlgorithm::Crc64nvme);
        let mut size_bytes = 0;
        let payloads = stream::once(async { Ok(head) })
            .chain(stream::try_unfold(rest, |mut rest| async move {
                Ok::<_, ObjectStoreError>(rest.next_part().await?.map(|payload| (payload, rest)))
            }))
            .boxed();
        // Parts are cut, counted, and checksummed in order, and the next one
        // is cut only while the window has room, so the window bounds the
        // parts kept in memory.
        let mut parts: Vec<UploadedPart> = payloads
            .enumerate()
            .map(|(index, payload)| {
                let payload = payload?;
                if index >= PROVIDER_MAX_MULTIPART_PARTS as usize {
                    return Err(ObjectStoreError::transport(
                        key,
                        format!(
                            "payload needs more than the provider's \
                             {PROVIDER_MAX_MULTIPART_PARTS}-part limit at this part size"
                        ),
                    ));
                }
                crc.update(&payload);
                size_bytes += payload.len() as u64;
                let part_number = u32::try_from(index)
                    .expect("a part index below the provider part limit should fit u32")
                    + 1;
                Ok(self.upload_part(key, &upload_id, part_number, payload))
            })
            .try_buffer_unordered(part_window)
            .try_collect()
            .await?;
        parts.sort_unstable_by_key(|part| part.part_number);
        let crc = crc.finish();
        let etag = self
            .complete_started_upload(key, &upload_id, &parts, &crc)
            .await?;
        abort_on_drop.disarm();
        Ok(ObjectMetadata {
            etag: Some(etag),
            version: None,
            size_bytes,
            last_modified_at_ms: None,
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
        self.assemble_parts(key, sources, tail, expected)
            .await
            .map_err(crate::assembly::precondition_failed_when_missing)
    }
}

#[async_trait]
impl ClientMultipart for S3RequestSigner {
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
        let parts: Vec<_> = parts.iter().map(UploadedPart::from).collect();
        match self
            .complete_upload(key, provider_upload_id, &parts, checksum)
            .await
        {
            Ok(_) => Ok(()),
            // The key is occupied, most often by this upload's own earlier
            // completion, whose response was lost. The caller's
            // stored-checksum check determines whether it's the client's object.
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        let signed = self
            .request_signer
            .presign_abort_multipart(key, provider_upload_id, INTERNAL_REQUEST_TTL, wall_now())
            .await?;
        let response = send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
        let code = response.provider_error_code();
        if is_unknown_upload(code.as_deref()) {
            return Ok(());
        }
        ensure_succeeded(key, &response, code.as_deref())
    }
}

impl S3RequestSigner {
    async fn create_upload(&self, key: &str) -> Result<String> {
        let signed = self
            .request_signer
            .presign_create_multipart(key, INTERNAL_REQUEST_TTL, wall_now())
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
    ) -> Result<UploadedPart> {
        let checksum = Checksum::crc64nvme(&payload);
        let etag = self
            .retry
            .run(key, "put_part", payload.len() as u64, || async {
                let signed = self
                    .request_signer
                    .presign_multipart_part(
                        PresignedPartRequest {
                            object_key: key,
                            provider_upload_id: upload_id,
                            part_number,
                            checksum: &checksum,
                            ttl: INTERNAL_REQUEST_TTL,
                        },
                        wall_now(),
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
        Ok(UploadedPart {
            part_number,
            etag,
            checksum: Some(checksum),
        })
    }

    async fn copy_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        source: &ResolvedSource<'_>,
        range: &ByteRange,
    ) -> Result<UploadedPart> {
        let source_etag = source.etag()?;
        let source_etag = match self.kind.copy_source_precondition() {
            CopySourcePrecondition::IfMatch => Some(source_etag.as_str()),
            CopySourcePrecondition::Unconditional => None,
        };
        let response = self
            .retry
            .run(key, "copy_part", 0, || async {
                let signed = self
                    .request_signer
                    .presign_copy_part(
                        PresignedCopyPartRequest {
                            object_key: key,
                            provider_upload_id: upload_id,
                            part_number,
                            source_key: &source.source.key,
                            source_range: range,
                            source_etag,
                            ttl: INTERNAL_REQUEST_TTL,
                        },
                        wall_now(),
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
            .and_then(|value| decode_base64_checksum(ChecksumAlgorithm::Crc64nvme, &value));
        if self.kind.copy_part_reports_checksum() && checksum.is_none() {
            return Err(ObjectStoreError::transport(
                key,
                "part copy returned no crc64nvme",
            ));
        }
        Ok(UploadedPart {
            part_number,
            etag,
            checksum,
        })
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
        ensure_succeeded(key, &response, code.as_deref())?;
        Ok(response)
    }

    /// Sends a create-only `CompleteMultipartUpload`, returning `None` when
    /// the provider no longer has the upload.
    async fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[UploadedPart],
        checksum: &Checksum,
    ) -> Result<Option<SignedResponse>> {
        let signed = self
            .request_signer
            .presign_complete_multipart(key, upload_id, checksum, INTERNAL_REQUEST_TTL, wall_now())
            .await?;
        let response = send_signed(
            &self.http,
            key,
            signed,
            complete_multipart_body(key, parts)?.into(),
        )
        .await?;
        let code = response.provider_error_code();
        if is_unknown_upload(code.as_deref()) {
            return Ok(None);
        }
        ensure_succeeded(key, &response, code.as_deref())?;
        Ok(Some(response))
    }

    /// Completes an upload this signer started and returns the new ETag.
    async fn complete_started_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[UploadedPart],
        checksum: &Checksum,
    ) -> Result<String> {
        let response = self
            .complete_upload(key, upload_id, parts, checksum)
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
        AbortUploadOnDrop::new(key, self.io_runtime.clone(), abort.boxed())
    }
}

#[async_trait]
impl SourceReader for S3RequestSigner {
    async fn get_range(&self, key: &str, range: ByteRange, etag: &str) -> Result<Bytes> {
        self.retry
            .run(key, "get_range", 0, || async {
                let mut signed = self
                    .request_signer
                    .presign_get(
                        PresignedGetRequest {
                            object_key: key,
                            range: range.clone(),
                            ttl: INTERNAL_REQUEST_TTL,
                        },
                        wall_now(),
                    )
                    .await?;
                signed
                    .headers
                    .insert(S3_IF_MATCH_HEADER.to_owned(), etag.to_owned());
                let response =
                    send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
                checked_data(key, response).map(|response| response.body)
            })
            .await
    }
}

/// Accepts a data response by its status alone. A success body is the bytes
/// themselves, which may contain anything, so only a failure body is read as
/// the provider's error document. Control responses go through
/// `send_checked`, because the provider can respond to those with a success
/// status and an error in the body.
fn checked_data(key: &str, response: SignedResponse) -> Result<SignedResponse> {
    let code = (!response.status.is_success())
        .then(|| response.provider_error_code())
        .flatten();
    ensure_succeeded(key, &response, code.as_deref())?;
    Ok(response)
}

/// Whether the provider reported that it no longer has the multipart upload.
fn is_unknown_upload(code: Option<&str>) -> bool {
    code == Some("NoSuchUpload")
}

fn s3_stored_checksum(headers: &http::HeaderMap) -> Option<Checksum> {
    let value = headers.get(S3_CRC64NVME_CHECKSUM_HEADER)?.to_str().ok()?;
    decode_base64_checksum(ChecksumAlgorithm::Crc64nvme, value)
}

/// Reads the ETag of a part copy or a completion, whose XML escapes the
/// quotes that the same ETag has in a response header.
fn xml_etag(document: &str) -> Option<String> {
    xml_element(document, "ETag").map(|etag| etag.replace("&quot;", "\""))
}

/// Reads the text of the first `<name>` element in a provider document.
///
/// The S3 control documents this crate reads are a handful of flat elements
/// (such as an upload ID or an error code), so a scan finds them without an XML
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
fn complete_multipart_body(key: &str, parts: &[UploadedPart]) -> Result<String> {
    if parts.is_empty() {
        return Err(invalid_request(
            key,
            "a multipart upload completes with at least one part",
        ));
    }
    let mut body = String::from("<CompleteMultipartUpload>");
    let mut previous = 0;
    for part in parts {
        if part.part_number <= previous {
            return Err(invalid_request(
                key,
                "multipart parts must be listed once each, in ascending part order",
            ));
        }
        previous = part.part_number;
        body.push_str("<Part><PartNumber>");
        body.push_str(&part.part_number.to_string());
        body.push_str("</PartNumber><ETag>");
        body.push_str(&xml_escape(&part.etag));
        body.push_str("</ETag>");
        if let Some(checksum) = &part.checksum {
            body.push_str("<ChecksumCRC64NVME>");
            body.push_str(&xml_escape(&base64_crc64nvme(key, checksum)?));
            body.push_str("</ChecksumCRC64NVME>");
        }
        body.push_str("</Part>");
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
mod tests;
