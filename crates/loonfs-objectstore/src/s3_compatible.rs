//! S3-compatible provider constructors, and the requests this crate signs
//! for them.
//!
//! AWS S3 and Cloudflare R2 differ by addressing, credentials,
//! assembly part sizes, and copy preconditions.

mod assembly;

use crate::assembly::ResolvedSource;
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
    S3CompatiblePresigner, S3PresignerConfig, INTERNAL_REQUEST_TTL, S3_CRC64NVME_CHECKSUM_HEADER,
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
            direct_put_max_content_bytes: config
                .kind
                .direct_put_max_content_bytes()
                .expect("an S3-compatible store signs direct puts"),
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
        // parts held in memory.
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
            .complete_started_upload(key, &upload_id, &parts, Some(&crc))
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
            .complete_upload(key, provider_upload_id, &parts, Some(checksum))
            .await
        {
            Ok(_) => Ok(()),
            // The key is occupied, most often by this upload's own earlier
            // completion, whose response was lost. The caller's
            // stored-checksum check decides whether it's the client's object.
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
    /// the provider no longer knows the upload.
    async fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[UploadedPart],
        checksum: Option<&Checksum>,
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

    /// Completes an upload this signer started and answers the new ETag.
    async fn complete_started_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[UploadedPart],
        checksum: Option<&Checksum>,
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

/// Accepts a data response by its status alone. A success body is the bytes
/// themselves, which may hold anything, so only a failure body is read as
/// the provider's error document. Control responses go through
/// `send_checked`, because the provider can answer those with a success
/// status and an error in the body.
fn checked_data(key: &str, response: SignedResponse) -> Result<SignedResponse> {
    let code = (!response.status.is_success())
        .then(|| response.provider_error_code())
        .flatten();
    ensure_succeeded(key, &response, code.as_deref())?;
    Ok(response)
}

/// Whether the provider reported that it no longer knows the multipart upload.
fn is_unknown_upload(code: Option<&str>) -> bool {
    code == Some("NoSuchUpload")
}

fn s3_stored_checksum(headers: &http::HeaderMap) -> Option<Checksum> {
    let value = headers.get(S3_CRC64NVME_CHECKSUM_HEADER)?.to_str().ok()?;
    decode_base64_checksum(ChecksumAlgorithm::Crc64nvme, value)
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
mod tests {
    use super::{
        aws_s3, checked_data, object_store_endpoint_url, AwsS3StoreConfig, S3RequestSigner,
    };
    use crate::aws_credentials::static_aws_credentials_source;
    use crate::keyspace::KeyScope;
    use crate::presign::{
        base64_crc64nvme, S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES,
    };
    use crate::provider_limits::{PROVIDER_ATTEMPT_TIMEOUT, PROVIDER_OPERATION_DEADLINE};
    use crate::provider_object_store::ProviderParts;
    use crate::retry::{one_attempt_retry_config, provider_retry_config, TransportRetry, DEFAULT};
    use crate::signed_request::{ensure_succeeded, SignedResponse};
    use crate::signed_upload::ProviderSigner;
    use crate::test_support::{
        aws_environment_lock, isolated_aws_environment, time_out, RequestFault,
    };
    use crate::ByteRange;
    use crate::{
        AssemblySource, AwsS3Credentials, ConfiguredObjectStoreKind, ObjectStoreError,
        ObjectStoreErrorKind,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use loonfs_test_support::clock::TokioClock;
    use loonfs_types::MonotonicTimer;
    use loonfs_types::{saturating_duration_ms, Checksum, SecretString};
    use object_store::client::{
        HttpClient, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    };
    use std::assert_matches;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    const KEY: &str = "namespaces/demo/content/con_014d2pf2dbsqqg28t5cy4tqkff";
    const BASE_KEY: &str = "namespaces/demo/content/con_7yvjx9gxjm688fxq5tk1v58cgg";

    #[derive(Debug, Clone)]
    struct ScriptedProvider {
        kind: ConfiguredObjectStoreKind,
        base: Arc<Vec<u8>>,
        operations: Arc<Mutex<Vec<&'static str>>>,
        aborted: Arc<Notify>,
        objects: Arc<Mutex<std::collections::HashMap<String, Bytes>>>,
        parts: Arc<Mutex<Vec<Bytes>>>,
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
                        Some(Bytes::copy_from_slice(&self.base))
                    } else {
                        self.objects.lock().expect("objects").get(request.uri().path()).cloned()
                    };
                    let response = match bytes {
                        Some(bytes) => response
                            .header(http::header::CONTENT_LENGTH, bytes.len())
                            .header(http::header::ETAG, "\"base\"")
                            .header(
                                "x-amz-checksum-crc64nvme",
                                base64_crc64nvme("key", &Checksum::crc64nvme(&bytes)).expect("checksum"),
                            )
                            .body(Bytes::new()),
                        None => response.status(http::StatusCode::NOT_FOUND).body(Bytes::new()),
                    };
                    ("head", response)
                }
                http::Method::GET => {
                    let (first, last) = request.headers()[http::header::RANGE]
                        .to_str().expect("range").strip_prefix("bytes=").expect("bytes")
                        .split_once('-').expect("bounds");
                    let first: usize = first.parse().expect("first");
                    let end = last.parse::<usize>().expect("last") + 1;
                    assert!(end - first <= loonfs_types::usize_from_u64(crate::assembly::READ_BYTES));
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
                    let bytes = Bytes::copy_from_slice(&self.base[range]);
                    let reported = base64_crc64nvme("key", &Checksum::crc64nvme(&bytes))
                        .expect("crc64nvme");
                    self.parts.lock().expect("parts").push(bytes);
                    let checksum = if request.headers().contains_key("x-amz-copy-source-if-match") {
                        format!("<ChecksumCRC64NVME>{reported}</ChecksumCRC64NVME>")
                    } else {
                        String::new()
                    };
                    (
                        "copy",
                        response.body(Bytes::from(format!(
                            "<CopyPartResult><ETag>\"copied\"</ETag>{checksum}</CopyPartResult>"
                        ))),
                    )
                }
                http::Method::PUT if !query.contains("partNumber") => {
                    use http_body_util::BodyExt as _;
                    let (parts, body) = request.into_parts();
                    let bytes = body.collect().await?.to_bytes();
                    let checksum = base64_crc64nvme("key", &Checksum::crc64nvme(&bytes)).expect("checksum");
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
                http::Method::PUT => {
                    use http_body_util::BodyExt as _;
                    let (parts, body) = request.into_parts();
                    let bytes = body.collect().await?.to_bytes();
                    let checksum = base64_crc64nvme("key", &Checksum::crc64nvme(&bytes)).expect("checksum");
                    assert_eq!(parts.headers["x-amz-checksum-crc64nvme"], checksum);
                    self.parts.lock().expect("parts").push(bytes);
                    (
                        "upload",
                        response
                            .header(http::header::ETAG, "\"uploaded\"")
                            .body(Bytes::new()),
                    )
                }
                http::Method::POST => {
                    use http_body_util::BodyExt as _;
                    let (parts, body) = request.into_parts();
                    assert_eq!(parts.headers["if-none-match"], "*");
                    let body = body.collect().await?.to_bytes();
                    let xml = std::str::from_utf8(&body).expect("completion XML");
                    let copied = xml.split("</Part>").next().expect("copied part");
                    let r2 = self.kind == ConfiguredObjectStoreKind::CloudflareR2;
                    assert_eq!(copied.contains("ChecksumCRC64NVME"), !r2);
                    let bytes: Vec<u8> = self.parts.lock().expect("parts").iter()
                        .flat_map(|part| part.iter().copied()).collect();
                    let mut objects = self.objects.lock().expect("objects");
                    if objects.contains_key(parts.uri.path()) {
                        (
                            "complete_refused",
                            response.status(http::StatusCode::PRECONDITION_FAILED).body(
                                Bytes::from_static(b"<Error><Code>PreconditionFailed</Code></Error>"),
                            ),
                        )
                    } else {
                        objects.insert(parts.uri.path().to_owned(), bytes.into());
                        (
                            "complete",
                            response.body(Bytes::from_static(
                                b"<CompleteMultipartUploadResult><ETag>\"assembled\"</ETag></CompleteMultipartUploadResult>",
                            )),
                        )
                    }
                }
                http::Method::DELETE if !query.contains("uploadId") => {
                    assert!(self.objects.lock().expect("objects").remove(request.uri().path()).is_some());
                    ("delete", response.status(http::StatusCode::NO_CONTENT).body(Bytes::new()))
                }
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

    fn scripted_signer<P: HttpService + Clone>(
        kind: ConfiguredObjectStoreKind,
        provider: &P,
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
            KeyScope::default(),
            static_aws_credentials_source(
                SecretString::new("test-access"),
                SecretString::new("test-secret"),
                None,
            ),
        );
        S3RequestSigner {
            request_signer: Arc::new(request_signer),
            http: HttpClient::new(provider.clone()),
            io_runtime: tokio::runtime::Handle::current(),
            kind,
            retry: Arc::new(TransportRetry::new(Arc::new(TokioClock::start()))),
        }
    }

    /// Builds a store over `provider` as the provider constructor does: a
    /// client that retries under the provider retry configuration, the same
    /// client with no retries for conditional puts, and the signer sharing
    /// the store's retry state and its clock.
    fn scripted_store<P>(
        kind: ConfiguredObjectStoreKind,
        provider: &P,
    ) -> crate::ProviderObjectStore
    where
        P: object_store::client::HttpConnector + HttpService + Clone,
    {
        let builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("test-access")
            .with_secret_access_key("test-secret")
            .with_endpoint("http://provider.invalid")
            .with_allow_http(true)
            .with_checksum_algorithm(object_store::aws::Checksum::CRC64NVME)
            .with_disable_bulk_delete(true)
            .with_http_connector(provider.clone());
        let client = Arc::new(
            builder
                .clone()
                .with_retry(provider_retry_config())
                .build()
                .expect("client"),
        );
        let one_attempt = builder
            .with_retry(one_attempt_retry_config())
            .build()
            .expect("client");
        let signer = Arc::new(scripted_signer(kind, provider));
        let retry = Arc::clone(&signer.retry);
        crate::ProviderObjectStore::new(ProviderParts {
            provider: client,
            one_attempt: Arc::new(one_attempt),
            signer: Arc::clone(&signer) as Arc<dyn ProviderSigner>,
            client_multipart: Some(signer),
            kind,
            key_scope: KeyScope::default(),
            location: "bucket".to_owned(),
            io_runtime: crate::store_io_runtime::StoreIoRuntime::new().expect("runtime"),
            retry,
        })
    }

    fn scripted_provider(kind: ConfiguredObjectStoreKind) -> ScriptedProvider {
        ScriptedProvider {
            kind,
            base: Arc::new(b"source".to_vec()),
            operations: Arc::default(),
            aborted: Arc::default(),
            objects: Arc::default(),
            parts: Arc::default(),
        }
    }

    /// Fails every request of one operation with one fault and counts the
    /// attempts, and sends every other request to the scripted provider.
    #[derive(Debug, Clone)]
    struct FaultyProvider {
        inner: ScriptedProvider,
        operation: &'static str,
        fault: RequestFault,
        attempts: Arc<AtomicUsize>,
    }

    impl object_store::client::HttpConnector for FaultyProvider {
        fn connect(&self, _: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    /// The scripted provider's name for the operation `request` performs.
    fn s3_operation(request: &HttpRequest) -> &'static str {
        let query = request.uri().query().unwrap_or_default();
        match *request.method() {
            http::Method::HEAD => "head",
            http::Method::DELETE if !query.contains("uploadId") => "delete",
            http::Method::PUT if request.headers().contains_key("x-amz-copy-source") => "copy",
            http::Method::PUT if query.contains("partNumber") => "upload",
            http::Method::PUT => "put",
            _ => "other",
        }
    }

    #[async_trait]
    impl HttpService for FaultyProvider {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            if s3_operation(&request) != self.operation {
                return self.inner.call(request).await;
            }
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let body_bytes = request.body().content_length() as u64;
            match self.fault {
                RequestFault::Status(status) => {
                    let code = match status {
                        503 => "ServiceUnavailable",
                        _ => "InvalidRequest",
                    };
                    Ok(http::Response::builder()
                        .status(status)
                        .body(Bytes::from(format!("<Error><Code>{code}</Code></Error>")))
                        .expect("fault response")
                        .map(HttpResponseBody::from))
                }
                RequestFault::Timeout => Err(time_out(body_bytes).await),
                RequestFault::FirstTimeout if attempt == 1 => Err(time_out(body_bytes).await),
                RequestFault::FirstTimeout => self.inner.call(request).await,
            }
        }
    }

    /// Checks that each failing request is attempted at most once more than
    /// the retry count, within one operation deadline and one attempt
    /// bound, that a refused request fails at once wherever its status can
    /// be read, and that every failure is reported as non-retryable.
    ///
    /// The retrying client retries a delete's 5xx responses, and LoonFS
    /// resends a delete whose attempt timed out. LoonFS retries a verified
    /// immutable write, a signed HEAD and a signed part. A part that spends
    /// its budget fails the verified write around it, so the multipart
    /// upload is created once.
    #[tokio::test(start_paused = true)]
    async fn a_failing_request_is_attempted_under_one_retry_budget() {
        use crate::ObjectStore;
        let retried = usize::try_from(DEFAULT.max_retries).expect("retry count") + 1;
        // Four 30 s attempts and their backoffs pass the 120 s deadline.
        let timed_out = 4;
        let failed = Some(ObjectStoreErrorKind::Other);
        for (operation, fault, attempts, outcome) in [
            ("delete", RequestFault::Status(400), 1, failed),
            ("delete", RequestFault::Status(503), retried, failed),
            // The retrying client doesn't resend a timed-out DELETE, because
            // it doesn't mark DELETE idempotent, so LoonFS resends it.
            ("delete", RequestFault::Timeout, timed_out, failed),
            ("delete", RequestFault::FirstTimeout, 2, None),
            // The client that sends a conditional put once reports a 400 as
            // the same opaque error as a 503, so LoonFS retries both.
            ("put", RequestFault::Status(400), retried, failed),
            ("put", RequestFault::Status(503), retried, failed),
            ("put", RequestFault::Timeout, timed_out, failed),
            ("head", RequestFault::Status(400), 1, failed),
            ("head", RequestFault::Status(503), retried, failed),
            ("head", RequestFault::Timeout, timed_out, failed),
            ("upload", RequestFault::Status(400), 1, failed),
            ("upload", RequestFault::Status(503), retried, failed),
            ("upload", RequestFault::Timeout, timed_out, failed),
        ] {
            let kind = ConfiguredObjectStoreKind::AwsS3;
            let provider = FaultyProvider {
                inner: scripted_provider(kind),
                operation,
                fault,
                attempts: Arc::default(),
            };
            let store = scripted_store(kind, &provider);
            if operation == "delete" {
                store
                    .put_if_absent(KEY, Bytes::from_static(b"payload"))
                    .await
                    .expect("seed the deleted object");
            }
            let clock = TokioClock::start();
            let reported = match operation {
                "delete" => store.delete(KEY).await.err().map(|error| error.kind()),
                "head" => store.head(KEY).await.err().map(|error| error.kind()),
                _ => {
                    // A one-part upload, so the part's attempts are the only
                    // faulted requests.
                    let (store, payload) = match operation {
                        "put" => (store, Bytes::from_static(b"payload")),
                        _ => (store.multipart_geometry(3, 3), Bytes::from_static(b"one")),
                    };
                    store
                        .put_immutable_verified(KEY, payload)
                        .await
                        .err()
                        .map(|error| error.kind())
                }
            };
            let elapsed_ms = clock.monotonic_now_ms();
            assert_eq!(
                reported, outcome,
                "{operation} {fault:?} outcome (a failure is reported as non-retryable)"
            );
            assert_eq!(
                provider.attempts.load(Ordering::SeqCst),
                attempts,
                "{operation} {fault:?} attempts"
            );
            let creates = provider
                .inner
                .operations()
                .iter()
                .filter(|recorded| **recorded == "create")
                .count();
            assert_eq!(
                creates,
                usize::from(operation == "upload"),
                "{operation} {fault:?} multipart creates"
            );
            let bound = match outcome {
                Some(_) => PROVIDER_OPERATION_DEADLINE + PROVIDER_ATTEMPT_TIMEOUT,
                None => PROVIDER_OPERATION_DEADLINE,
            };
            assert!(
                elapsed_ms <= saturating_duration_ms(bound),
                "{operation} {fault:?} took {elapsed_ms} ms"
            );
        }
    }

    /// The base object as a caller's HEAD reports it to the scripted provider.
    fn base_metadata(provider: &ScriptedProvider) -> crate::ObjectMetadata {
        crate::ObjectMetadata {
            etag: Some("\"base\"".to_owned()),
            version: None,
            size_bytes: provider.base.len() as u64,
            last_modified_at_ms: None,
            checksum: Some(Checksum::crc64nvme(&provider.base)),
        }
    }

    /// Checks that a direct multipart completion is create-only, and that a
    /// completion refused because the key is occupied returns success, so
    /// the caller's stored-checksum check decides.
    ///
    /// The retry of a completion whose response was lost is refused by its
    /// own object, and a completion of other bytes leaves the object alone.
    #[tokio::test]
    async fn a_direct_completion_over_an_occupied_key_leaves_the_object_and_succeeds() {
        use crate::ObjectStore;
        let kind = ConfiguredObjectStoreKind::AwsS3;
        let provider = ScriptedProvider {
            kind,
            base: Arc::new(b"source".to_vec()),
            operations: Arc::default(),
            aborted: Arc::default(),
            objects: Arc::default(),
            parts: Arc::default(),
        };
        let store = scripted_store(kind, &provider);
        let first = Bytes::from_static(b"direct part");
        let first_checksum = Checksum::crc64nvme(&first);
        for (bytes, attempts) in [(first.clone(), 2), (Bytes::from_static(b"other part"), 1)] {
            *provider.parts.lock().expect("parts") = vec![bytes.clone()];
            let checksum = Checksum::crc64nvme(&bytes);
            let parts = [crate::MultipartPart {
                part_number: 1,
                etag: "\"uploaded\"".to_owned(),
                checksum: checksum.clone(),
            }];
            for _ in 0..attempts {
                store
                    .complete_multipart_upload(KEY, "scripted", &parts, &checksum)
                    .await
                    .expect("a completion over an occupied key succeeds");
            }
        }
        assert_eq!(
            provider.operations(),
            ["complete", "complete_refused", "complete_refused"]
        );
        let stored = store.head(KEY).await.expect("head").expect("object");
        assert_eq!(stored.checksum, Some(first_checksum));
        assert_eq!(stored.size_bytes, first.len() as u64);
    }

    #[tokio::test]
    async fn verified_put_and_assembly_accept_the_same_stored_checksum() {
        use crate::{ObjectStore, ObjectStoreError};
        for kind in [
            ConfiguredObjectStoreKind::AwsS3,
            ConfiguredObjectStoreKind::CloudflareR2,
        ] {
            let provider = ScriptedProvider {
                kind,
                base: Arc::new(b"source".to_vec()),
                operations: Arc::default(),
                aborted: Arc::default(),
                objects: Arc::default(),
                parts: Arc::default(),
            };
            let store = scripted_store(kind, &provider);
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
                metadata: base_metadata(&provider),
            }];
            let accepted = store
                .assemble(KEY, &sources, vec![Bytes::from_static(b" tail")], &expected)
                .await
                .expect("same object");
            assert_eq!(accepted.checksum, Some(expected));
            assert_matches!(
                store
                    .assemble(
                        KEY,
                        &sources,
                        vec![Bytes::from_static(b" other")],
                        &Checksum::crc64nvme(b"source other")
                    )
                    .await,
                Err(ObjectStoreError::DifferentObject { object_key }) if object_key == KEY
            );
            // Each assembly learns of the occupied key from its create, and
            // then checks the stored checksum with one HEAD.
            assert_eq!(
                provider.operations(),
                ["put", "get", "put", "head", "get", "put", "head"]
            );
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
                    kind,
                    base: Arc::new(base),
                    operations: Arc::default(),
                    aborted: Arc::default(),
                    objects: Arc::default(),
                    parts: Arc::default(),
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
                        metadata: base_metadata(&provider),
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
                    assert_eq!(provider.operations(), ["get", "get", "put"]);
                } else {
                    assert_matches!(result, Err(ObjectStoreError::ChecksumMismatch { .. }));
                    assert_eq!(provider.operations(), ["get"]);
                }
            }
        }
    }

    #[tokio::test]
    async fn assemblies_verify_copied_bytes_before_accepting_the_object() {
        for kind in [
            ConfiguredObjectStoreKind::AwsS3,
            ConfiguredObjectStoreKind::CloudflareR2,
        ] {
            let length = if kind == ConfiguredObjectStoreKind::CloudflareR2 {
                64 * 1024 * 1024
            } else {
                loonfs_types::usize_from_u64(crate::assembly::READ_BYTES)
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
                    kind,
                    base: Arc::new(base.clone()),
                    operations: Arc::default(),
                    aborted: Arc::default(),
                    objects: Arc::default(),
                    parts: Arc::default(),
                };
                let signer = scripted_signer(kind, &provider);
                let sources = [
                    AssemblySource {
                        key: BASE_KEY.to_owned(),
                        range: None,
                        checksum: checksum.clone(),
                        metadata: base_metadata(&provider),
                    },
                    AssemblySource {
                        key: BASE_KEY.to_owned(),
                        range: Some(ByteRange {
                            start_inclusive: 0,
                            end_exclusive: 3,
                        }),
                        checksum: Checksum::crc64nvme(&base[..3]),
                        metadata: base_metadata(&provider),
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
                    let mut operations = vec!["create", "copy", "get", "upload", "complete"];
                    if kind == ConfiguredObjectStoreKind::CloudflareR2 {
                        operations.push("head");
                    }
                    assert_eq!(provider.operations(), operations);
                } else {
                    assert_matches!(result, Err(ObjectStoreError::ChecksumMismatch { .. }));
                    let mut operations = vec!["create", "copy", "get", "upload"];
                    if kind == ConfiguredObjectStoreKind::CloudflareR2 {
                        // R2 verifies after completion and leaves the rejected
                        // object for collection; no writer deletes it.
                        operations.extend(["complete", "head"]);
                    } else {
                        provider.aborted.notified().await;
                        operations.push("abort");
                    }
                    assert_eq!(provider.operations(), operations);
                    assert_eq!(
                        provider.objects.lock().expect("objects").is_empty(),
                        kind != ConfiguredObjectStoreKind::CloudflareR2
                    );
                }
            }
        }
    }

    /// Heads `BASE_KEY` the way a fold heads each merged extent, and returns
    /// the sources with the checksum their stored checksums combine to.
    async fn headed_sources(
        store: &crate::ProviderObjectStore,
        count: usize,
    ) -> (Vec<AssemblySource>, Checksum) {
        use crate::ObjectStore;
        let mut sources = Vec::new();
        let mut combined = Checksum::crc64nvme(&[]);
        for _ in 0..count {
            let metadata = store.head(BASE_KEY).await.expect("head").expect("source");
            let checksum = metadata.checksum.clone().expect("stored checksum");
            combined = combined
                .crc_combine(&checksum, metadata.size_bytes)
                .expect("combine");
            sources.push(AssemblySource {
                key: BASE_KEY.to_owned(),
                range: None,
                checksum,
                metadata,
            });
        }
        (sources, combined)
    }

    #[tokio::test]
    async fn a_small_assembly_sends_one_put_after_one_head_and_one_get_per_source() {
        use crate::ObjectStore;
        for kind in [
            ConfiguredObjectStoreKind::AwsS3,
            ConfiguredObjectStoreKind::CloudflareR2,
        ] {
            for count in 0..=3 {
                let provider = ScriptedProvider {
                    kind,
                    base: Arc::new(b"source".to_vec()),
                    operations: Arc::default(),
                    aborted: Arc::default(),
                    objects: Arc::default(),
                    parts: Arc::default(),
                };
                let store = scripted_store(kind, &provider);
                let (sources, combined) = headed_sources(&store, count).await;
                let tail = Bytes::from_static(b"tail");
                let expected = combined
                    .crc_combine(&Checksum::crc64nvme(&tail), tail.len() as u64)
                    .expect("tail");
                let written = store
                    .assemble(KEY, &sources, vec![tail], &expected)
                    .await
                    .expect("assembly");
                assert_eq!(written.checksum, Some(expected));
                // A fresh value is one PUT. A merged source costs the
                // caller's HEAD and one GET, and the destination costs no HEAD.
                let mut operations = vec!["head"; count];
                operations.extend(vec!["get"; count]);
                operations.push("put");
                assert_eq!(provider.operations(), operations, "{count} sources");
            }
        }
    }

    #[tokio::test]
    async fn a_multipart_assembly_checks_its_key_once_before_copying() {
        use crate::ObjectStore;
        let kind = ConfiguredObjectStoreKind::AwsS3;
        let provider = ScriptedProvider {
            kind,
            base: Arc::new(vec![
                b'a';
                loonfs_types::usize_from_u64(
                    crate::assembly::READ_BYTES
                )
            ]),
            operations: Arc::default(),
            aborted: Arc::default(),
            objects: Arc::default(),
            parts: Arc::default(),
        };
        let store = scripted_store(kind, &provider);
        let (sources, combined) = headed_sources(&store, 1).await;
        let tail = Bytes::from_static(b"tail");
        let expected = combined
            .crc_combine(&Checksum::crc64nvme(&tail), tail.len() as u64)
            .expect("tail");
        for operations in [
            vec!["head", "create", "copy", "upload", "complete"],
            vec!["head"],
        ] {
            provider.operations.lock().expect("operations").clear();
            let written = store
                .assemble(KEY, &sources, vec![tail.clone()], &expected)
                .await
                .expect("multipart assembly");
            assert_eq!(written.checksum, Some(expected.clone()));
            assert_eq!(provider.operations(), operations);
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
            ("AccessDenied", ObjectStoreErrorKind::PermissionDenied),
            ("NoSuchKey", ObjectStoreErrorKind::NotFound),
            (
                "PreconditionFailed",
                ObjectStoreErrorKind::PreconditionFailed,
            ),
            ("SlowDown", ObjectStoreErrorKind::RetryableTransport),
            ("MalformedXML", ObjectStoreErrorKind::Other),
        ] {
            let response = SignedResponse {
                status: http::StatusCode::BAD_REQUEST,
                headers: http::HeaderMap::new(),
                body: bytes::Bytes::new(),
            };
            assert_eq!(
                ensure_succeeded("private-key", &response, Some(code))
                    .expect_err("provider error should classify")
                    .kind(),
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
