//! Google Cloud Storage provider.

#[path = "gcs_assembly.rs"]
mod assembly;

use crate::checksum_header::encode_base64_checksum;
use crate::configured::ConfiguredObjectStoreKind;
use crate::keyspace::{normalize_key_prefix, scope_object_key};
use crate::object_store::Result;
use crate::presign::{
    percent_encode_segment, stored_crc32c, DirectTransferIssuers, GcsV4Presigner, CHECKSUM_HEAD_TTL,
};
use crate::provider_object_store::{
    map_provider_error, AbortUploadOnDrop, CompareToken, MultipartController, PartReader,
    ProviderClient, StoredChecksumReader,
};
use crate::retry::{with_transport_retry, RetryWarnings};
use crate::signed_request::{
    classify_signed_response, send, send_signed, signing_time, SignedResponse,
};
use crate::store_io_runtime::StoreIoRuntime;
use crate::timing::StdMonotonicTimer;
use crate::{
    AssemblySource, GcpGcsCredentials, ObjectMetadata, ObjectStoreError, ProviderObjectStore,
    ProviderObjectStoreConfig,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use http::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, LOCATION};
use loonfs_types::{Checksum, ChecksumAlgorithm, ChecksumDigest};
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody};
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Root of the JSON API object resources.
const GCS_JSON_OBJECTS: &str = "https://storage.googleapis.com/storage/v1/b";

/// Root of the JSON API media and resumable uploads.
const GCS_JSON_UPLOADS: &str = "https://storage.googleapis.com/upload/storage/v1/b";

/// The header that carries an object's checksums, in requests and responses.
const GCS_HASH_HEADER: &str = "x-goog-hash";

/// Supplies a credential source and key scoping for the native Google Cloud Storage adapter.
///
/// Config files spell this as a store table with `kind = "gcp_gcs"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcpGcsStoreConfig {
    /// Bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Credential source used for requests and direct-transfer signing.
    pub credentials: GcpGcsCredentials,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,
}

#[derive(Clone)]
struct GcsRequestSigner {
    request_signer: Arc<GcsV4Presigner>,
    http: HttpClient,
    io_runtime: tokio::runtime::Handle,
    credentials: GcpCredentialProvider,
    bucket: String,
    key_prefix: Option<String>,
    retry_warnings: Arc<RetryWarnings>,
}

/// Builds a native GCS adapter whose compare tokens are object generations.
pub fn gcp_gcs(config: GcpGcsStoreConfig) -> Result<ProviderObjectStore> {
    gcp_gcs_with_issuers(config).map(|(store, _)| store)
}

/// Returns the store and the issuers that sign direct transfers against it.
pub(crate) fn gcp_gcs_with_issuers(
    config: GcpGcsStoreConfig,
) -> Result<(ProviderObjectStore, DirectTransferIssuers)> {
    let request_signer = Arc::new(GcsV4Presigner::new(&config)?);
    let key_prefix = normalize_key_prefix(config.key_prefix.as_deref())?;
    let GcpGcsCredentials::ServiceAccountFile {
        path: service_account_key_path,
    } = &config.credentials;

    let io_runtime = StoreIoRuntime::new()?;
    let http = io_runtime
        .connector()
        .connect(&crate::provider_object_store::provider_client_options())
        .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?;
    let builder = GoogleCloudStorageBuilder::new()
        .with_http_connector(io_runtime.connector())
        .with_client_options(crate::provider_object_store::provider_client_options())
        .with_retry(crate::provider_object_store::provider_retry_config())
        .with_bucket_name(config.bucket.clone())
        .with_service_account_path(service_account_key_path);

    let provider = Arc::new(
        builder
            .clone()
            .build()
            .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?,
    );
    let credentials = Arc::clone(provider.credentials());
    let one_attempt = builder
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            ..crate::provider_object_store::provider_retry_config()
        })
        .with_credentials(Arc::clone(&credentials))
        .build()
        .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?;
    let retry_warnings = Arc::new(RetryWarnings::new(Arc::new(StdMonotonicTimer::default())));
    let signer = Arc::new(GcsRequestSigner {
        request_signer: Arc::clone(&request_signer),
        http,
        io_runtime: io_runtime.handle(),
        credentials,
        bucket: config.bucket,
        key_prefix,
        retry_warnings: Arc::clone(&retry_warnings),
    });
    let store = ProviderObjectStore::new(
        Arc::clone(&provider) as Arc<dyn object_store::ObjectStore>,
        Arc::new(one_attempt),
        provider,
        ProviderObjectStoreConfig {
            key_prefix: config.key_prefix,
        },
        ConfiguredObjectStoreKind::GcpGcs,
        ChecksumAlgorithm::Crc32c,
        io_runtime,
        signer.clone(),
        signer,
    )?;
    let direct_transfers = DirectTransferIssuers {
        get: request_signer.clone(),
        put: Some(request_signer),
        multipart: None,
    };

    Ok((
        store
            .compare_token(CompareToken::Generation)
            .retry_warnings(retry_warnings),
        direct_transfers,
    ))
}

impl GcsRequestSigner {
    /// The JSON API path segment naming `key`'s object.
    fn object_name(&self, key: &str) -> Result<String> {
        scope_object_key(self.key_prefix.as_deref(), key).map(|name| percent_encode_segment(&name))
    }

    /// Sends one JSON API request under the service account's OAuth token
    /// and fails on any error status.
    async fn send_authorized(
        &self,
        key: &str,
        request: http::request::Builder,
        body: HttpRequestBody,
    ) -> Result<SignedResponse> {
        let credential = self
            .credentials
            .get_credential()
            .await
            .map_err(|error| map_provider_error(key, error, ProviderClient::Retrying))?;
        let request = request.header(AUTHORIZATION, format!("Bearer {}", credential.bearer));
        succeeded(key, send(&self.http, key, request, body).await?)
    }
}

#[async_trait]
impl StoredChecksumReader for GcsRequestSigner {
    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let mut metadata = with_transport_retry(&self.retry_warnings, key, "head", 0, || async {
            let signed =
                self.request_signer
                    .presign_head(key, CHECKSUM_HEAD_TTL, signing_time())?;
            let response = send_signed(&self.http, key, signed, HttpRequestBody::empty()).await?;
            crate::signed_request::metadata_from_signed_head(
                key,
                &response,
                "x-goog-generation",
                stored_crc32c_from_headers,
            )
        })
        .await?;
        if let Some(metadata) = &mut metadata {
            metadata.etag.clone_from(&metadata.version);
        }
        Ok(metadata)
    }
}

#[async_trait]
impl MultipartController for GcsRequestSigner {
    /// Sends the parts through one JSON API resumable upload whose start
    /// includes `ifGenerationMatch=0`, because XML API multipart uploads
    /// refuse preconditions. A resumable upload takes its bytes in order, so
    /// the parts go one at a time whatever the window, and a chunk that fails
    /// with a retryable transport error is sent again at the same offset.
    async fn put_if_absent(
        &self,
        key: &str,
        head: Bytes,
        mut rest: PartReader<'_>,
        _part_window: usize,
    ) -> Result<ObjectMetadata> {
        if rest.exhausted() {
            let checksum = Checksum::crc32c(&head);
            let url = format!(
                "{GCS_JSON_UPLOADS}/{}/o?uploadType=media&ifGenerationMatch=0&name={}",
                percent_encode_segment(&self.bucket),
                self.object_name(key)?
            );
            let request = http::Request::post(url)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(GCS_HASH_HEADER, crc32c_header(&checksum));
            let response = self.send_authorized(key, request, head.into()).await?;
            return gcs_object(key, &response.body);
        }
        let url = format!(
            "{GCS_JSON_UPLOADS}/{}/o?uploadType=resumable&ifGenerationMatch=0&name={}",
            percent_encode_segment(&self.bucket),
            self.object_name(key)?,
        );
        let request = http::Request::post(url).header(CONTENT_TYPE, "application/json");
        let started = match self
            .send_authorized(key, request, Bytes::from_static(b"{}").into())
            .await
        {
            Err(error @ ObjectStoreError::PreconditionFailed { .. }) => {
                while rest.next_part().await?.is_some() {}
                return Err(error);
            }
            result => result?,
        };
        let session = started
            .headers
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                ObjectStoreError::transport(key, "resumable upload returned no session")
            })?
            .to_owned();
        let (http, cancelled, cancelled_key) = (self.http.clone(), session.clone(), key.to_owned());
        let cancel = async move {
            let request = http::Request::delete(cancelled);
            send(&http, &cancelled_key, request, HttpRequestBody::empty())
                .await
                .map(|_| ())
        };
        let mut abort_on_drop =
            AbortUploadOnDrop::new(key, self.io_runtime.clone(), cancel.boxed());

        let mut checksum = ChecksumDigest::new(ChecksumAlgorithm::Crc32c);
        let mut offset = 0;
        let mut chunk = head;
        loop {
            checksum.update(&chunk);
            let end = offset + chunk.len() as u64;
            let last = chunk.is_empty() || rest.exhausted();
            let content_range = match (chunk.is_empty(), last) {
                (true, _) => format!("bytes */{offset}"),
                (false, true) => format!("bytes {offset}-{}/{end}", end - 1),
                (false, false) => format!("bytes {offset}-{}/*", end - 1),
            };
            let full_checksum = last.then(|| {
                std::mem::replace(
                    &mut checksum,
                    ChecksumDigest::new(ChecksumAlgorithm::Crc32c),
                )
                .finish()
            });
            let response = with_transport_retry(
                &self.retry_warnings,
                key,
                "put_chunk",
                chunk.len() as u64,
                || {
                    let mut request = http::Request::put(&session)
                        .header(CONTENT_LENGTH, chunk.len())
                        .header(CONTENT_RANGE, &content_range);
                    if let Some(full) = &full_checksum {
                        request = request.header(GCS_HASH_HEADER, crc32c_header(full));
                    }
                    let chunk = chunk.clone();
                    async move {
                        let response = send(&self.http, key, request, chunk.into()).await?;
                        match response.status.as_u16() {
                            308 => Ok(response),
                            _ => succeeded(key, response),
                        }
                    }
                },
            )
            .await?;
            if response.status.as_u16() != 308 {
                abort_on_drop.disarm();
                return gcs_object(key, &response.body);
            }
            if last || persisted_bytes(&response.headers) != Some(end) {
                return Err(ObjectStoreError::transport(
                    key,
                    "the resumable upload did not keep every byte sent",
                ));
            }
            offset = end;
            chunk = rest.next_part().await?.unwrap_or_default();
        }
    }

    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        self.assemble_objects(key, sources, tail, expected).await
    }
}

/// A write conditioned on a version reads that version's absence as its
/// precondition failing.
fn precondition_failed_when_missing(error: ObjectStoreError) -> ObjectStoreError {
    match error {
        ObjectStoreError::NotFound { object_key } => {
            ObjectStoreError::PreconditionFailed { object_key }
        }
        error => error,
    }
}

fn crc32c_header(checksum: &Checksum) -> String {
    format!("crc32c={}", encode_base64_checksum(checksum))
}

fn succeeded(key: &str, response: SignedResponse) -> Result<SignedResponse> {
    match classify_signed_response(key, response.status, None) {
        Some(error) => Err(error),
        None => Ok(response),
    }
}

/// How many bytes a resumable upload kept, from its `Range: bytes=0-{last}`.
fn persisted_bytes(headers: &http::HeaderMap) -> Option<u64> {
    let last = headers
        .get(http::header::RANGE)?
        .to_str()
        .ok()?
        .strip_prefix("bytes=0-")?;
    last.parse::<u64>().ok()?.checked_add(1)
}

/// The fields of a JSON API object resource that this adapter reads.
#[derive(serde::Deserialize)]
struct GcsObject {
    generation: String,
    size: String,
    crc32c: Option<String>,
}

/// Reads a JSON API object resource: its metadata, with the generation as
/// the compare token, and its stored CRC-32C.
fn gcs_object(key: &str, body: &[u8]) -> Result<ObjectMetadata> {
    let unreadable = || ObjectStoreError::transport(key, "unreadable object resource");
    let object: GcsObject = serde_json::from_slice(body).map_err(|_| unreadable())?;
    let crc32c = object
        .crc32c
        .as_ref()
        .and_then(|value| stored_crc32c(&format!("crc32c={value}")));
    let metadata = ObjectMetadata {
        size_bytes: object.size.parse().map_err(|_| unreadable())?,
        etag: Some(object.generation.clone()),
        version: Some(object.generation),
        last_modified_at_ms: None,
        checksum: crc32c,
    };
    Ok(metadata)
}

/// Finds the stored CRC-32C among a metadata response's hash headers.
///
/// GCS reports its hashes either as one comma-joined `x-goog-hash` value or
/// as a header line per algorithm, and promises no order between them. Every
/// value is searched, so which spelling arrives cannot decide whether the
/// checksum is found — reading only the first header line would miss a
/// crc32c that happened to follow an md5.
fn stored_crc32c_from_headers(headers: &http::HeaderMap) -> Option<Checksum> {
    headers
        .get_all(GCS_HASH_HEADER)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find_map(stored_crc32c)
}

#[cfg(test)]
mod tests {
    use super::{gcp_gcs, gcs_object, stored_crc32c_from_headers, GcpGcsStoreConfig};
    use crate::retry::{RetryWarnings, DEFAULT};
    use crate::test_support::{
        gcs_fixture_service_account_key_file, time_out, RequestFault, TokioTimer,
    };
    use crate::timing::MonotonicTimer;
    use crate::GcpGcsCredentials;
    use crate::{ObjectStore, ObjectStoreError};
    use bytes::Bytes;
    use loonfs_types::Checksum;
    use object_store::client::HttpClient;
    use std::assert_matches;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_json_object_resource_reports_its_generation_and_stored_crc32c() {
        let body = serde_json::json!({
            "kind": "storage#object",
            "generation": "1700000000000001",
            "size": "5",
            "crc32c": "mnG7TA==",
        });
        let metadata = gcs_object("key", body.to_string().as_bytes()).expect("object resource");
        assert_eq!(metadata.etag.as_deref(), Some("1700000000000001"));
        assert_eq!(metadata.version, metadata.etag);
        assert_eq!(metadata.size_bytes, 5);
        assert_eq!(metadata.checksum, Some(Checksum::crc32c(b"hello")));
    }

    type UploadRequest = (String, http::HeaderMap, Bytes);

    #[derive(Debug, Clone, Default)]
    struct UploadRequests(std::sync::Arc<std::sync::Mutex<Vec<UploadRequest>>>);

    #[async_trait::async_trait]
    impl object_store::client::HttpService for UploadRequests {
        async fn call(
            &self,
            request: object_store::client::HttpRequest,
        ) -> Result<object_store::client::HttpResponse, object_store::client::HttpError> {
            use http_body_util::BodyExt as _;
            let (parts, body) = request.into_parts();
            let bytes = body.collect().await?.to_bytes();
            self.0.lock().expect("requests").push((
                parts.uri.to_string(),
                parts.headers.clone(),
                bytes,
            ));
            let response = http::Response::builder();
            let response = if parts
                .uri
                .query()
                .is_some_and(|query| query.contains("alt=media"))
            {
                assert_eq!(parts.headers[http::header::RANGE], "bytes=0-4");
                response.body(Bytes::from_static(b"hello"))
            } else if parts
                .uri
                .query()
                .is_some_and(|query| query.contains("uploadType=resumable"))
            {
                response
                    .header(http::header::LOCATION, "https://upload.invalid/session")
                    .body(Bytes::new())
            } else if parts
                .headers
                .get("content-range")
                .is_some_and(|range| range.to_str().expect("range").ends_with("/*"))
            {
                let range = parts.headers["content-range"].to_str().expect("range");
                let end = range
                    .split('-')
                    .nth(1)
                    .expect("end")
                    .split('/')
                    .next()
                    .expect("length");
                response
                    .status(308)
                    .header(http::header::RANGE, format!("bytes=0-{end}"))
                    .body(Bytes::new())
            } else {
                response.body(Bytes::from_static(
                    br#"{"generation":"1","size":"5","crc32c":"mnG7TA=="}"#,
                ))
            };
            Ok(response
                .expect("response")
                .map(object_store::client::HttpResponseBody::from))
        }
    }

    /// Builds a signer that sends its requests to `http`, with the fixture
    /// service account at `path` and a static token.
    fn test_signer(path: &std::path::Path, http: HttpClient) -> super::GcsRequestSigner {
        use std::sync::Arc;
        super::GcsRequestSigner {
            request_signer: Arc::new(
                super::GcsV4Presigner::new(&GcpGcsStoreConfig {
                    bucket: "bucket".to_owned(),
                    credentials: GcpGcsCredentials::ServiceAccountFile {
                        path: path.display().to_string(),
                    },
                    key_prefix: None,
                })
                .expect("signer"),
            ),
            http,
            io_runtime: tokio::runtime::Handle::current(),
            credentials: Arc::new(object_store::StaticCredentialProvider::new(
                object_store::gcp::GcpCredential {
                    bearer: "test".to_owned(),
                },
            )),
            bucket: "bucket".to_owned(),
            key_prefix: None,
            retry_warnings: Arc::new(RetryWarnings::new(Arc::new(TokioTimer::start()))),
        }
    }

    /// Fails every resumable chunk but the last with one fault and counts
    /// the attempts, and sends every other request to the upload double.
    #[derive(Debug, Clone)]
    struct FaultyChunks {
        inner: UploadRequests,
        fault: RequestFault,
        attempts: std::sync::Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl object_store::client::HttpService for FaultyChunks {
        async fn call(
            &self,
            request: object_store::client::HttpRequest,
        ) -> Result<object_store::client::HttpResponse, object_store::client::HttpError> {
            let partial_chunk = request
                .headers()
                .get(http::header::CONTENT_RANGE)
                .is_some_and(|range| range.to_str().expect("range").ends_with("/*"));
            if !partial_chunk {
                return self.inner.call(request).await;
            }
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let body_bytes = request.body().content_length() as u64;
            match self.fault {
                RequestFault::Status(status) => Ok(http::Response::builder()
                    .status(status)
                    .body(Bytes::from(format!(r#"{{"error":{{"code":{status}}}}}"#)))
                    .expect("fault response")
                    .map(object_store::client::HttpResponseBody::from)),
                RequestFault::Timeout => Err(time_out(body_bytes).await),
                RequestFault::FirstTimeout if attempt == 1 => Err(time_out(body_bytes).await),
                RequestFault::FirstTimeout => self.inner.call(request).await,
            }
        }
    }

    /// Checks that a resumable chunk that fails is sent again at most once
    /// more than the retry count, within one operation deadline and one
    /// attempt bound, that a refused chunk fails at once, and that a chunk
    /// that spends its budget fails the verified write around it without
    /// starting another resumable session.
    #[tokio::test(start_paused = true)]
    async fn a_failing_chunk_is_attempted_under_one_retry_budget() {
        use super::*;
        use futures::{stream, StreamExt};
        let (_directory, path) = gcs_fixture_service_account_key_file("gcs-chunk-retry");
        let store_directory = tempfile::tempdir().expect("store directory");
        let store = crate::local_fs_store::LocalFsStore::new(store_directory.path())
            .expect("store for the verified write's occupied-key check");
        let retried = usize::try_from(DEFAULT.max_retries).expect("retry count") + 1;
        // Four 30 s attempts and their backoffs pass the 120 s deadline.
        for (fault, attempts) in [
            (RequestFault::Status(400), 1),
            (RequestFault::Status(503), retried),
            (RequestFault::Timeout, 4),
        ] {
            let service = FaultyChunks {
                inner: UploadRequests::default(),
                fault,
                attempts: Arc::default(),
            };
            let signer = test_signer(&path, HttpClient::new(service.clone()));
            let bytes = Bytes::from_static(b"hello");
            let clock = TokioTimer::start();
            let error = crate::immutable_write::put_with(
                &store,
                &signer.retry_warnings,
                "key",
                &bytes,
                || {
                    let mut parts =
                        PartReader::new(stream::iter(vec![Ok(bytes.clone())]).boxed(), 3);
                    let signer = &signer;
                    async move {
                        let head = parts.next_part().await?.unwrap_or_default();
                        signer.put_if_absent("key", head, parts, 1).await
                    }
                },
            )
            .await
            .expect_err("every first chunk fails");
            let elapsed_ms = clock.monotonic_now_ms();
            assert_matches!(
                error,
                crate::ImmutableWriteError::Transport { source, .. }
                    if source.kind() == crate::ObjectStoreErrorKind::Other,
                "{fault:?} is reported as non-retryable"
            );
            assert_eq!(
                service.attempts.load(Ordering::SeqCst),
                attempts,
                "{fault:?} attempts"
            );
            let sessions = service
                .inner
                .0
                .lock()
                .expect("requests")
                .iter()
                .filter(|(url, _, _)| url.contains("uploadType=resumable"))
                .count();
            assert_eq!(sessions, 1, "{fault:?} resumable sessions");
            assert!(
                elapsed_ms
                    <= loonfs_types::saturating_duration_ms(
                        crate::PROVIDER_OPERATION_DEADLINE + crate::PROVIDER_ATTEMPT_TIMEOUT
                    ),
                "{fault:?} took {elapsed_ms} ms"
            );
        }
    }

    #[tokio::test]
    async fn media_and_resumable_uploads_send_crc32c_and_chunk_lengths() {
        use super::*;
        use futures::{stream, StreamExt};
        let (_directory, path) = gcs_fixture_service_account_key_file("gcs-checksum");
        for part_bytes in [3, 5, 8] {
            let requests = UploadRequests::default();
            let signer = test_signer(&path, HttpClient::new(requests.clone()));
            let mut parts = PartReader::new(
                stream::iter(vec![
                    Ok(Bytes::from_static(b"he")),
                    Ok(Bytes::from_static(b"llo")),
                ])
                .boxed(),
                part_bytes,
            );
            let head = parts.next_part().await.expect("head").expect("bytes");
            let metadata = signer
                .put_if_absent("key", head, parts, 1)
                .await
                .expect("upload");
            assert_eq!(metadata.checksum, Some(Checksum::crc32c(b"hello")));
            let requests = requests.0.lock().expect("requests");
            assert!(requests[0].0.contains("ifGenerationMatch=0"));
            let (_, headers, _) = requests.last().expect("completion");
            assert_eq!(headers["x-goog-hash"], "crc32c=mnG7TA==");
            if part_bytes <= 5 {
                assert_eq!(requests[0].2, b"{}"[..]);
                for (_, headers, bytes) in requests.iter().skip(1) {
                    assert_eq!(
                        headers[CONTENT_LENGTH].to_str().expect("content length"),
                        bytes.len().to_string()
                    );
                }
            }
            if part_bytes == 5 {
                assert_eq!(headers[CONTENT_LENGTH], "0");
                assert_eq!(headers[CONTENT_RANGE], "bytes */5");
            }
        }
    }

    #[tokio::test]
    async fn small_assemblies_read_sources_and_create_with_one_media_request() {
        use super::*;
        let (_directory, path) = gcs_fixture_service_account_key_file("gcs-assembly");
        let requests = UploadRequests::default();
        let signer = test_signer(&path, HttpClient::new(requests.clone()));
        let expected = Checksum::crc32c(b"hello");
        let key = "namespaces/demo/content/con_0123456789abcdef0123456789abcdef";
        for sources in [
            vec![],
            vec![AssemblySource {
                key: "source".to_owned(),
                range: None,
                checksum: expected.clone(),
                metadata: ObjectMetadata {
                    etag: Some("1".to_owned()),
                    version: Some("1".to_owned()),
                    size_bytes: 5,
                    last_modified_at_ms: None,
                    checksum: Some(expected.clone()),
                },
            }],
        ] {
            requests.0.lock().expect("requests").clear();
            let tail = if sources.is_empty() {
                vec![Bytes::from_static(b"hello")]
            } else {
                vec![]
            };
            let metadata = signer
                .assemble(key, &sources, tail, &expected)
                .await
                .expect("assembly");
            assert_eq!(metadata.checksum, Some(expected.clone()));
            let requests = requests.0.lock().expect("requests");
            assert_eq!(requests.len(), sources.len() + 1);
            if !sources.is_empty() {
                assert!(requests[0].0.contains("alt=media&generation=1"));
            }
            let (url, headers, bytes) = requests.last().expect("create");
            assert!(url.contains("uploadType=media&ifGenerationMatch=0"));
            assert_eq!(headers["x-goog-hash"], "crc32c=mnG7TA==");
            assert_eq!(bytes.as_ref(), b"hello");
        }
    }

    #[tokio::test]
    async fn invalid_keys_are_rejected_before_generation_tokens() {
        let (_key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("gcs-invalid-key");
        let store = gcp_gcs(GcpGcsStoreConfig {
            bucket: "bucket".to_owned(),
            credentials: GcpGcsCredentials::ServiceAccountFile {
                path: service_account_key_path.display().to_string(),
            },
            key_prefix: None,
        })
        .expect("construct gcs store");

        assert_matches!(
            store
                .compare_and_swap("../escape", "not-a-generation", Bytes::from_static(b"oops"))
                .await,
            Err(ObjectStoreError::InvalidKey { .. })
        );
    }

    #[tokio::test]
    async fn compare_and_swap_rejects_a_non_generation_token() {
        let (_key_dir, service_account_key_path) =
            gcs_fixture_service_account_key_file("gcs-streamed-cas");
        let store = gcp_gcs(GcpGcsStoreConfig {
            bucket: "bucket".to_owned(),
            credentials: GcpGcsCredentials::ServiceAccountFile {
                path: service_account_key_path.display().to_string(),
            },
            key_prefix: None,
        })
        .expect("construct gcs store");
        let error = store
            .compare_and_swap(
                "namespaces/demo/hint.json",
                "not-a-generation",
                Bytes::from_static(b"payload"),
            )
            .await
            .expect_err("non-generation compare token should fail");

        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
    }

    #[test]
    fn service_account_key_path_is_required() {
        assert_matches!(
            gcp_gcs(GcpGcsStoreConfig {
                bucket: "bucket".to_owned(),
                credentials: GcpGcsCredentials::ServiceAccountFile {
                    path: " ".to_owned(),
                },
                key_prefix: None,
            }),
            Err(ObjectStoreError::Configuration(_))
        );
    }

    #[test]
    fn a_service_account_key_that_cannot_sign_stops_the_store_from_being_built() {
        let (key_dir, _key_path) = gcs_fixture_service_account_key_file("gcs-unsignable");
        let unsignable = key_dir.path().join("unsignable.json");
        std::fs::write(
            &unsignable,
            br#"{"client_email":"a@b.iam.gserviceaccount.com","private_key":"private_key","disable_oauth":true}"#,
        )
        .expect("write unsignable service account key");

        assert_matches!(
            gcp_gcs(GcpGcsStoreConfig {
                bucket: "bucket".to_owned(),
                credentials: GcpGcsCredentials::ServiceAccountFile {
                    path: unsignable.display().to_string(),
                },
                key_prefix: None,
            }),
            Err(ObjectStoreError::Configuration(_))
        );
    }

    #[test]
    fn the_stored_crc32c_is_found_however_gcs_spells_its_hash_header() {
        let crc32c_of_hello = "9a71bb4c";
        let md5 = "md5=XUFAKrxLKna5cZ2REBfFkg==";
        let crc32c = "crc32c=mnG7TA==";

        for values in [
            vec![crc32c],
            vec![&format!("{md5},{crc32c}")],
            vec![&format!("{crc32c},{md5}")],
            // A header line per algorithm, with the crc32c second.
            vec![md5, crc32c],
            vec![crc32c, md5],
        ] {
            let mut headers = http::HeaderMap::new();
            for value in &values {
                headers.append(
                    "x-goog-hash",
                    http::HeaderValue::from_str(value).expect("header value"),
                );
            }
            assert_eq!(
                stored_crc32c_from_headers(&headers).map(|checksum| checksum.value.to_string()),
                Some(crc32c_of_hello.to_owned()),
                "crc32c not found in {values:?}"
            );
        }

        // An object GCS describes without a crc32c is described without one.
        let mut md5_only = http::HeaderMap::new();
        md5_only.append(
            "x-goog-hash",
            http::HeaderValue::from_static("md5=XUFAKrxLKna5cZ2REBfFkg=="),
        );
        assert_eq!(stored_crc32c_from_headers(&md5_only), None);
        assert_eq!(stored_crc32c_from_headers(&http::HeaderMap::new()), None);
    }
}
