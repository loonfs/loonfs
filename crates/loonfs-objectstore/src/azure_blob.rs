//! The native Azure Blob Storage adapter.
//!
//! Reads, listings, deletes, and conditional writes go through the
//! `object_store` client. Creates, block uploads, block copies, and the HEAD
//! that reads the stored checksum are signed here with the account key, so
//! that every write includes the CRC-64 the service verifies, and the whole
//! blob's checksum is recorded where a HEAD reads it back.

mod assembly;

use crate::assembly::{ResolvedSource, SourceReader};
use crate::checksum_header::{
    decode_base64_checksum_little_endian, encode_base64_checksum_little_endian,
};
use crate::configured::ConfiguredObjectStoreKind;
use crate::endpoint::parse_endpoint_url;
use crate::keyspace::KeyScope;
use crate::object_store::Result;
use crate::presign::{percent_encode_path, percent_encode_segment, INTERNAL_REQUEST_TTL};
use crate::provider_error::{map_provider_error, ProviderClient};
use crate::provider_limits::{provider_client_options, PROVIDER_MAX_MULTIPART_PARTS};
use crate::provider_object_store::{
    configuration_error, provider_http, provider_path, ProviderParts,
};
use crate::retry::{one_attempt_retry_config, provider_retry_config, TransportRetry};
use crate::signed_request::{
    ensure_succeeded, metadata_from_signed_head, send_request, with_body, SignedResponse,
};
use crate::signed_upload::{PartReader, ProviderSigner};
use crate::store_io_runtime::StoreIoRuntime;
use crate::{
    AssemblySource, AzureBlobCredentials, ByteRange, ObjectMetadata, ObjectStoreError,
    ProviderObjectStore,
};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use http::header::{CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH, RANGE};
use loonfs_types::{Checksum, ChecksumAlgorithm, ChecksumDigest, StdMonotonicTimer};
use object_store::azure::{
    AzureAuthorizer, AzureCredentialProvider, MicrosoftAzure, MicrosoftAzureBuilder,
};
use object_store::client::{HttpClient, HttpRequestBody};
use object_store::signer::Signer as _;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The most bytes one block can hold, which bounds every copied source range.
pub(crate) const MAX_BLOCK_BYTES: u64 = 4000 * 1024 * 1024;

/// The CRC-64 of a request body, which the service verifies before it
/// stores the body. Azure spells it base64, least significant byte first.
const CONTENT_CRC64_HEADER: &str = "x-ms-content-crc64";
/// The metadata entry that records a blob's whole CRC-64/NVME, spelled as
/// `x-ms-content-crc64` spells one. Azure stores no checksum of its own for
/// a block blob, so the adapter records this one at the commit and reads it
/// back with HEAD.
const STORED_CRC64_HEADER: &str = "x-ms-meta-crc64nvme";
const BLOB_TYPE_HEADER: &str = "x-ms-blob-type";
const COPY_SOURCE_HEADER: &str = "x-ms-copy-source";
const SOURCE_RANGE_HEADER: &str = "x-ms-source-range";
const VERSION_ID_HEADER: &str = "x-ms-version-id";

/// Supplies the account, the container, and key scoping for Azure Blob
/// Storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureBlobStoreConfig {
    /// The storage account that owns the container.
    pub account_name: String,
    /// The container that holds every object.
    pub container: String,
    /// The blob service endpoint, or `None` for the account's endpoint on
    /// `blob.core.windows.net`. A sovereign cloud or Azurite sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// How requests authenticate.
    pub credentials: AzureBlobCredentials,
    /// Optional logical key prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,
}

/// Signs the requests that include the checksum headers the `object_store`
/// client doesn't send.
struct AzureRequestSigner {
    provider: Arc<MicrosoftAzure>,
    http: HttpClient,
    credentials: AzureCredentialProvider,
    account_name: String,
    /// The container's URL, without a trailing slash.
    container_url: String,
    key_scope: KeyScope,
    retry: Arc<TransportRetry>,
}

/// One block of a blob under construction.
struct Block {
    id: String,
    checksum: Checksum,
}

/// Builds a native Azure Blob Storage adapter whose compare tokens are ETags.
pub fn azure_blob(config: AzureBlobStoreConfig) -> Result<ProviderObjectStore> {
    let AzureBlobCredentials::AccountKey { account_key } = &config.credentials;
    let key_scope = KeyScope::new(config.key_prefix.as_deref())?;
    let (endpoint, allow_http) = match &config.endpoint_url {
        Some(endpoint_url) => {
            let endpoint = endpoint_url.trim().trim_end_matches('/').to_owned();
            let allow_http = parse_endpoint_url(&endpoint)?.scheme == "http";
            (endpoint, allow_http)
        }
        None => (
            format!("https://{}.blob.core.windows.net", config.account_name),
            false,
        ),
    };
    let location = match &config.endpoint_url {
        Some(_) => format!("{endpoint} {}", config.container),
        None => format!("{} {}", config.account_name, config.container),
    };

    let io_runtime = StoreIoRuntime::new()?;
    let http = provider_http(&io_runtime)?;
    let builder = MicrosoftAzureBuilder::new()
        .with_http_connector(io_runtime.connector())
        .with_client_options(provider_client_options())
        .with_retry(provider_retry_config())
        .with_account(config.account_name.clone())
        .with_container_name(config.container.clone())
        .with_access_key(account_key.expose())
        .with_endpoint(endpoint.clone())
        .with_allow_http(allow_http);
    let provider = Arc::new(builder.clone().build().map_err(configuration_error)?);
    let credentials = Arc::clone(provider.credentials());
    let one_attempt = builder
        .with_retry(one_attempt_retry_config())
        .with_credentials(Arc::clone(&credentials))
        .build()
        .map_err(configuration_error)?;
    let retry = Arc::new(TransportRetry::new(Arc::new(StdMonotonicTimer::default())));
    let signer = Arc::new(AzureRequestSigner {
        provider: Arc::clone(&provider),
        http,
        credentials,
        account_name: config.account_name,
        container_url: format!("{endpoint}/{}", percent_encode_segment(&config.container)),
        key_scope: key_scope.clone(),
        retry: Arc::clone(&retry),
    });
    Ok(ProviderObjectStore::new(ProviderParts {
        provider,
        one_attempt: Arc::new(one_attempt),
        signer,
        client_multipart: None,
        kind: ConfiguredObjectStoreKind::AzureBlob,
        key_scope,
        location,
        io_runtime,
        retry,
    }))
}

impl AzureRequestSigner {
    /// The URL of the blob at `key`.
    fn blob_url(&self, key: &str) -> Result<String> {
        Ok(format!(
            "{}/{}",
            self.container_url,
            percent_encode_path(&self.key_scope.object_key(key)?)
        ))
    }

    /// The URL that uploads or copies block `id` of the blob at `key`.
    fn block_url(&self, key: &str, id: &str) -> Result<String> {
        Ok(format!(
            "{}?comp=block&blockid={}",
            self.blob_url(key)?,
            percent_encode_segment(id)
        ))
    }

    /// Signs `request` with the account key and sends it.
    async fn send(
        &self,
        key: &str,
        request: http::request::Builder,
        body: HttpRequestBody,
    ) -> Result<SignedResponse> {
        let mut request = with_body(key, request, body)?;
        let credential = self
            .credentials
            .get_credential()
            .await
            .map_err(|err| map_provider_error(key, err, ProviderClient::Retrying))?;
        AzureAuthorizer::new(&credential, &self.account_name)
            .try_authorize(&mut request)
            .map_err(|err| map_provider_error(key, err, ProviderClient::Retrying))?;
        send_request(&self.http, key, request).await
    }

    /// Sends one signed request and fails on any provider error.
    async fn send_checked(
        &self,
        key: &str,
        request: http::request::Builder,
        body: HttpRequestBody,
    ) -> Result<SignedResponse> {
        let response = self.send(key, request, body).await?;
        ensure_succeeded(key, &response, None)?;
        Ok(response)
    }

    /// Uploads `payload` as block `id` of the blob at `key`, under the
    /// CRC-64 the service verifies.
    async fn put_block(&self, key: &str, id: String, payload: Bytes) -> Result<Block> {
        let checksum = Checksum::crc64nvme(&payload);
        let url = self.block_url(key, &id)?;
        let crc64 = encode_base64_checksum_little_endian(&checksum);
        let (url, crc64) = (url.as_str(), crc64.as_str());
        self.retry
            .run(key, "put_part", payload.len() as u64, || {
                let payload = payload.clone();
                async move {
                    self.send_checked(
                        key,
                        http::Request::put(url).header(CONTENT_CRC64_HEADER, crc64),
                        HttpRequestBody::from(payload),
                    )
                    .await
                }
            })
            .await?;
        Ok(Block { id, checksum })
    }

    /// Copies `range` of `source` into block `id` of the blob at `key`, and
    /// returns the CRC-64 the service computed over the copied bytes.
    ///
    /// Put Block From URL takes no credentials for the source, so the source
    /// is named by a short-lived shared access signature of the account's
    /// own. It takes no source precondition either; the combined checksum
    /// of the blocks is checked against the expected one before the commit.
    async fn copy_block(
        &self,
        key: &str,
        id: String,
        source: &ResolvedSource<'_>,
        range: &ByteRange,
    ) -> Result<Block> {
        let source_key = source.source.key.as_str();
        let source_path = provider_path(source_key, self.key_scope.object_key(source_key)?)?;
        let source_range = range
            .http_range_header()
            .expect("an assembly plans no empty block");
        let url = self.block_url(key, &id)?;
        let (url, source_range, source_path) = (url.as_str(), source_range.as_str(), &source_path);
        let response = self
            .retry
            .run(key, "copy_part", 0, || async move {
                let source_url = self
                    .provider
                    .signed_url(http::Method::GET, source_path, INTERNAL_REQUEST_TTL)
                    .await
                    .map_err(|err| map_provider_error(key, err, ProviderClient::Retrying))?;
                self.send_checked(
                    key,
                    http::Request::put(url)
                        .header(COPY_SOURCE_HEADER, source_url.as_str())
                        .header(SOURCE_RANGE_HEADER, source_range),
                    HttpRequestBody::empty(),
                )
                .await
            })
            .await?;
        let checksum = stored_crc64(&response.headers, CONTENT_CRC64_HEADER)
            .ok_or_else(|| ObjectStoreError::transport(key, "block copy returned no crc64"))?;
        Ok(Block { id, checksum })
    }

    /// Commits `blocks` in order as the blob at `key` while `key` is absent,
    /// and records `checksum` as the blob's stored checksum.
    async fn put_block_list(
        &self,
        key: &str,
        blocks: &[Block],
        size_bytes: u64,
        checksum: &Checksum,
    ) -> Result<ObjectMetadata> {
        let url = format!("{}?comp=blocklist", self.blob_url(key)?);
        let body = Bytes::from(block_list_body(blocks));
        let response = self
            .send_checked(
                key,
                http::Request::put(&url)
                    .header(CONTENT_TYPE, "application/xml")
                    .header(IF_NONE_MATCH, "*")
                    .header(
                        STORED_CRC64_HEADER,
                        encode_base64_checksum_little_endian(checksum),
                    ),
                HttpRequestBody::from(body),
            )
            .await?;
        Ok(created_metadata(&response, size_bytes, checksum))
    }
}

#[async_trait]
impl ProviderSigner for AzureRequestSigner {
    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let url = self.blob_url(key)?;
        let url = url.as_str();
        self.retry
            .run(key, "head", 0, || async move {
                let response = self
                    .send(key, http::Request::head(url), HttpRequestBody::empty())
                    .await?;
                metadata_from_signed_head(key, &response, VERSION_ID_HEADER, |headers| {
                    stored_crc64(headers, STORED_CRC64_HEADER)
                })
            })
            .await
    }

    async fn create_checked(
        &self,
        key: &str,
        bytes: Bytes,
        checksum: &Checksum,
    ) -> Result<ObjectMetadata> {
        let url = self.blob_url(key)?;
        let size_bytes = bytes.len() as u64;
        let crc64 = encode_base64_checksum_little_endian(checksum);
        let response = self
            .send_checked(
                key,
                http::Request::put(&url)
                    .header(IF_NONE_MATCH, "*")
                    .header(BLOB_TYPE_HEADER, "BlockBlob")
                    .header(CONTENT_CRC64_HEADER, &crc64)
                    .header(STORED_CRC64_HEADER, &crc64),
                HttpRequestBody::from(bytes),
            )
            .await?;
        Ok(created_metadata(&response, size_bytes, checksum))
    }

    async fn put_if_absent(
        &self,
        key: &str,
        head: Bytes,
        rest: PartReader<'_>,
        part_window: usize,
    ) -> Result<ObjectMetadata> {
        let upload = loonfs_types::generated_id("blk");
        let mut crc = ChecksumDigest::new(ChecksumAlgorithm::Crc64nvme);
        let mut size_bytes = 0;
        let payloads = stream::once(async { Ok(head) })
            .chain(stream::try_unfold(rest, |mut rest| async move {
                Ok::<_, ObjectStoreError>(rest.next_part().await?.map(|payload| (payload, rest)))
            }))
            .boxed();
        // Blocks are cut, counted, and checksummed in order, and the next
        // one is cut only while the window has room, so the window bounds
        // the blocks kept in memory.
        let mut blocks: Vec<(usize, Block)> = payloads
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
                let id = block_id(&upload, index);
                Ok(async move {
                    self.put_block(key, id, payload)
                        .await
                        .map(|block| (index, block))
                })
            })
            .try_buffer_unordered(part_window)
            .try_collect()
            .await?;
        blocks.sort_unstable_by_key(|(index, _)| *index);
        let blocks: Vec<Block> = blocks.into_iter().map(|(_, block)| block).collect();
        self.put_block_list(key, &blocks, size_bytes, &crc.finish())
            .await
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
impl SourceReader for AzureRequestSigner {
    async fn get_range(&self, key: &str, range: ByteRange, etag: &str) -> Result<Bytes> {
        let url = self.blob_url(key)?;
        let range = range
            .http_range_header()
            .expect("an assembly reads no empty range");
        let (url, range) = (url.as_str(), range.as_str());
        self.retry
            .run(key, "get_range", 0, || async move {
                let response = self
                    .send_checked(
                        key,
                        http::Request::get(url)
                            .header(RANGE, range)
                            .header(IF_MATCH, etag),
                        HttpRequestBody::empty(),
                    )
                    .await?;
                Ok(response.body)
            })
            .await
    }
}

/// The ID of block `index` of upload `upload`.
///
/// Every block of one blob needs an ID of one length, and two uploads of one
/// key must not share IDs, or one upload's block list would commit the
/// other's bytes.
fn block_id(upload: &str, index: usize) -> String {
    base64::engine::general_purpose::STANDARD.encode(format!("{upload}-{index:05}"))
}

/// The body of a Put Block List that commits `blocks` in order.
fn block_list_body(blocks: &[Block]) -> String {
    let mut body = String::from(r#"<?xml version="1.0" encoding="utf-8"?><BlockList>"#);
    for block in blocks {
        body.push_str("<Uncommitted>");
        body.push_str(&block.id);
        body.push_str("</Uncommitted>");
    }
    body.push_str("</BlockList>");
    body
}

/// The metadata of the blob that `response` created.
fn created_metadata(
    response: &SignedResponse,
    size_bytes: u64,
    checksum: &Checksum,
) -> ObjectMetadata {
    ObjectMetadata {
        etag: header_value(&response.headers, ETAG),
        version: header_value(&response.headers, VERSION_ID_HEADER),
        size_bytes,
        last_modified_at_ms: None,
        checksum: Some(checksum.clone()),
    }
}

fn header_value(
    headers: &http::HeaderMap,
    name: impl http::header::AsHeaderName,
) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// The CRC-64/NVME that header `name` spells, when `headers` include it.
fn stored_crc64(headers: &http::HeaderMap, name: &str) -> Option<Checksum> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| decode_base64_checksum_little_endian(ChecksumAlgorithm::Crc64nvme, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::CONTENT_LENGTH;
    use loonfs_test_support::clock::TokioClock;
    use object_store::client::{
        HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    };
    use std::assert_matches;
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Mutex;

    /// Azurite's published development account key.
    const EMULATOR_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const ENDPOINT: &str = "http://127.0.0.1:10000/devstoreaccount1";
    const CONTAINER_PATH: &str = "/devstoreaccount1/container";

    #[derive(Debug)]
    struct Blob {
        bytes: Vec<u8>,
        stored_crc64: Option<String>,
        etag: String,
    }

    #[derive(Debug)]
    struct Request {
        method: http::Method,
        path: String,
        query: Option<String>,
        headers: http::HeaderMap,
    }

    /// A blob service that keeps committed blobs and uncommitted blocks, and
    /// verifies what Azure verifies: the CRC-64 of every uploaded body, and
    /// the absence of the blob under `If-None-Match: *`.
    #[derive(Debug, Default)]
    struct BlobState {
        blobs: HashMap<String, Blob>,
        blocks: HashMap<String, HashMap<String, Vec<u8>>>,
        requests: Vec<Request>,
        next_etag: u64,
    }

    #[derive(Debug, Clone, Default)]
    struct BlobService(Arc<Mutex<BlobState>>);

    #[async_trait]
    impl HttpService for BlobService {
        async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
            use http_body_util::BodyExt as _;
            let (parts, body) = request.into_parts();
            let body = body.collect().await?.to_bytes();
            assert!(
                parts.headers.contains_key(http::header::AUTHORIZATION),
                "every request is signed"
            );
            let path = parts.uri.path().to_owned();
            let query = parts.uri.query().map(str::to_owned);
            let mut state = self.0.lock().expect("state");
            state.requests.push(Request {
                method: parts.method.clone(),
                path: path.clone(),
                query: query.clone(),
                headers: parts.headers.clone(),
            });
            let response = state.respond(
                &parts.method,
                &path,
                query.as_deref(),
                &parts.headers,
                &body,
            );
            Ok(response.map(HttpResponseBody::from))
        }
    }

    impl BlobState {
        fn respond(
            &mut self,
            method: &http::Method,
            path: &str,
            query: Option<&str>,
            headers: &http::HeaderMap,
            body: &[u8],
        ) -> http::Response<Bytes> {
            let comp = query_value(query, "comp");
            match (method, comp.as_deref()) {
                (&http::Method::HEAD, None) => match self.blobs.get(path) {
                    Some(blob) => {
                        let mut response = http::Response::builder()
                            .status(200)
                            .header(CONTENT_LENGTH, blob.bytes.len())
                            .header(ETAG, &blob.etag);
                        if let Some(crc64) = &blob.stored_crc64 {
                            response = response.header(STORED_CRC64_HEADER, crc64);
                        }
                        response.body(Bytes::new()).expect("response")
                    }
                    None => status(404),
                },
                (&http::Method::GET, None) => {
                    let Some(blob) = self.blobs.get(path) else {
                        return status(404);
                    };
                    if header(headers, "if-match").is_some_and(|etag| etag != blob.etag) {
                        return status(412);
                    }
                    let (first, last) = parse_range(header(headers, "range").expect("range"));
                    http::Response::builder()
                        .status(206)
                        .header(ETAG, &blob.etag)
                        .body(Bytes::copy_from_slice(&blob.bytes[first..=last]))
                        .expect("response")
                }
                (&http::Method::PUT, Some("block")) => {
                    let id = query_value(query, "blockid").expect("block id");
                    let blocks = self.blocks.entry(path.to_owned()).or_default();
                    if let Some(existing) = blocks.keys().next() {
                        assert_eq!(
                            existing.len(),
                            id.len(),
                            "the block IDs of one blob have one length"
                        );
                    }
                    let bytes = match header(headers, COPY_SOURCE_HEADER) {
                        Some(source) => {
                            let source = http::Uri::try_from(source).expect("source url");
                            assert!(
                                source.query().is_some_and(|query| query.contains("sig=")),
                                "the copy source includes a shared access signature"
                            );
                            let Some(blob) = self.blobs.get(source.path()) else {
                                return status(404);
                            };
                            let (first, last) = parse_range(
                                header(headers, SOURCE_RANGE_HEADER).expect("source range"),
                            );
                            blob.bytes[first..=last].to_vec()
                        }
                        None => {
                            let sent = header(headers, CONTENT_CRC64_HEADER).expect("block crc");
                            if sent
                                != encode_base64_checksum_little_endian(&Checksum::crc64nvme(body))
                            {
                                return status(400);
                            }
                            body.to_vec()
                        }
                    };
                    let crc64 = encode_base64_checksum_little_endian(&Checksum::crc64nvme(&bytes));
                    blocks.insert(id, bytes);
                    http::Response::builder()
                        .status(201)
                        .header(CONTENT_CRC64_HEADER, crc64)
                        .body(Bytes::new())
                        .expect("response")
                }
                (&http::Method::PUT, Some("blocklist")) => {
                    if header(headers, "if-none-match") == Some("*")
                        && self.blobs.contains_key(path)
                    {
                        return status(409);
                    }
                    let blocks = self.blocks.remove(path).unwrap_or_default();
                    let list = std::str::from_utf8(body).expect("xml");
                    let mut bytes = Vec::new();
                    for id in list
                        .split("<Uncommitted>")
                        .skip(1)
                        .map(|rest| rest.split("</Uncommitted>").next().expect("closing tag"))
                    {
                        bytes.extend_from_slice(blocks.get(id).expect("an uploaded block"));
                    }
                    let etag = self.create(path, bytes, header(headers, STORED_CRC64_HEADER));
                    http::Response::builder()
                        .status(201)
                        .header(ETAG, etag)
                        .body(Bytes::new())
                        .expect("response")
                }
                (&http::Method::PUT, None) => {
                    if header(headers, "if-none-match") == Some("*")
                        && self.blobs.contains_key(path)
                    {
                        return status(409);
                    }
                    assert_eq!(header(headers, BLOB_TYPE_HEADER), Some("BlockBlob"));
                    let sent = header(headers, CONTENT_CRC64_HEADER).expect("blob crc");
                    if sent != encode_base64_checksum_little_endian(&Checksum::crc64nvme(body)) {
                        return status(400);
                    }
                    let etag =
                        self.create(path, body.to_vec(), header(headers, STORED_CRC64_HEADER));
                    http::Response::builder()
                        .status(201)
                        .header(ETAG, etag)
                        .body(Bytes::new())
                        .expect("response")
                }
                _ => panic!("unexpected request {method} {path}?{query:?}"),
            }
        }

        fn create(&mut self, path: &str, bytes: Vec<u8>, stored_crc64: Option<&str>) -> String {
            self.next_etag += 1;
            let etag = format!("\"0x{:X}\"", self.next_etag);
            self.blobs.insert(
                path.to_owned(),
                Blob {
                    bytes,
                    stored_crc64: stored_crc64.map(str::to_owned),
                    etag: etag.clone(),
                },
            );
            etag
        }
    }

    fn header<'h>(headers: &'h http::HeaderMap, name: &str) -> Option<&'h str> {
        headers.get(name).and_then(|value| value.to_str().ok())
    }

    fn status(code: u16) -> http::Response<Bytes> {
        http::Response::builder()
            .status(code)
            .body(Bytes::new())
            .expect("response")
    }

    fn query_value(query: Option<&str>, name: &str) -> Option<String> {
        query?
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
            .map(percent_decode)
    }

    fn percent_decode(value: &str) -> String {
        let mut bytes = Vec::new();
        let mut rest = value.as_bytes();
        while let Some((&first, tail)) = rest.split_first() {
            if first == b'%' {
                let hex = std::str::from_utf8(&tail[..2]).expect("hex");
                bytes.push(u8::from_str_radix(hex, 16).expect("hex"));
                rest = &tail[2..];
            } else {
                bytes.push(first);
                rest = tail;
            }
        }
        String::from_utf8(bytes).expect("utf8")
    }

    /// The inclusive first and last byte of a `bytes=first-last` header.
    fn parse_range(value: &str) -> (usize, usize) {
        let (first, last) = value
            .strip_prefix("bytes=")
            .expect("bytes range")
            .split_once('-')
            .expect("range");
        (first.parse().expect("first"), last.parse().expect("last"))
    }

    /// Builds a signer for Azurite's development account that sends its
    /// requests to `service`.
    fn test_signer(service: BlobService) -> AzureRequestSigner {
        let provider = MicrosoftAzureBuilder::new()
            .with_account("devstoreaccount1")
            .with_container_name("container")
            .with_access_key(EMULATOR_KEY)
            .with_endpoint(ENDPOINT.to_owned())
            .with_allow_http(true)
            .build()
            .expect("provider");
        let credentials = Arc::clone(provider.credentials());
        AzureRequestSigner {
            provider: Arc::new(provider),
            http: HttpClient::new(service),
            credentials,
            account_name: "devstoreaccount1".to_owned(),
            container_url: format!("{ENDPOINT}/container"),
            key_scope: KeyScope::default(),
            retry: Arc::new(TransportRetry::new(Arc::new(TokioClock::start()))),
        }
    }

    #[tokio::test]
    async fn a_create_sends_the_crc64_the_service_verifies_and_records_it_for_head() {
        let service = BlobService::default();
        let signer = test_signer(service.clone());
        let key = "namespaces/demo/wal/1";
        let bytes = Bytes::from_static(b"hello azure");
        let checksum = Checksum::crc64nvme(&bytes);

        let created = signer
            .create_checked(key, bytes.clone(), &checksum)
            .await
            .expect("create");
        assert_eq!(created.size_bytes, bytes.len() as u64);
        assert_eq!(created.checksum, Some(checksum.clone()));
        assert!(created.etag.is_some());

        let head = signer
            .head_metadata(key)
            .await
            .expect("head")
            .expect("present");
        assert_eq!(head.checksum, Some(checksum.clone()));
        assert_eq!(head.etag, created.etag);
        assert_eq!(head.size_bytes, bytes.len() as u64);
        assert!(signer
            .head_metadata("namespaces/demo/wal/2")
            .await
            .expect("head")
            .is_none());

        let error = signer
            .create_checked(
                key,
                Bytes::from_static(b"other"),
                &Checksum::crc64nvme(b"other"),
            )
            .await
            .expect_err("occupied key");
        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });

        let state = service.0.lock().expect("state");
        let create = state
            .requests
            .iter()
            .find(|request| request.method == http::Method::PUT)
            .expect("create request");
        assert_eq!(create.path, format!("{CONTAINER_PATH}/{key}"));
        let crc64 = encode_base64_checksum_little_endian(&checksum);
        assert_eq!(create.headers[CONTENT_CRC64_HEADER], crc64);
        assert_eq!(create.headers[STORED_CRC64_HEADER], crc64);
        assert_eq!(create.headers[IF_NONE_MATCH], "*");
        assert_eq!(create.headers[BLOB_TYPE_HEADER], "BlockBlob");
        assert_eq!(state.blobs[&format!("{CONTAINER_PATH}/{key}")].bytes, bytes);
    }

    #[tokio::test]
    async fn a_streamed_put_commits_its_blocks_in_order_while_the_key_is_absent() {
        let service = BlobService::default();
        let signer = test_signer(service.clone());
        let key = "namespaces/demo/content/con_1";
        let path = format!("{CONTAINER_PATH}/{key}");
        let parts = [
            Bytes::from_static(b"first "),
            Bytes::from_static(b"second "),
            Bytes::from_static(b"third"),
        ];
        let whole: Vec<u8> = parts.iter().flat_map(|part| part.iter().copied()).collect();
        let rest = PartReader::new(
            stream::iter(parts[1..].iter().cloned().map(Ok::<_, ObjectStoreError>)).boxed(),
            7,
        );

        let created = signer
            .put_if_absent(key, parts[0].clone(), rest, 2)
            .await
            .expect("put");
        assert_eq!(created.size_bytes, whole.len() as u64);
        assert_eq!(created.checksum, Some(Checksum::crc64nvme(&whole)));
        {
            let state = service.0.lock().expect("state");
            assert_eq!(state.blobs[&path].bytes, whole);
            let ids: BTreeSet<String> = state
                .requests
                .iter()
                .filter(|request| request.path == path)
                .filter_map(|request| query_value(request.query.as_deref(), "blockid"))
                .collect();
            assert_eq!(ids.len(), 3, "every block has its own ID");
            let list = state
                .requests
                .iter()
                .find(|request| request.query.as_deref() == Some("comp=blocklist"))
                .expect("block list");
            assert_eq!(list.headers[IF_NONE_MATCH], "*");
            assert_eq!(
                list.headers[STORED_CRC64_HEADER],
                encode_base64_checksum_little_endian(&Checksum::crc64nvme(&whole))
            );
        }

        let rest = PartReader::new(
            stream::iter([Ok::<_, ObjectStoreError>(Bytes::from_static(b"other"))]).boxed(),
            7,
        );
        let error = signer
            .put_if_absent(key, Bytes::from_static(b"an "), rest, 2)
            .await
            .expect_err("occupied key");
        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
        assert_eq!(
            service.0.lock().expect("state").blobs[&path].bytes,
            whole,
            "a refused commit leaves the blob alone"
        );
    }

    #[tokio::test]
    async fn an_assembly_copies_whole_source_runs_and_uploads_the_rest() {
        let service = BlobService::default();
        let signer = test_signer(service.clone());
        let mib = 1024 * 1024;
        let first = Bytes::from(vec![b'a'; 6 * mib]);
        let second = Bytes::from(vec![b'b'; 6 * mib]);
        let tail = Bytes::from_static(b"tail");
        let mut sources = Vec::new();
        for (key, bytes) in [
            ("namespaces/demo/content/con_a", &first),
            ("namespaces/demo/content/con_b", &second),
        ] {
            let checksum = Checksum::crc64nvme(bytes);
            let metadata = signer
                .create_checked(key, bytes.clone(), &checksum)
                .await
                .expect("source");
            sources.push(AssemblySource {
                key: key.to_owned(),
                range: None,
                checksum,
                metadata,
            });
        }
        let whole: Vec<u8> = [first.as_ref(), second.as_ref(), tail.as_ref()].concat();
        let expected = Checksum::crc64nvme(&whole);

        let error = signer
            .assemble(
                "namespaces/demo/content/con_wrong",
                &sources,
                vec![tail.clone()],
                &Checksum::crc64nvme(b"wrong"),
            )
            .await
            .expect_err("wrong expectation");
        assert_matches!(error, ObjectStoreError::ChecksumMismatch { .. });
        assert!(
            !service
                .0
                .lock()
                .expect("state")
                .blobs
                .contains_key(&format!(
                    "{CONTAINER_PATH}/namespaces/demo/content/con_wrong"
                )),
            "a wrong expectation commits nothing"
        );

        let key = "namespaces/demo/content/con_c";
        let path = format!("{CONTAINER_PATH}/{key}");
        let created = signer
            .assemble(key, &sources, vec![tail.clone()], &expected)
            .await
            .expect("assembly");
        assert_eq!(created.size_bytes, whole.len() as u64);
        assert_eq!(created.checksum, Some(expected.clone()));
        let head = signer
            .head_metadata(key)
            .await
            .expect("head")
            .expect("present");
        assert_eq!(head.checksum, Some(expected));

        let error = signer
            .get_range(
                "namespaces/demo/content/con_a",
                ByteRange {
                    start_inclusive: 0,
                    end_exclusive: 4,
                },
                "\"stale\"",
            )
            .await
            .expect_err("stale etag");
        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });

        let state = service.0.lock().expect("state");
        assert_eq!(state.blobs[&path].bytes, whole);
        let blocks: Vec<_> = state
            .requests
            .iter()
            .filter(|request| {
                request.path == path
                    && request
                        .query
                        .as_deref()
                        .is_some_and(|query| query.starts_with("comp=block&"))
            })
            .collect();
        assert_eq!(blocks.len(), 3, "two copied runs and one uploaded tail");
        let copies: Vec<_> = blocks
            .iter()
            .filter(|request| request.headers.contains_key(COPY_SOURCE_HEADER))
            .collect();
        assert_eq!(copies.len(), 2);
        assert!(copies[0].headers[COPY_SOURCE_HEADER]
            .to_str()
            .expect("source")
            .starts_with(&format!(
                "{ENDPOINT}/container/namespaces/demo/content/con_a?"
            )));
        assert_eq!(copies[0].headers[SOURCE_RANGE_HEADER], "bytes=0-6291455");
        assert_eq!(copies[0].headers[CONTENT_LENGTH], "0");
        let uploads: Vec<_> = blocks
            .iter()
            .filter(|request| !request.headers.contains_key(COPY_SOURCE_HEADER))
            .collect();
        assert_eq!(uploads.len(), 1);
        assert_eq!(
            uploads[0].headers[CONTENT_CRC64_HEADER],
            encode_base64_checksum_little_endian(&Checksum::crc64nvme(&tail))
        );
    }
}
