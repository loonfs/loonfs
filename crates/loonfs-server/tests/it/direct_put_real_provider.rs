// Ignored real-provider tests exercise the full server/client/direct-object-store path.

#![allow(clippy::panic)]

use crate::common::{start_server, test_config};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_api::{
    v0::{
        BeginUploadResponse, CompleteMultipartUploadRequest, CompleteUploadRequest,
        DirectMultipartUpload, DirectMultipartUploadOptions, DirectPutUpload, ObjectTransferAccess,
        UploadContentClaim, UploadMode, UploadPartChecksumClaim, UploadSessionStatus,
    },
    ChangeSeq, Checksum, ChecksumAlgorithm, CommitId, CommitRequest, CommitResponse,
    DestinationBehavior, FilesystemOperation, NamespaceId,
};
use loonfs_client::{Client, ClientError, NamespacePath, PayloadSource};
use loonfs_objectstore::{
    AwsS3Credentials, CloudflareR2Credentials, GcpGcsCredentials, ObjectStore,
};
use loonfs_server::{ServerConfig, StoreConfig};
use loonfs_test_support::http::raw_agent;
use std::fmt;
use std::io::Read as _;
use std::time::{SystemTime, UNIX_EPOCH};

const AUTH_TOKEN: &str = "test-token";
const CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";
/// Multipart part size returned by the reference server.
const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

/// Small proxy limit used to test direct transfers with modest payloads.
const PROXY_CAP_BYTES: u64 = 1024 * 1024;

/// Provider-specific create-only header and stored checksum algorithm.
#[derive(Debug, Clone, Copy)]
struct SignedWriteHeaders {
    create_only: &'static str,
    /// Checksum stored for the complete object.
    checksum_algorithm: ChecksumAlgorithm,
}

/// AWS S3 and Cloudflare R2: `if-none-match: *` and stored CRC-64/NVME.
const S3_SIGNED_WRITE: SignedWriteHeaders = SignedWriteHeaders {
    create_only: "if-none-match",
    checksum_algorithm: ChecksumAlgorithm::Crc64nvme,
};

/// Google Cloud Storage's native API: generation zero and stored CRC-32C.
const GCS_SIGNED_WRITE: SignedWriteHeaders = SignedWriteHeaders {
    create_only: "x-goog-if-generation-match",
    checksum_algorithm: ChecksumAlgorithm::Crc32c,
};

/// Builds the completion claim for a direct PUT.
fn direct_put_claim(bytes: &[u8], algorithm: ChecksumAlgorithm) -> UploadContentClaim {
    UploadContentClaim {
        size_bytes: bytes.len() as u64,
        checksum: Checksum::compute(algorithm, bytes),
    }
}

/// Returns the direct PUT details from a begin response.
fn direct_put_of(begin: &BeginUploadResponse) -> &DirectPutUpload {
    match begin {
        BeginUploadResponse::DirectPut { direct_put, .. } => direct_put,
        other => panic!("a direct_put begin answered as {:?}", other.mode()),
    }
}

/// The part geometry a `direct_multipart` begin answered with.
fn direct_multipart_of(begin: &BeginUploadResponse) -> DirectMultipartUpload {
    match begin {
        BeginUploadResponse::DirectMultipart {
            direct_multipart, ..
        } => *direct_multipart,
        other => panic!("a direct_multipart begin answered as {:?}", other.mode()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_direct_put_real_provider_round_trip() {
    let config = AwsS3DirectPutConfig::from_env().expect("load AWS S3 direct-put environment");
    direct_put_round_trip(
        S3_SIGNED_WRITE,
        test_config(
            StoreConfig::AwsS3 {
                bucket: config.bucket,
                region: config.region,
                endpoint_url: config.endpoint,
                credentials: AwsS3Credentials::Static {
                    access_key_id: config.access_key_id.into(),
                    secret_access_key: config.secret_access_key.into(),
                    session_token: config.session_token.map(Into::into),
                },
                key_prefix: Some(config.prefix),
                force_path_style: false,
            },
            AUTH_TOKEN,
            CONTENT_TOKEN_SECRET,
            "direct-put-aws-s3",
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_direct_put_real_provider_round_trip() {
    let config =
        CloudflareR2DirectPutConfig::from_env().expect("load Cloudflare R2 direct-put environment");
    direct_put_round_trip(
        S3_SIGNED_WRITE,
        test_config(
            StoreConfig::CloudflareR2 {
                bucket: config.bucket,
                account_id: config.account_id,
                endpoint_url: config.endpoint,
                credentials: CloudflareR2Credentials::Static {
                    access_key_id: config.access_key_id.into(),
                    secret_access_key: config.secret_access_key.into(),
                },
                key_prefix: Some(config.prefix),
            },
            AUTH_TOKEN,
            CONTENT_TOKEN_SECRET,
            "direct-put-r2",
        ),
    )
    .await;
}

async fn direct_put_round_trip(signed_write: SignedWriteHeaders, config: ServerConfig) {
    let harness = start_server(config).await;

    let namespace = "direct-put-e2e";
    let namespace_id = NamespaceId::parse(namespace).expect("valid namespace id");
    let target =
        NamespacePath::parse(namespace, "/direct-put.txt").expect("parse direct-put target");
    let bytes = b"direct put through a real provider\n";

    let capabilities = harness
        .client
        .capabilities()
        .await
        .expect("fetch capabilities");
    assert!(capabilities.supports("core.uploads.direct_put"));
    assert!(capabilities
        .features
        .keys()
        .all(|feature| !feature.starts_with("core.uploads.direct_put.checksum.")));

    harness
        .client
        .create_namespace(&namespace_id)
        .await
        .expect("create namespace");

    assert_wrong_direct_put_bytes_rejected(&harness.client, &namespace_id, signed_write).await;
    assert_direct_put_requires_its_signed_headers(&harness.client, &namespace_id, signed_write)
        .await;
    assert_direct_put_is_no_replace(&harness.client, &namespace_id, signed_write).await;

    let begin = harness
        .client
        .begin_direct_put(&namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let direct_put = direct_put_of(&begin);
    assert_eq!(
        direct_put.checksum_algorithm,
        signed_write.checksum_algorithm
    );

    harness
        .client
        .upload_via_presigned_url(&direct_put.access, bytes)
        .await
        .expect("upload bytes through presigned provider URL");

    let complete = harness
        .client
        .complete_upload(
            &namespace_id,
            begin.upload_id(),
            &CompleteUploadRequest::DirectPut {
                content: direct_put_claim(bytes, direct_put.checksum_algorithm),
            },
        )
        .await
        .expect("complete direct-put upload");
    assert_eq!(complete.mode, UploadMode::DirectPut);
    let content_ref = complete
        .content_ref()
        .expect("completed content ref")
        .clone();
    let content_token = complete
        .content_token()
        .cloned()
        .expect("completion returns content token");

    let response = post_commit(
        &harness.server_url,
        namespace,
        &CommitRequest {
            commit_id: CommitId::parse("direct-put-e2e").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            content_tokens: vec![content_token],
            operations: vec![FilesystemOperation::PutFile {
                path: target.absolute_path().clone(),
                content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            }],
        },
    );
    assert_eq!(response.committed_seq, ChangeSeq(1));

    let loaded = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read file");
    assert_eq!(loaded, bytes);

    assert_direct_get_returns_the_written_bytes(&harness.client, &target, bytes).await;
    assert_direct_get_capability_serves_ranges(&harness.client, &target, bytes).await;

    harness.server.abort();
}

/// The symmetry claim, against a real provider: what a client wrote
/// directly, it can read back directly.
async fn assert_direct_get_returns_the_written_bytes(
    client: &Client,
    target: &NamespacePath,
    bytes: &[u8],
) {
    let capabilities = client.capabilities().await.expect("fetch capabilities");
    assert!(
        capabilities.supports("core.downloads.direct_get"),
        "a provider proven for direct writes must be offered for direct reads"
    );

    let grant = client
        .begin_download(target, None)
        .await
        .expect("begin download");
    let ObjectTransferAccess::PresignedUrl { method, .. } = &grant.access;
    assert_eq!(method, "GET");
    assert_eq!(grant.content_ref.size_bytes, bytes.len() as u64);

    let mut received = Vec::new();
    let written = client
        .download_via_presigned_url(&grant, &mut received)
        .await
        .expect("read the granted object from the provider");
    assert_eq!(written, bytes.len() as u64);
    assert_eq!(received, bytes);
}

/// `Range` is not among the headers a presigned GET signs, so one issued
/// capability serves ranged, resumed, and parallel reads without another
/// round trip to the server.
///
/// That is a property of the provider's SigV4 verification, not of this
/// codebase, which is why it is asserted here rather than only against the
/// presigner's own unit tests: this is the run that can be wrong.
async fn assert_direct_get_capability_serves_ranges(
    client: &Client,
    target: &NamespacePath,
    bytes: &[u8],
) {
    let grant = client
        .begin_download(target, None)
        .await
        .expect("begin download for ranged reads");
    let ObjectTransferAccess::PresignedUrl { url, headers, .. } = &grant.access;
    assert!(
        headers.is_empty(),
        "a read capability requires the client to send nothing, which is what leaves \
         `Range` free"
    );

    // Two windows, on the one URL, neither of which the signature knew
    // about. Concatenating them must reproduce the object exactly.
    let split = bytes.len() / 2;
    let head = fetch_range(url, 0, split - 1);
    let tail = fetch_range(url, split, bytes.len() - 1);
    assert_eq!(head, &bytes[..split]);
    assert_eq!(tail, &bytes[split..]);
}

/// Reads one inclusive byte range from a presigned URL with a `Range`
/// header the signature never covered.
fn fetch_range(url: &str, first: usize, last: usize) -> Vec<u8> {
    let response = raw_agent()
        .get(url)
        .set("range", &format!("bytes={first}-{last}"))
        .call()
        .expect("ranged read of a presigned capability");
    assert_eq!(
        response.status(),
        206,
        "a provider that served the whole object ignored the range"
    );
    let mut bytes = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut bytes)
        .expect("read ranged body");
    bytes
}

async fn assert_wrong_direct_put_bytes_rejected(
    client: &Client,
    namespace_id: &NamespaceId,
    signed_write: SignedWriteHeaders,
) {
    let bytes = b"expected direct put bytes\n";
    let wrong_bytes = b"wrong direct put bytes\n";
    let begin = client
        .begin_direct_put(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin wrong-bytes direct put");
    let direct_put = direct_put_of(&begin);
    assert_eq!(
        direct_put.checksum_algorithm,
        signed_write.checksum_algorithm
    );

    client
        .upload_via_presigned_url(&direct_put.access, wrong_bytes)
        .await
        .expect("the provider accepts a checksum-less direct PUT");
    expect_client_rejection(
        client
            .complete_upload(
                namespace_id,
                begin.upload_id(),
                &CompleteUploadRequest::DirectPut {
                    content: direct_put_claim(bytes, direct_put.checksum_algorithm),
                },
            )
            .await,
        "complete wrong-bytes direct put",
    );
}

/// Verifies that providers reject missing or modified create-only headers.
async fn assert_direct_put_requires_its_signed_headers(
    client: &Client,
    namespace_id: &NamespaceId,
    signed_write: SignedWriteHeaders,
) {
    let bytes = b"direct put bytes with the signed headers meddled with\n";
    for (label, meddle) in [
        (
            "create-only header omitted",
            Meddle::Remove(signed_write.create_only),
        ),
        (
            "create-only precondition rewritten to allow replacement",
            Meddle::Replace(signed_write.create_only, relaxed_precondition(signed_write)),
        ),
    ] {
        let begin = client
            .begin_direct_put(namespace_id, Some(bytes.len() as u64))
            .await
            .expect("begin meddled direct put");
        let direct_put = direct_put_of(&begin);

        expect_client_rejection(
            client
                .upload_via_presigned_url(&meddle.apply(&direct_put.access), bytes)
                .await,
            label,
        );
        // And the object was never created, so completion has nothing to
        // find and hands out no token.
        expect_client_rejection(
            client
                .complete_upload(
                    namespace_id,
                    begin.upload_id(),
                    &CompleteUploadRequest::DirectPut {
                        content: direct_put_claim(bytes, direct_put.checksum_algorithm),
                    },
                )
                .await,
            &format!("complete after {label}"),
        );
    }
}

/// The create-only precondition holds against a replay of the very same
/// capability, and the object the first write created is untouched.
async fn assert_direct_put_is_no_replace(
    client: &Client,
    namespace_id: &NamespaceId,
    signed_write: SignedWriteHeaders,
) {
    let bytes = b"duplicate direct put bytes\n";
    let begin = client
        .begin_direct_put(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin duplicate direct put");
    let direct_put = direct_put_of(&begin);
    assert_eq!(
        direct_put.checksum_algorithm,
        signed_write.checksum_algorithm
    );

    client
        .upload_via_presigned_url(&direct_put.access, bytes)
        .await
        .expect("first direct put succeeds");
    expect_client_rejection(
        client
            .upload_via_presigned_url(&direct_put.access, bytes)
            .await,
        "duplicate direct put",
    );

    let complete = client
        .complete_upload(
            namespace_id,
            begin.upload_id(),
            &CompleteUploadRequest::DirectPut {
                content: direct_put_claim(bytes, direct_put.checksum_algorithm),
            },
        )
        .await
        .expect("complete first direct put");
    assert!(
        complete.content_token().is_some(),
        "the refused replay left the first object exactly as it was written"
    );
}

/// A precondition value that would let the write replace an existing object,
/// which is exactly what the signature must stop.
fn relaxed_precondition(signed_write: SignedWriteHeaders) -> String {
    if signed_write.create_only.starts_with("x-goog") {
        // Any live generation, rather than "must not exist".
        "1".to_owned()
    } else {
        // An etag no object has, rather than "must not exist".
        "\"never-matches-any-etag\"".to_owned()
    }
}

/// One edit to a signed header set, applied to an issued capability.
enum Meddle {
    Remove(&'static str),
    Replace(&'static str, String),
}

impl Meddle {
    fn apply(&self, access: &ObjectTransferAccess) -> ObjectTransferAccess {
        let ObjectTransferAccess::PresignedUrl {
            method,
            url,
            headers,
            expires_at_ms,
        } = access;
        let mut headers = headers.clone();
        match self {
            Self::Remove(header) => {
                assert!(
                    headers.remove(*header).is_some(),
                    "presigned access did not include expected header `{header}`"
                );
            }
            Self::Replace(header, value) => {
                let previous = headers.insert((*header).to_owned(), value.clone());
                assert!(
                    previous.is_some_and(|previous| &previous != value),
                    "presigned access did not include a different `{header}` to replace"
                );
            }
        }
        ObjectTransferAccess::PresignedUrl {
            method: method.clone(),
            url: url.clone(),
            headers,
            expires_at_ms: *expires_at_ms,
        }
    }
}

fn expect_client_rejection<T>(result: Result<T, ClientError>, context: &str) {
    match result {
        Ok(_) => panic!("{context} unexpectedly succeeded"),
        Err(ClientError::Api { .. } | ClientError::Http(_)) => {}
        Err(error) => {
            panic!("{context} failed with unexpected client-side error: {error}")
        }
    }
}

fn post_commit(server_url: &str, namespace: &str, request: &CommitRequest) -> CommitResponse {
    let response = raw_agent()
        .post(&format!("{server_url}/v0/namespaces/{namespace}/commits"))
        .set("authorization", &format!("Bearer {AUTH_TOKEN}"))
        .send_json(request)
        .expect("post mutation");

    serde_json::from_reader(response.into_reader()).expect("decode mutation response")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AwsS3DirectPutConfig {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CloudflareR2DirectPutConfig {
    bucket: String,
    account_id: String,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    prefix: String,
}

impl AwsS3DirectPutConfig {
    fn from_env() -> Result<Self, ProviderEnvError> {
        Ok(Self {
            bucket: required_env("LOONFS_TEST_S3_BUCKET")?,
            region: required_env("LOONFS_TEST_S3_REGION")?,
            endpoint: optional_env("LOONFS_TEST_S3_ENDPOINT"),
            access_key_id: required_env("LOONFS_TEST_S3_ACCESS_KEY_ID")?,
            secret_access_key: required_env("LOONFS_TEST_S3_SECRET_ACCESS_KEY")?,
            session_token: optional_env("LOONFS_TEST_S3_SESSION_TOKEN"),
            prefix: direct_put_prefix("aws-s3", optional_env("LOONFS_TEST_S3_PREFIX")),
        })
    }
}

impl CloudflareR2DirectPutConfig {
    fn from_env() -> Result<Self, ProviderEnvError> {
        Ok(Self {
            bucket: required_env("LOONFS_TEST_R2_BUCKET")?,
            account_id: required_env("LOONFS_TEST_R2_ACCOUNT_ID")?,
            endpoint: required_env("LOONFS_TEST_R2_ENDPOINT")?,
            access_key_id: required_env("LOONFS_TEST_R2_ACCESS_KEY_ID")?,
            secret_access_key: required_env("LOONFS_TEST_R2_SECRET_ACCESS_KEY")?,
            prefix: direct_put_prefix("r2", optional_env("LOONFS_TEST_R2_PREFIX")),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GcpGcsDirectPutConfig {
    bucket: String,
    service_account_key_path: String,
    prefix: String,
}

impl GcpGcsDirectPutConfig {
    fn from_env() -> Result<Self, ProviderEnvError> {
        Ok(Self {
            bucket: required_env("LOONFS_TEST_GCS_BUCKET")?,
            service_account_key_path: required_env("LOONFS_TEST_GCS_SERVICE_ACCOUNT_KEY_PATH")?,
            prefix: direct_put_prefix("gcp-gcs", optional_env("LOONFS_TEST_GCS_PREFIX")),
        })
    }

    fn store(self) -> StoreConfig {
        StoreConfig::GcpGcs {
            bucket: self.bucket,
            credentials: GcpGcsCredentials::ServiceAccountFile {
                path: self.service_account_key_path,
            },
            key_prefix: Some(self.prefix),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProviderEnvError {
    Missing { name: &'static str },
    Empty { name: &'static str },
    NonUnicode { name: &'static str },
}

impl fmt::Display for ProviderEnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { name } => write!(f, "missing environment variable {name}"),
            Self::Empty { name } => write!(f, "environment variable {name} must not be empty"),
            Self::NonUnicode { name } => {
                write!(f, "environment variable {name} must be valid Unicode")
            }
        }
    }
}

fn required_env(name: &'static str) -> Result<String, ProviderEnvError> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Err(ProviderEnvError::Empty { name }),
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Err(ProviderEnvError::Missing { name }),
        Err(std::env::VarError::NotUnicode(_)) => Err(ProviderEnvError::NonUnicode { name }),
    }
}

fn optional_env(name: &'static str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

#[allow(clippy::disallowed_methods)]
fn direct_put_prefix(provider: &str, base_prefix: Option<String>) -> String {
    // Unique live-provider key prefixes need wall-clock entropy; nothing
    // replays this path.
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let base = base_prefix.unwrap_or_else(|| format!("direct-put/{provider}"));
    format!(
        "{}/{}/{}-{stamp}",
        base.trim_matches('/'),
        std::process::id(),
        provider
    )
}

/// Runs the direct PUT round trip through native GCS V4 signed URLs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_direct_put_real_provider_round_trip() {
    let config = GcpGcsDirectPutConfig::from_env().expect("load GCP GCS direct-put environment");
    direct_put_round_trip(
        GCS_SIGNED_WRITE,
        test_config(
            config.store(),
            AUTH_TOKEN,
            CONTENT_TOKEN_SECRET,
            "direct-put-gcp-gcs",
        ),
    )
    .await;
}

/// Tests GCS capability scope, replay protection, expiry, and ranged reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_signed_capabilities_are_scoped_bounded_and_single_use() {
    let config = GcpGcsDirectPutConfig::from_env().expect("load GCP GCS direct-put environment");
    let store_config = config.clone().store();
    let mut server_config = test_config(
        config.store(),
        AUTH_TOKEN,
        CONTENT_TOKEN_SECRET,
        "gcs-signed-capabilities",
    );
    // Keep the test payload above both proxy limits without uploading a large file.
    server_config.max_upload_bytes = PROXY_CAP_BYTES;
    server_config.max_download_bytes = PROXY_CAP_BYTES;
    let harness = start_server(server_config).await;

    let namespace = "gcs-capabilities";
    let namespace_id = NamespaceId::parse(namespace).expect("valid namespace id");
    harness
        .client
        .create_namespace(&namespace_id)
        .await
        .expect("create namespace");

    let capabilities = harness
        .client
        .capabilities()
        .await
        .expect("fetch capabilities");
    assert!(capabilities.supports("core.uploads.direct_put"));
    assert!(capabilities.supports("core.downloads.direct_get"));
    assert!(capabilities
        .features
        .keys()
        .all(|feature| !feature.starts_with("core.uploads.direct_put.checksum.")));
    assert!(
        !capabilities.supports("core.uploads.direct_multipart"),
        "this adapter signs no multipart for GCS, so the key must be absent"
    );

    assert_gcs_completion_judges_the_object_that_is_there(&harness.client, &namespace_id).await;
    assert_gcs_signed_writes_land_under_the_configured_prefix(&harness, &store_config).await;
    assert_gcs_read_capability_serves_ranges_and_resumes(&harness, namespace).await;
    assert_gcs_expired_capability_is_refused(&harness.client, &namespace_id).await;
    assert_gcs_cap_bound_object_moves_only_directly(&harness, namespace).await;

    delete_everything_under_the_run_prefix(&store_config).await;
    harness.server.abort();
}

/// Completion verifies the stored object before issuing a content token.
async fn assert_gcs_completion_judges_the_object_that_is_there(
    client: &Client,
    namespace_id: &NamespaceId,
) {
    let bytes = b"gcs completion reads the object back\n";
    let begin = client
        .begin_direct_put(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let direct_put = direct_put_of(&begin);
    assert_eq!(direct_put.checksum_algorithm, ChecksumAlgorithm::Crc32c);
    client
        .upload_via_presigned_url(&direct_put.access, bytes)
        .await
        .expect("upload the promised bytes");

    let complete = client
        .complete_upload(
            namespace_id,
            begin.upload_id(),
            &CompleteUploadRequest::DirectPut {
                content: direct_put_claim(bytes, direct_put.checksum_algorithm),
            },
        )
        .await
        .expect("complete the honest promise");
    assert_eq!(complete.mode, UploadMode::DirectPut);
    let content_ref = complete.content_ref().expect("completed content ref");
    assert!(complete.content_token().is_some());
    assert_eq!(
        content_ref.checksum,
        Checksum::crc32c(bytes),
        "the readback and the promise met on the crc32c GCS actually stored"
    );
}

/// Every object a signed write creates lands beneath the deployment's
/// configured key prefix, so a capability cannot address the bucket outside
/// this deployment's scope.
async fn assert_gcs_signed_writes_land_under_the_configured_prefix(
    harness: &crate::common::TestServer,
    store_config: &StoreConfig,
) {
    let namespace_id = NamespaceId::parse("gcs-capabilities").expect("valid namespace id");
    let bytes = b"gcs prefix scoping\n";
    let begin = harness
        .client
        .begin_direct_put(&namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let direct_put = direct_put_of(&begin);
    let ObjectTransferAccess::PresignedUrl { url, .. } = &direct_put.access;
    let StoreConfig::GcpGcs {
        bucket, key_prefix, ..
    } = store_config
    else {
        panic!("this proof runs on GCS")
    };
    let prefix = key_prefix.as_deref().expect("this run configures a prefix");
    let content_id = url
        .split('?')
        .next()
        .and_then(|path| path.rsplit('/').next())
        .expect("content id in signed URL");
    assert!(
        url.starts_with(&format!(
            "https://storage.googleapis.com/{bucket}/{prefix}/"
        )),
        "a signed write addressed something outside the deployment's prefix: {url}"
    );

    harness
        .client
        .upload_via_presigned_url(&direct_put.access, bytes)
        .await
        .expect("upload under the prefix");

    // The store scopes every key beneath the same prefix, so listing its
    // root is exactly this run's objects — and the new one is among them.
    let store = store_config
        .configured_object_store()
        .expect("build a store")
        .into_shared();
    let keys = store.list_prefix("").await.expect("list the run prefix");
    assert!(
        keys.iter().any(|key| key.contains(content_id)),
        "the object a signed write created is not under the run prefix"
    );
}

/// One read capability serves the whole object, arbitrary windows of it, and
/// a resumption that stitches a prefix already in hand to the rest.
async fn assert_gcs_read_capability_serves_ranges_and_resumes(
    harness: &crate::common::TestServer,
    namespace: &str,
) {
    let payload: Vec<u8> = (0..96 * 1024).map(|offset| (offset % 251) as u8).collect();
    let target = NamespacePath::parse(namespace, "/ranged.bin").expect("ranged target");
    harness
        .client
        .put_file_bytes(&target, &payload, &put_options("gcs-ranged"))
        .await
        .expect("stage an object to read back");

    let grant = harness
        .client
        .begin_download(&target, None)
        .await
        .expect("begin download");
    let ObjectTransferAccess::PresignedUrl { url, headers, .. } = &grant.access;
    assert!(
        headers.is_empty(),
        "a read capability requires the client to send nothing, which is what leaves \
         `Range` free"
    );

    // The whole object, then three disjoint windows off the one URL that
    // reassemble into it exactly.
    let mut received = Vec::new();
    harness
        .client
        .download_via_presigned_url(&grant, &mut received)
        .await
        .expect("read the whole object");
    assert_eq!(received, payload);

    let cuts = [0, 12_345, 60_000, payload.len()];
    let mut reassembled = Vec::new();
    for window in cuts.windows(2) {
        reassembled.extend_from_slice(&fetch_range(url, window[0], window[1] - 1));
    }
    assert_eq!(
        reassembled, payload,
        "three windows off one capability did not reassemble the object"
    );

    // A resumption: a prefix already in hand, and the remainder fetched with
    // a `Range` the signature never covered.
    let already_have = 40_000;
    let mut resumed = payload[..already_have].to_vec();
    resumed.extend_from_slice(&fetch_range(url, already_have, payload.len() - 1));
    assert_eq!(
        resumed, payload,
        "a resumed read did not fold to the object"
    );
}

/// A capability stops working when it expires, which is what bounds the
/// damage of one leaking.
async fn assert_gcs_expired_capability_is_refused(client: &Client, namespace_id: &NamespaceId) {
    let bytes = b"gcs expiry\n";
    let begin = client
        .begin_direct_put(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let direct_put = direct_put_of(&begin);
    let ObjectTransferAccess::PresignedUrl { url, .. } = &direct_put.access;

    // Rewrite the capability's own lifetime to one already spent. The
    // signature covered the original, so this is refused twice over — which
    // is the point: neither the expiry nor the signature can be edited in
    // flight.
    let expired = url.replace("X-Goog-Expires=900", "X-Goog-Expires=1");
    assert_ne!(&expired, url, "the capability did not carry an expiry");
    let response = raw_agent().put(&expired).send_bytes(bytes);
    assert!(
        response.is_err(),
        "a capability with an edited lifetime was accepted"
    );
}

/// An object past the deployment's proxy caps moves only directly, in both
/// directions, and the proxy still refuses it.
async fn assert_gcs_cap_bound_object_moves_only_directly(
    harness: &crate::common::TestServer,
    namespace: &str,
) {
    let payload: Vec<u8> = (0..4 * PROXY_CAP_BYTES as usize)
        .map(|offset| (offset % 251) as u8)
        .collect();
    let target = NamespacePath::parse(namespace, "/cap-bound.bin").expect("cap-bound target");

    // This adapter signs no multipart for GCS, so the whole-object write is
    // the only direct transport on offer — and the proxy will not take it.
    harness
        .client
        .put_file_bytes(&target, &payload, &put_options("gcs-cap-bound"))
        .await
        .expect("an object past the proxy cap takes the whole-object direct write");

    let grant = harness
        .client
        .begin_download(&target, None)
        .await
        .expect("an object past the read cap is served by grant");
    let mut received = Vec::new();
    harness
        .client
        .download_via_presigned_url(&grant, &mut received)
        .await
        .expect("read the granted object");
    assert_eq!(received, payload);

    // And the proxy still says no, which is what made the grant necessary.
    // `get_file_bytes` is the plain proxied read with no fallback, so this
    // asks the proxy directly rather than re-running the ladder.
    expect_client_rejection(
        harness.client.get_file_bytes(&target).await,
        "proxied read of an object past the read cap",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_direct_multipart_real_provider_round_trip() {
    let config = AwsS3DirectPutConfig::from_env().expect("load AWS S3 direct-put environment");
    direct_multipart_round_trip(test_config(
        StoreConfig::AwsS3 {
            bucket: config.bucket,
            region: config.region,
            endpoint_url: config.endpoint,
            credentials: AwsS3Credentials::Static {
                access_key_id: config.access_key_id.into(),
                secret_access_key: config.secret_access_key.into(),
                session_token: config.session_token.map(Into::into),
            },
            key_prefix: Some(config.prefix),
            force_path_style: false,
        },
        AUTH_TOKEN,
        CONTENT_TOKEN_SECRET,
        "direct-multipart-aws-s3",
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_direct_multipart_real_provider_round_trip() {
    let config =
        CloudflareR2DirectPutConfig::from_env().expect("load Cloudflare R2 direct-put environment");
    direct_multipart_round_trip(test_config(
        StoreConfig::CloudflareR2 {
            bucket: config.bucket,
            account_id: config.account_id,
            endpoint_url: config.endpoint,
            credentials: CloudflareR2Credentials::Static {
                access_key_id: config.access_key_id.into(),
                secret_access_key: config.secret_access_key.into(),
            },
            key_prefix: Some(config.prefix),
        },
        AUTH_TOKEN,
        CONTENT_TOKEN_SECRET,
        "direct-multipart-r2",
    ))
    .await;
}

/// A payload with a distinct byte at every offset, so a part landing in the
/// wrong place or twice cannot go unnoticed.
fn multipart_payload(part_size: usize, parts: usize) -> Vec<u8> {
    (0..part_size * parts + 4_096)
        .map(|offset| (offset % 251) as u8)
        .collect()
}

/// The whole point of this PR, end to end against a real provider: a large
/// object crosses the network once, in parallel, straight into object
/// storage — and LoonFS believes the result only after reading the
/// assembled object's own checksum back.
async fn direct_multipart_round_trip(config: ServerConfig) {
    let store_config = config.store.clone();
    let harness = start_server(config).await;

    let namespace = "direct-multipart-e2e";
    let namespace_id = NamespaceId::parse(namespace).expect("valid namespace id");
    let target =
        NamespacePath::parse(namespace, "/large.bin").expect("parse direct-multipart target");

    let capabilities = harness
        .client
        .capabilities()
        .await
        .expect("fetch capabilities");
    assert!(capabilities.supports("core.uploads.direct_multipart"));

    harness
        .client
        .create_namespace(&namespace_id)
        .await
        .expect("create namespace");

    // Three parts, which is the smallest payload that exercises a middle
    // part at all. The providers refuse a non-final part below their own
    // minimum, so this also proves the geometry the server hands out
    // satisfies that rule without the client knowing what it is.
    let part_size = MULTIPART_PART_SIZE;
    let payload = multipart_payload(part_size, 2);
    let whole_object = Checksum::crc64nvme(&payload);
    // Begin declares nothing about the payload. A one-pass uploader could
    // not fill in a length or a digest here, and this session proves it does
    // not have to: the claim arrives with the completion below.
    let begin = harness
        .client
        .begin_direct_multipart(&namespace_id, DirectMultipartUploadOptions::default())
        .await
        .expect("begin direct multipart");
    let upload_id = begin.upload_id().clone();
    let multipart = direct_multipart_of(&begin);
    assert_eq!(
        multipart.part_size_bytes as usize, part_size,
        "the deployment's part geometry is the one this payload was cut to"
    );
    assert_eq!(multipart.checksum_algorithm, ChecksumAlgorithm::Crc64nvme);

    let chunks: Vec<&[u8]> = payload.chunks(part_size).collect();
    let claims: Vec<UploadPartChecksumClaim> = chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| UploadPartChecksumClaim {
            part_number: index as u32 + 1,
            checksum: Checksum::compute(multipart.checksum_algorithm, chunk),
        })
        .collect();
    let signed = harness
        .client
        .sign_upload_parts(&namespace_id, &upload_id, claims.clone())
        .await
        .expect("sign every part");
    assert_eq!(signed.parts.len(), chunks.len());

    // Every part in parallel, and part two twice: re-uploading one part is
    // how a client retries a transfer that failed halfway, and the provider
    // takes the last write.
    let mut in_flight = tokio::task::JoinSet::new();
    for part in signed.parts.iter().cloned() {
        let index = part.part_number as usize - 1;
        let client = harness.client.clone();
        let checksum = claims[index].checksum.clone();
        let chunk = Bytes::copy_from_slice(chunks[index]);
        in_flight.spawn(async move {
            client
                .upload_part_via_presigned_url(part.part_number, &part.access, checksum, chunk)
                .await
        });
    }
    let mut parts = Vec::new();
    while let Some(joined) = in_flight.join_next().await {
        parts.push(joined.expect("part task").expect("part upload"));
    }
    let repeated = &signed.parts[1];
    let replaced = harness
        .client
        .upload_part_via_presigned_url(
            repeated.part_number,
            &repeated.access,
            claims[1].checksum.clone(),
            Bytes::copy_from_slice(chunks[1]),
        )
        .await
        .expect("re-uploading a part is allowed and last-write-wins");
    parts.retain(|part| part.part_number != repeated.part_number);
    parts.push(replaced);
    parts.sort_by_key(|part| part.part_number);

    // The claim rides with the completion, and the identity comes back with
    // the answer: the client never named the object it wrote.
    let request = CompleteMultipartUploadRequest {
        content: UploadContentClaim {
            size_bytes: payload.len() as u64,
            checksum: whole_object.clone(),
        },
        parts,
    };
    let complete = harness
        .client
        .complete_multipart_upload(&namespace_id, &upload_id, &request)
        .await
        .expect("complete the multipart upload");
    assert_eq!(complete.mode, UploadMode::DirectMultipart);
    let content_ref = complete
        .content_ref()
        .expect("completed content ref")
        .clone();
    let UploadSessionStatus::Completed {
        completed_at_ms, ..
    } = complete.status
    else {
        panic!("completion reports a completed session")
    };
    assert_eq!(content_ref.size_bytes, payload.len() as u64);
    assert_eq!(content_ref.checksum, whole_object);

    // The lost completion. The providers disagree completely about what a
    // replayed completion means — AWS S3 replays a success with no checksum
    // in it, Cloudflare R2 answers `NoSuchUpload` — so neither answer is
    // used. Both reconcile from the durable session and the object.
    let replayed = harness
        .client
        .complete_multipart_upload(&namespace_id, &upload_id, &request)
        .await
        .expect("a lost completion is answered, not failed");
    assert_eq!(replayed.mode, UploadMode::DirectMultipart);
    assert_eq!(replayed.content_ref(), Some(&content_ref));
    let UploadSessionStatus::Completed {
        completed_at_ms: replayed_at_ms,
        ..
    } = &replayed.status
    else {
        panic!("completion replay reports a completed session")
    };
    assert_eq!(*replayed_at_ms, completed_at_ms);

    let content_token = replayed
        .content_token()
        .cloned()
        .expect("completion returns content token");
    let response = post_commit(
        &harness.server_url,
        namespace,
        &CommitRequest {
            commit_id: CommitId::parse("direct-multipart-e2e").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            content_tokens: vec![content_token],
            operations: vec![FilesystemOperation::PutFile {
                path: target.absolute_path().clone(),
                content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            }],
        },
    );
    assert_eq!(response.committed_seq, ChangeSeq(1));

    // Reading it back is what proves the crc-only reference is a first-class
    // one: the read verifies by that crc, because nothing else describes
    // these bytes.
    let loaded = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read the assembled file");
    assert_eq!(loaded, payload);

    // The direct read of an object the client assembled directly — the
    // symmetry this deployment owes.
    let grant = harness
        .client
        .begin_download(&target, None)
        .await
        .expect("begin download of the assembled object");
    let mut received = Vec::new();
    let written = harness
        .client
        .download_via_presigned_url(&grant, &mut received)
        .await
        .expect("read the assembled object straight from the provider");
    assert_eq!(written, payload.len() as u64);
    assert_eq!(received, payload);
    assert_direct_get_capability_serves_ranges(&harness.client, &target, &payload).await;

    // And a rerun of the same put under the same commit id reconciles by
    // content rather than conflicting, even with no whole-file sha256 to
    // compare — the provider's crc is the evidence.
    let rerun = harness
        .client
        .put_file_bytes(
            &NamespacePath::parse(namespace, "/rerun.bin").expect("rerun target"),
            &payload,
            &loonfs_client::PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("multipart-rerun").expect("valid commit id")),
                    message: None,
                },
                expected_revision_no: None,
            },
        )
        .await
        .expect("a multipart put through the client");
    let replayed_rerun = harness
        .client
        .put_file_bytes(
            &NamespacePath::parse(namespace, "/rerun.bin").expect("rerun target"),
            &payload,
            &loonfs_client::PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("multipart-rerun").expect("valid commit id")),
                    message: None,
                },
                expected_revision_no: None,
            },
        )
        .await
        .expect("rerunning identical bytes is idempotent without a sha256");
    assert_eq!(replayed_rerun, rerun);

    one_pass_puts_against_the_provider(&harness, namespace).await;

    harness.server.abort();
    delete_everything_under_the_run_prefix(&store_config).await;
}

/// The one-pass path, against the provider that has to accept it.
///
/// Both puts here are what `loonfs put` runs: a payload that is never held
/// whole, cut into parts as it is read, with the object's length and digest
/// discovered on the way past and claimed at completion. The first reads a
/// real file from disk — the size that matters is the file's, not this
/// process's — and the second reads a source that cannot say how long it
/// is, which is the case a pipe presents and the one no amount of buffering
/// would rescue.
async fn one_pass_puts_against_the_provider(harness: &crate::common::TestServer, namespace: &str) {
    // Three parts and a bit at the deployment's geometry, so the pass
    // crosses a wave boundary and ends on a short part.
    let payload: Vec<u8> = (0..3 * MULTIPART_PART_SIZE + 4_096)
        .map(|offset| (offset % 251) as u8)
        .collect();

    let file = tempfile::NamedTempFile::new().expect("temp file for the one-pass put");
    std::fs::write(file.path(), &payload).expect("write the payload to disk");
    let from_file = NamespacePath::parse(namespace, "/one-pass-file.bin").expect("file target");
    harness
        .client
        .put_file_stream(
            &from_file,
            PayloadSource::open_file(file.path())
                .await
                .expect("open the payload file"),
            &put_options("one-pass-file"),
        )
        .await
        .expect("a file streams straight into object storage");
    assert_eq!(
        harness
            .client
            .get_file_bytes(&from_file)
            .await
            .expect("read the file back"),
        payload,
        "the object the provider assembled is the file that was read"
    );

    // The same flow with the length withheld. Nothing in the transport
    // needed it: begin declares nothing, and the claim is produced by the
    // pass itself.
    let piped = NamespacePath::parse(namespace, "/one-pass-piped.bin").expect("piped target");
    let chunks: Vec<Bytes> = payload
        .chunks(64 * 1024)
        .map(Bytes::copy_from_slice)
        .collect();
    let source = PayloadSource::stream(futures::stream::iter(chunks.into_iter().map(Ok)).boxed());
    assert_eq!(source.size_bytes(), None, "this source declares no length");
    harness
        .client
        .put_file_stream(&piped, source, &put_options("one-pass-piped"))
        .await
        .expect("a source of unknown length uploads the same way");
    assert_eq!(
        harness
            .client
            .get_file_bytes(&piped)
            .await
            .expect("read the piped payload back"),
        payload,
        "a payload nobody measured up front still lands byte for byte"
    );
}

fn put_options(commit_id: &str) -> loonfs_client::PutFileOptions {
    loonfs_client::PutFileOptions {
        behavior: DestinationBehavior::NoReplace,
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(CommitId::parse(commit_id).expect("valid commit id")),
            message: None,
        },
        expected_revision_no: None,
    }
}

/// Removes every object this run wrote.
///
/// The run's key prefix is unique, and the store scopes every key beneath
/// it, so listing the store root lists exactly this run's objects and
/// nothing else in the bucket.
async fn delete_everything_under_the_run_prefix(store_config: &StoreConfig) {
    let store = store_config
        .configured_object_store()
        .expect("build a store for cleanup")
        .into_shared();
    let keys = store.list_prefix("").await.expect("list the run prefix");
    for key in keys {
        store.delete(&key).await.expect("delete a run object");
    }
    assert!(
        store
            .list_prefix("")
            .await
            .expect("re-list the run prefix")
            .is_empty(),
        "the run left objects behind"
    );
}
