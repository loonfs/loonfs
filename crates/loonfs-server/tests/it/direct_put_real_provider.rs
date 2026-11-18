// Ignored real-provider tests exercise the full server/client/direct-object-store path.

use crate::common::{start_server, test_config};
use loonfs_client::{Client, ClientError, DownloadOptions, NamespacePath};
use loonfs_objectstore::gcs::GcpGcsStoreConfig;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{GcpGcsCredentials, ObjectStore};
use loonfs_server::{ServerConfig, StoreConfig};
use loonfs_test_support::http::raw_agent;
use loonfs_types::{
    Checksum, ChecksumAlgorithm, Commit, CommitId, CommitRequest, CommitSeq, CompleteUploadBody,
    DestinationBehavior, FilesystemOperation, NamespaceId, ObjectTransfer, UploadContentClaim,
    UploadMode, UploadSession, UploadSessionStatus,
};
use std::fmt;
use std::io::Read as _;
use std::time::{SystemTime, UNIX_EPOCH};

const AUTH_TOKEN: &str = "test-token";
const CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";
/// Small proxy limit used to test direct transfers with modest payloads.
const PROXY_CAP_BYTES: u64 = 1024 * 1024;

/// Provider-specific create-only header and stored checksum algorithm.
#[derive(Debug, Clone, Copy)]
struct SignedWriteHeaders {
    create_only: &'static str,
    /// Checksum stored for the complete object.
    checksum_algorithm: ChecksumAlgorithm,
}

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

/// Extracts the checksum algorithm and write access from a direct PUT response.
fn direct_put_of(begin: &UploadSession) -> (ChecksumAlgorithm, &ObjectTransfer) {
    match &begin.status {
        UploadSessionStatus::Open {
            checksum_algorithm,
            transfer: Some(access),
            ..
        } => (*checksum_algorithm, access),
        other => panic!("a direct_put begin answered as {:?}", other),
    }
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
        .get_capabilities()
        .await
        .expect("fetch capabilities");
    assert!(capabilities.supports("filesystem.uploads.direct_put"));

    harness
        .client
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    assert_wrong_direct_put_bytes_rejected(&harness.client, &namespace_id, signed_write).await;
    assert_direct_put_requires_its_signed_headers(&harness.client, &namespace_id, signed_write)
        .await;
    assert_direct_put_is_no_replace(&harness.client, &namespace_id, signed_write).await;

    let begin = harness
        .client
        .create_direct_put_upload_target(&namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let (checksum_algorithm, access) = direct_put_of(&begin);
    assert_eq!(checksum_algorithm, signed_write.checksum_algorithm);

    harness
        .client
        .upload_via_presigned_url(access, bytes)
        .await
        .expect("upload bytes through presigned provider URL");

    let complete = harness
        .client
        .complete_upload(
            &namespace_id,
            &begin.upload_id,
            &CompleteUploadBody::DirectPut {
                content: direct_put_claim(bytes, checksum_algorithm),
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
            preconditions: Vec::new(),
            commit_id: CommitId::parse("direct-put-e2e").expect("valid commit id"),
            message: None,
            content_tokens: vec![content_token],
            operations: vec![FilesystemOperation::PutFile {
                path: target.absolute_path().clone(),
                content: loonfs_types::FileContent::Ref(content_ref),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            }],
        },
    );
    assert_eq!(response.committed_seq, CommitSeq(1));

    let loaded = harness.client.read_file(&target).await.expect("read file");
    assert_eq!(loaded, bytes);

    assert_direct_get_returns_the_written_bytes(&harness.client, &target, bytes).await;
    assert_direct_get_capability_serves_only_its_range(&harness.client, &target, bytes).await;

    harness.server.abort();
}

/// The symmetry claim, against a real provider: what a client wrote
/// directly, it can read back directly.
async fn assert_direct_get_returns_the_written_bytes(
    client: &Client,
    target: &NamespacePath,
    bytes: &[u8],
) {
    let capabilities = client.get_capabilities().await.expect("fetch capabilities");
    assert!(
        capabilities.supports("filesystem.downloads.direct_get"),
        "a provider proven for direct writes must be offered for direct reads"
    );

    let grant = client
        .create_download(target)
        .await
        .expect("begin download");
    let ObjectTransfer::PresignedUrl { method, .. } = &grant.ranges[0].transfer else {
        panic!("expected a presigned URL");
    };
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

/// A grant signs the `Range` it names, so the provider serves those bytes
/// and refuses any other range on the same URL, and a resume takes a new
/// grant from its offset.
///
/// That is a property of the provider's signature verification, not of this
/// codebase, which is why it is asserted here rather than only against the
/// presigner's own unit tests: this is the run that can be wrong.
async fn assert_direct_get_capability_serves_only_its_range(
    client: &Client,
    target: &NamespacePath,
    bytes: &[u8],
) {
    let grant = client
        .create_download(target)
        .await
        .expect("begin download");
    let ObjectTransfer::PresignedUrl { url, headers, .. } = &grant.ranges[0].transfer else {
        panic!("expected a presigned URL");
    };
    let whole = format!("bytes=0-{}", bytes.len() - 1);
    assert_eq!(headers.get("range"), Some(&whole));
    assert_eq!(fetch_range(url, 0, bytes.len() - 1), bytes);
    raw_agent()
        .get(url)
        .set("range", &format!("bytes=1-{}", bytes.len() - 1))
        .call()
        .expect_err("a range the grant does not sign was served");

    let split = bytes.len() / 2;
    let rest = client
        .create_download_with_options(
            target,
            &DownloadOptions {
                start_offset: split as u64,
                ..DownloadOptions::default()
            },
        )
        .await
        .expect("a grant from the resume offset");
    let ObjectTransfer::PresignedUrl { url, .. } = &rest.ranges[0].transfer else {
        panic!("expected a presigned URL");
    };
    assert_eq!(fetch_range(url, split, bytes.len() - 1), &bytes[split..]);
}

/// Reads one inclusive byte range from a presigned URL with the `Range`
/// header its signature covers.
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
    let mut wrong_bytes = *bytes;
    wrong_bytes[0] ^= 1;
    let begin = client
        .create_direct_put_upload_target(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin wrong-bytes direct put");
    let (checksum_algorithm, access) = direct_put_of(&begin);
    assert_eq!(checksum_algorithm, signed_write.checksum_algorithm);

    client
        .upload_via_presigned_url(access, &wrong_bytes)
        .await
        .expect("the provider accepts a checksum-less direct PUT");
    expect_client_rejection(
        client
            .complete_upload(
                namespace_id,
                &begin.upload_id,
                &CompleteUploadBody::DirectPut {
                    content: direct_put_claim(bytes, checksum_algorithm),
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
            .create_direct_put_upload_target(namespace_id, Some(bytes.len() as u64))
            .await
            .expect("begin meddled direct put");
        let (checksum_algorithm, access) = direct_put_of(&begin);

        expect_client_rejection(
            client
                .upload_via_presigned_url(&meddle.apply(access), bytes)
                .await,
            label,
        );
        // And the object was never created, so completion has nothing to
        // find and hands out no token.
        expect_client_rejection(
            client
                .complete_upload(
                    namespace_id,
                    &begin.upload_id,
                    &CompleteUploadBody::DirectPut {
                        content: direct_put_claim(bytes, checksum_algorithm),
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
        .create_direct_put_upload_target(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin duplicate direct put");
    let (checksum_algorithm, access) = direct_put_of(&begin);
    assert_eq!(checksum_algorithm, signed_write.checksum_algorithm);

    client
        .upload_via_presigned_url(access, bytes)
        .await
        .expect("first direct put succeeds");
    expect_client_rejection(
        client.upload_via_presigned_url(access, bytes).await,
        "duplicate direct put",
    );

    let complete = client
        .complete_upload(
            namespace_id,
            &begin.upload_id,
            &CompleteUploadBody::DirectPut {
                content: direct_put_claim(bytes, checksum_algorithm),
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
    fn apply(&self, access: &ObjectTransfer) -> ObjectTransfer {
        let ObjectTransfer::PresignedUrl {
            method,
            url,
            headers,
            expires_at_ms,
        } = access
        else {
            panic!("expected a presigned URL");
        };
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
        ObjectTransfer::PresignedUrl {
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
        Err(ClientError::Api { .. } | ClientError::HttpStatus { .. } | ClientError::Http(_)) => {}
        Err(error) => {
            panic!("{context} failed with unexpected client-side error: {error}")
        }
    }
}

fn post_commit(server_url: &str, namespace: &str, request: &CommitRequest) -> Commit {
    let response = raw_agent()
        .post(&format!("{server_url}/v0/namespaces/{namespace}/commits"))
        .set("authorization", &format!("Bearer {AUTH_TOKEN}"))
        .set("Loonfs-Context", "service")
        .set("Loonfs-Actor", "test-actor")
        .send_json(request)
        .expect("post mutation");

    serde_json::from_reader(response.into_reader()).expect("decode mutation response")
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
        StoreConfig::GcpGcs(GcpGcsStoreConfig {
            bucket: self.bucket,
            credentials: GcpGcsCredentials::ServiceAccountFile {
                path: self.service_account_key_path,
            },
            key_prefix: Some(self.prefix),
        })
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

#[tokio::test]
#[ignore = "requires real GCP GCS credentials"]
async fn gcp_gcs_append_keeps_crc32c_after_a_snapshot() {
    let store_config = GcpGcsDirectPutConfig::from_env()
        .expect("load GCP GCS append environment")
        .store();
    let harness = start_server(test_config(
        store_config.clone(),
        AUTH_TOKEN,
        CONTENT_TOKEN_SECRET,
        "gcs-append",
    ))
    .await;
    let namespace_id = NamespaceId::parse("gcs-append").expect("namespace");
    let target = NamespacePath::parse(namespace_id.as_str(), "/append.bin").expect("path");
    let actor = loonfs_test_support::test_actor();
    harness
        .client
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let mut payload = vec![b'a'; 9 * 1024 * 1024];
    harness
        .client
        .put_file(&target, &payload, &actor)
        .await
        .expect("put base");
    let base = harness.client.stat(&target).await.expect("stat base");
    let base = base.content_ref().expect("base reference");
    assert_eq!(base.checksum, Checksum::crc32c(&payload));

    let tail = vec![b'b'; 4096];
    harness
        .client
        .append_file(&target, &tail, &actor)
        .await
        .expect("append");
    payload.extend_from_slice(&tail);
    let appended = harness.client.stat(&target).await.expect("stat append");
    let appended = appended.content_ref().expect("appended reference");
    assert_eq!(appended.content_id, base.content_id);
    assert_eq!(appended.size_bytes, payload.len() as u64);
    assert_eq!(appended.checksum, Checksum::crc32c(&payload));
    assert_eq!(
        harness
            .client
            .read_file(&target)
            .await
            .expect("read append"),
        payload
    );

    harness
        .client
        .create_snapshot_with_options(
            &namespace_id,
            &loonfs_client::CreateSnapshotOptions {
                name: Some("appended".to_owned()),
                ..loonfs_client::CreateSnapshotOptions::default()
            },
        )
        .await
        .expect("snapshot append");
    let folded = harness
        .client
        .stat(&target)
        .await
        .expect("stat after snapshot");
    assert_eq!(folded.content_ref(), Some(appended));
    let grant = harness
        .client
        .create_download(&target)
        .await
        .expect("download");
    assert_eq!(&grant.content_ref, appended);
    let mut downloaded = Vec::new();
    let written = harness
        .client
        .download_via_presigned_url(&grant, &mut downloaded)
        .await
        .expect("download appended extents");
    assert_eq!(written, payload.len() as u64);
    assert_eq!(downloaded, payload);

    harness.server.abort();
    delete_everything_under_the_run_prefix(&store_config).await;
}

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
    let proxy_cap = std::num::NonZeroU64::new(PROXY_CAP_BYTES).expect("nonzero proxy cap");
    server_config.max_upload_bytes = proxy_cap;
    server_config.max_download_bytes = Some(proxy_cap);
    let harness = start_server(server_config).await;

    let namespace = "gcs-capabilities";
    let namespace_id = NamespaceId::parse(namespace).expect("valid namespace id");
    harness
        .client
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    let capabilities = harness
        .client
        .get_capabilities()
        .await
        .expect("fetch capabilities");
    assert!(capabilities.supports("filesystem.uploads.direct_put"));
    assert!(capabilities.supports("filesystem.downloads.direct_get"));
    assert!(
        !capabilities.supports("filesystem.uploads.direct_multipart"),
        "this adapter signs no multipart for GCS, so the key must be absent"
    );

    assert_gcs_completion_judges_the_object_that_is_there(&harness.client, &namespace_id).await;
    assert_gcs_signed_writes_stay_under_the_configured_prefix(&harness, &store_config).await;
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
        .create_direct_put_upload_target(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let (checksum_algorithm, access) = direct_put_of(&begin);
    assert_eq!(checksum_algorithm, ChecksumAlgorithm::Crc32c);
    client
        .upload_via_presigned_url(access, bytes)
        .await
        .expect("upload the promised bytes");

    let complete = client
        .complete_upload(
            namespace_id,
            &begin.upload_id,
            &CompleteUploadBody::DirectPut {
                content: direct_put_claim(bytes, checksum_algorithm),
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

/// Every object a signed write creates is written beneath the deployment's
/// configured key prefix, so a capability cannot address the bucket outside
/// this deployment's scope.
async fn assert_gcs_signed_writes_stay_under_the_configured_prefix(
    harness: &crate::common::TestServer,
    store_config: &StoreConfig,
) {
    let namespace_id = NamespaceId::parse("gcs-capabilities").expect("valid namespace id");
    let bytes = b"gcs prefix scoping\n";
    let begin = harness
        .client
        .create_direct_put_upload_target(&namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let (_, access) = direct_put_of(&begin);
    let ObjectTransfer::PresignedUrl { url, .. } = access else {
        panic!("expected a presigned URL");
    };
    let StoreConfig::GcpGcs(GcpGcsStoreConfig {
        bucket, key_prefix, ..
    }) = store_config
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
        .upload_via_presigned_url(access, bytes)
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

/// A capability stops working when it expires, which is what bounds the
/// damage of one leaking.
async fn assert_gcs_expired_capability_is_refused(client: &Client, namespace_id: &NamespaceId) {
    let bytes = b"gcs expiry\n";
    let begin = client
        .create_direct_put_upload_target(namespace_id, Some(bytes.len() as u64))
        .await
        .expect("begin direct put");
    let (_, access) = direct_put_of(&begin);
    let ObjectTransfer::PresignedUrl { url, .. } = access else {
        panic!("expected a presigned URL");
    };

    // Rewrite the capability's own lifetime to one already spent. The
    // signature covered the original, so this is refused twice over — which
    // is the point: neither the expiry nor the signature can be edited in
    // flight.
    let expired = url.replace("X-Goog-Expires=900", "X-Goog-Expires=1");
    assert_ne!(&expired, url, "the capability did not carry an expiry");
    let response = raw_agent().put(&expired).send_bytes(bytes);
    response.expect_err("a capability with an edited lifetime was accepted");
}

/// An object past the deployment's proxy caps moves only directly, in both
/// directions, and the proxy still refuses it.
async fn assert_gcs_cap_bound_object_moves_only_directly(
    harness: &crate::common::TestServer,
    namespace: &str,
) {
    let payload: Vec<u8> = (0..251_u8)
        .cycle()
        .take(loonfs_types::usize_from_u64(4 * PROXY_CAP_BYTES))
        .collect();
    let target = NamespacePath::parse(namespace, "/cap-bound.bin").expect("cap-bound target");

    // This adapter signs no multipart for GCS, so the whole-object write is
    // the only direct transport on offer — and the proxy will not take it.
    harness
        .client
        .put_file_with_options(
            &target,
            &payload,
            &loonfs_test_support::test_actor(),
            &put_options("gcs-cap-bound"),
        )
        .await
        .expect("an object past the proxy cap takes the whole-object direct write");

    let grant = harness
        .client
        .create_download(&target)
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
    // `read_file` is the plain proxied read with no fallback, so this
    // asks the proxy directly rather than re-running the ladder.
    expect_client_rejection(
        harness.client.read_file(&target).await,
        "proxied read of an object past the read cap",
    );
}

fn put_options(commit_id: &str) -> loonfs_client::PutFileOptions {
    loonfs_client::PutFileOptions {
        behavior: DestinationBehavior::NoReplace,
        commit: loonfs_types::options::CommitOptions {
            preconditions: Vec::new(),
            commit_id: Some(CommitId::parse(commit_id).expect("valid commit id")),
            message: None,
        },
        expected_inode_id: None,
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
