// Ignored real-provider tests exercise the full server/client/direct-object-store path.

use crate::common::{start_server, test_config};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_api::{
    v0::{
        CompleteUploadRequest, DirectMultipartContentClaim, DirectMultipartUploadOptions,
        DirectPutContentClaim, ObjectTransferAccess, UploadPartChecksumClaim,
        ValidatedContentToken,
    },
    ChangeSeq, CommitId, CommitRequest, CommitResponse, DestinationBehavior, FilesystemOperation,
    NamespaceId, StorageChecksum,
};
use loonfs_client::{Client, ClientError, NamespacePath, PayloadSource};
use loonfs_objectstore::ObjectStore;
use loonfs_server::{ServerConfig, StoreConfig};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

const AUTH_TOKEN: &str = "test-token";
const CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";
const SIGNED_CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";
/// The part size the reference server hands out. A live payload is cut to
/// this so it lands on a known part count; the assertions check the server
/// still says so.
const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

/// What a direct-put client declares about bytes it is holding.
fn direct_put_claim(bytes: &[u8]) -> DirectPutContentClaim {
    DirectPutContentClaim {
        size_bytes: bytes.len() as u64,
        sha256: StorageChecksum::sha256(bytes).value,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_direct_put_real_provider_round_trip() {
    let config = AwsS3DirectPutConfig::from_env().expect("load AWS S3 direct-put environment");
    direct_put_round_trip(test_config(
        StoreConfig::AwsS3 {
            bucket: config.bucket,
            region: config.region,
            endpoint_url: config.endpoint,
            access_key_id: config.access_key_id.into(),
            secret_access_key: config.secret_access_key.into(),
            session_token: config.session_token.map(Into::into),
            key_prefix: Some(config.prefix),
            force_path_style: false,
        },
        AUTH_TOKEN,
        CONTENT_TOKEN_SECRET,
        "direct-put-aws-s3",
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_direct_put_real_provider_round_trip() {
    let config =
        CloudflareR2DirectPutConfig::from_env().expect("load Cloudflare R2 direct-put environment");
    direct_put_round_trip(test_config(
        StoreConfig::CloudflareR2 {
            bucket: config.bucket,
            account_id: config.account_id,
            endpoint_url: config.endpoint,
            access_key_id: config.access_key_id.into(),
            secret_access_key: config.secret_access_key.into(),
            key_prefix: Some(config.prefix),
        },
        AUTH_TOKEN,
        CONTENT_TOKEN_SECRET,
        "direct-put-r2",
    ))
    .await;
}

async fn direct_put_round_trip(config: ServerConfig) {
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

    harness
        .client
        .create_namespace(&namespace_id)
        .await
        .expect("create namespace");

    assert_wrong_direct_put_bytes_rejected(&harness.client, &namespace_id).await;
    assert_direct_put_requires_signed_checksum_header(&harness.client, &namespace_id).await;
    assert_direct_put_is_no_replace(&harness.client, &namespace_id).await;

    let begin = harness
        .client
        .begin_direct_put(&namespace_id, direct_put_claim(bytes))
        .await
        .expect("begin direct put");
    let direct_put = begin.direct_put.expect("direct-put access");
    // The server minted the identity; the client learns it here and names it
    // everywhere afterwards.
    let content_ref = direct_put.content_ref.clone();
    assert_eq!(content_ref.size_bytes, bytes.len() as u64);
    assert_eq!(
        content_ref.whole_file_sha256.as_deref(),
        Some(content_ref.storage_checksum.value.as_str())
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
            &begin.upload_id,
            &CompleteUploadRequest::for_content_ref(content_ref.clone()),
        )
        .await
        .expect("complete direct-put upload");
    assert_eq!(complete.content_ref, content_ref);
    let validated_content_token = complete
        .validated_content_token
        .expect("completion returns content token");

    let response = post_commit(
        &harness.server_url,
        namespace,
        &CommitRequest {
            commit_id: CommitId::parse("direct-put-e2e").expect("valid commit id"),
            message: None,
            content_tokens: vec![ValidatedContentToken {
                content_ref: content_ref.clone(),
                token: validated_content_token,
            }],
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

    harness.server.abort();
}

async fn assert_wrong_direct_put_bytes_rejected(client: &Client, namespace_id: &NamespaceId) {
    let bytes = b"expected direct put bytes\n";
    let wrong_bytes = b"wrong direct put bytes\n";
    let begin = client
        .begin_direct_put(namespace_id, direct_put_claim(bytes))
        .await
        .expect("begin wrong-bytes direct put");
    let direct_put = begin.direct_put.expect("wrong-bytes direct-put access");
    let content_ref = direct_put.content_ref.clone();

    expect_client_rejection(
        client
            .upload_via_presigned_url(&direct_put.access, wrong_bytes)
            .await,
        "wrong-bytes direct put",
    );
    expect_client_rejection(
        client
            .complete_upload(
                namespace_id,
                &begin.upload_id,
                &CompleteUploadRequest::for_content_ref(content_ref),
            )
            .await,
        "complete wrong-bytes direct put",
    );
}

async fn assert_direct_put_requires_signed_checksum_header(
    client: &Client,
    namespace_id: &NamespaceId,
) {
    let bytes = b"direct put bytes without checksum header\n";
    let begin = client
        .begin_direct_put(namespace_id, direct_put_claim(bytes))
        .await
        .expect("begin missing-checksum direct put");
    let direct_put = begin
        .direct_put
        .expect("missing-checksum direct-put access");
    let content_ref = direct_put.content_ref.clone();
    let access_without_checksum =
        presigned_access_without_header(&direct_put.access, SIGNED_CHECKSUM_HEADER);

    expect_client_rejection(
        client
            .upload_via_presigned_url(&access_without_checksum, bytes)
            .await,
        "missing-checksum direct put",
    );
    expect_client_rejection(
        client
            .complete_upload(
                namespace_id,
                &begin.upload_id,
                &CompleteUploadRequest::for_content_ref(content_ref),
            )
            .await,
        "complete missing-checksum direct put",
    );
}

async fn assert_direct_put_is_no_replace(client: &Client, namespace_id: &NamespaceId) {
    let bytes = b"duplicate direct put bytes\n";
    let begin = client
        .begin_direct_put(namespace_id, direct_put_claim(bytes))
        .await
        .expect("begin duplicate direct put");
    let direct_put = begin.direct_put.expect("duplicate direct-put access");
    let content_ref = direct_put.content_ref.clone();

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
            &begin.upload_id,
            &CompleteUploadRequest::for_content_ref(content_ref),
        )
        .await
        .expect("complete first direct put");
    assert!(complete.validated_content_token.is_some());
}

fn presigned_access_without_header(
    access: &ObjectTransferAccess,
    header: &str,
) -> ObjectTransferAccess {
    match access {
        ObjectTransferAccess::PresignedUrl {
            method,
            url,
            headers,
            expires_at_ms,
        } => {
            let mut headers = headers.clone();
            assert!(
                headers.remove(header).is_some(),
                "presigned access did not include expected header `{header}`"
            );
            ObjectTransferAccess::PresignedUrl {
                method: method.clone(),
                url: url.clone(),
                headers,
                expires_at_ms: *expires_at_ms,
            }
        }
    }
}

fn expect_client_rejection<T>(result: Result<T, ClientError>, context: &str) {
    match result {
        Ok(_) => unreachable!("{context} unexpectedly succeeded"),
        Err(ClientError::Api { .. } | ClientError::Http(_)) => {}
        Err(error) => {
            unreachable!("{context} failed with unexpected client-side error: {error}")
        }
    }
}

fn post_commit(server_url: &str, namespace: &str, request: &CommitRequest) -> CommitResponse {
    let response = ureq::post(&format!("{server_url}/v0/namespaces/{namespace}/commits"))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_direct_multipart_real_provider_round_trip() {
    let config = AwsS3DirectPutConfig::from_env().expect("load AWS S3 direct-put environment");
    direct_multipart_round_trip(test_config(
        StoreConfig::AwsS3 {
            bucket: config.bucket,
            region: config.region,
            endpoint_url: config.endpoint,
            access_key_id: config.access_key_id.into(),
            secret_access_key: config.secret_access_key.into(),
            session_token: config.session_token.map(Into::into),
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
            access_key_id: config.access_key_id.into(),
            secret_access_key: config.secret_access_key.into(),
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
    let whole_object = StorageChecksum::crc64nvme(&payload);
    // Begin declares nothing about the payload. A one-pass uploader could
    // not fill in a length or a digest here, and this session proves it does
    // not have to: the claim arrives with the completion below.
    let begin = harness
        .client
        .begin_direct_multipart(&namespace_id, DirectMultipartUploadOptions::default())
        .await
        .expect("begin direct multipart");
    let upload_id = begin.upload_id.clone();
    let multipart = begin.direct_multipart.expect("multipart geometry");
    assert_eq!(
        multipart.part_size_bytes as usize, part_size,
        "the deployment's part geometry is the one this payload was cut to"
    );

    let chunks: Vec<&[u8]> = payload.chunks(part_size).collect();
    let claims: Vec<UploadPartChecksumClaim> = chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| UploadPartChecksumClaim {
            part_number: index as u32 + 1,
            crc64nvme: StorageChecksum::crc64nvme(chunk).value,
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
        let crc64nvme = claims[index].crc64nvme.clone();
        let chunk = Bytes::copy_from_slice(chunks[index]);
        in_flight.spawn(async move {
            client
                .upload_part_via_presigned_url(part.part_number, &part.access, crc64nvme, chunk)
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
            claims[1].crc64nvme.clone(),
            Bytes::copy_from_slice(chunks[1]),
        )
        .await
        .expect("re-uploading a part is allowed and last-write-wins");
    parts.retain(|part| part.part_number != repeated.part_number);
    parts.push(replaced);
    parts.sort_by_key(|part| part.part_number);

    // The claim rides with the completion, and the identity comes back with
    // the answer: the client never named the object it wrote.
    let request = CompleteUploadRequest::for_multipart(
        DirectMultipartContentClaim {
            size_bytes: payload.len() as u64,
            crc64nvme: whole_object.value.clone(),
        },
        parts,
    );
    let complete = harness
        .client
        .complete_upload(&namespace_id, &upload_id, &request)
        .await
        .expect("complete the multipart upload");
    let content_ref = complete.content_ref.clone();
    assert_eq!(content_ref.size_bytes, payload.len() as u64);
    assert_eq!(content_ref.storage_checksum, whole_object);
    assert!(
        content_ref.whole_file_sha256.is_none(),
        "a provider-assembled object carries no whole-file sha256"
    );

    // The lost completion. The providers disagree completely about what a
    // replayed completion means — AWS S3 replays a success with no checksum
    // in it, Cloudflare R2 answers `NoSuchUpload` — so neither answer is
    // used. Both reconcile from the durable session and the object.
    let replayed = harness
        .client
        .complete_upload(&namespace_id, &upload_id, &request)
        .await
        .expect("a lost completion is answered, not failed");
    assert_eq!(replayed.content_ref, content_ref);

    let validated_content_token = replayed
        .validated_content_token
        .expect("completion returns content token");
    let response = post_commit(
        &harness.server_url,
        namespace,
        &CommitRequest {
            commit_id: CommitId::parse("direct-multipart-e2e").expect("valid commit id"),
            message: None,
            content_tokens: vec![ValidatedContentToken {
                content_ref: content_ref.clone(),
                token: validated_content_token,
            }],
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
                commit_id: Some(CommitId::parse("multipart-rerun").expect("valid commit id")),
                message: None,
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
                commit_id: Some(CommitId::parse("multipart-rerun").expect("valid commit id")),
                message: None,
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
        commit_id: Some(CommitId::parse(commit_id).expect("valid commit id")),
        message: None,
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
