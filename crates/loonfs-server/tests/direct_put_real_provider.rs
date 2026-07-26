// Ignored real-provider tests exercise the full server/client/direct-object-store path.

mod common;

use common::{start_server, test_config};
use loonfs_api::{
    v0::{CompleteUploadRequest, ObjectTransferAccess, ValidatedContentToken},
    ChangeSeq, CommitId, CommitResponse, ContentRef, DestinationBehavior, FilesystemOperation,
    FilesystemOperationRequest, NamespaceId,
};
use loonfs_client::{Client, ClientError, NamespacePath};
use loonfs_server::{ServerConfig, StoreConfig};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

const AUTH_TOKEN: &str = "test-token";
const CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";
const SIGNED_CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";

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
    let content_ref = ContentRef::whole_file_v0(bytes);

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
        .begin_direct_put(&namespace_id, content_ref.clone())
        .await
        .expect("begin direct put");
    let direct_put = begin.direct_put.expect("direct-put access");
    assert_eq!(direct_put.content_ref, content_ref);

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
            &CompleteUploadRequest {
                content_ref: content_ref.clone(),
            },
        )
        .await
        .expect("complete direct-put upload");
    assert_eq!(complete.content_ref, content_ref);
    let validated_content_token = complete
        .validated_content_token
        .expect("completion returns content token");

    let response = post_filesystem_operation(
        &harness.server_url,
        namespace,
        &FilesystemOperationRequest {
            commit_id: CommitId::parse("direct-put-e2e").expect("valid commit id"),
            message: None,
            content_tokens: vec![ValidatedContentToken {
                content_ref: content_ref.clone(),
                token: validated_content_token,
            }],
            operation: FilesystemOperation::PutFile {
                path: target.absolute_path().clone(),
                content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
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
    let content_ref = ContentRef::whole_file_v0(bytes);
    let begin = client
        .begin_direct_put(namespace_id, content_ref.clone())
        .await
        .expect("begin wrong-bytes direct put");
    let direct_put = begin.direct_put.expect("wrong-bytes direct-put access");
    assert_eq!(direct_put.content_ref, content_ref);

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
                &CompleteUploadRequest { content_ref },
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
    let content_ref = ContentRef::whole_file_v0(bytes);
    let begin = client
        .begin_direct_put(namespace_id, content_ref.clone())
        .await
        .expect("begin missing-checksum direct put");
    let direct_put = begin
        .direct_put
        .expect("missing-checksum direct-put access");
    assert_eq!(direct_put.content_ref, content_ref);
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
                &CompleteUploadRequest { content_ref },
            )
            .await,
        "complete missing-checksum direct put",
    );
}

async fn assert_direct_put_is_no_replace(client: &Client, namespace_id: &NamespaceId) {
    let bytes = b"duplicate direct put bytes\n";
    let content_ref = ContentRef::whole_file_v0(bytes);
    let begin = client
        .begin_direct_put(namespace_id, content_ref.clone())
        .await
        .expect("begin duplicate direct put");
    let direct_put = begin.direct_put.expect("duplicate direct-put access");
    assert_eq!(direct_put.content_ref, content_ref);

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
            &CompleteUploadRequest { content_ref },
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

fn post_filesystem_operation(
    server_url: &str,
    namespace: &str,
    request: &FilesystemOperationRequest,
) -> CommitResponse {
    let response = ureq::post(&format!(
        "{server_url}/v0/namespaces/{namespace}/filesystem/operations"
    ))
    .set("authorization", &format!("Bearer {AUTH_TOKEN}"))
    .send_json(request)
    .expect("post filesystem operation");

    serde_json::from_reader(response.into_reader()).expect("decode filesystem operation response")
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
