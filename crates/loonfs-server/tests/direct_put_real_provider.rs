#![allow(clippy::panic, clippy::disallowed_methods)]
// Ignored real-provider tests exercise the full server/client/direct-object-store path.

use loonfs_api::{
    v0::{CompleteUploadRequest, ValidatedContentToken},
    ChangeSeq, CommitId, ContentRef, FilesystemOperation, FilesystemOperationRequest,
    FilesystemOperationResponse, FilesystemPutBehavior,
};
use loonfs_client::{Client, ClientConfig, NamespacePath};
use loonfs_server::{app, RuntimeCacheConfigOverrides, ServerConfig, StoreConfig};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

const AUTH_TOKEN: &str = "test-token";
const CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real AWS S3 credentials"]
async fn aws_s3_direct_put_real_provider_round_trip() {
    let config = AwsS3DirectPutConfig::from_env().expect("load AWS S3 direct-put environment");
    direct_put_round_trip(ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some(AUTH_TOKEN.to_owned()),
        content_token_secret: CONTENT_TOKEN_SECRET.to_owned(),
        writer_id: "direct-put-aws-s3".to_owned(),
        writer_version: "direct-put-aws-s3/0.1.0".to_owned(),
        lease_duration_ms: 60_000,
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        store: StoreConfig::AwsS3 {
            bucket: config.bucket,
            region: config.region,
            endpoint_url: config.endpoint,
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: config.session_token,
            key_prefix: Some(config.prefix),
            force_path_style: Some(false),
        },
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real Cloudflare R2 credentials"]
async fn cloudflare_r2_direct_put_real_provider_round_trip() {
    let config =
        CloudflareR2DirectPutConfig::from_env().expect("load Cloudflare R2 direct-put environment");
    direct_put_round_trip(ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some(AUTH_TOKEN.to_owned()),
        content_token_secret: CONTENT_TOKEN_SECRET.to_owned(),
        writer_id: "direct-put-r2".to_owned(),
        writer_version: "direct-put-r2/0.1.0".to_owned(),
        lease_duration_ms: 60_000,
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        store: StoreConfig::CloudflareR2 {
            bucket: config.bucket,
            account_id: config.account_id,
            endpoint_url: config.endpoint,
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            key_prefix: Some(config.prefix),
        },
    })
    .await;
}

async fn direct_put_round_trip(config: ServerConfig) {
    let harness = start_server(config).await;

    tokio::task::spawn_blocking(move || {
        let namespace = "direct-put-e2e";
        let target = NamespacePath::parse(&format!("{namespace}:/direct-put.txt"))
            .expect("parse direct-put target");
        let bytes = b"direct put through a real provider\n";
        let content_ref = ContentRef::whole_file_v0(bytes);

        let capabilities = harness.client.capabilities().expect("fetch capabilities");
        assert!(capabilities.supports("core.uploads.direct_put"));

        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");

        let begin = harness
            .client
            .begin_direct_put(namespace, content_ref.clone())
            .expect("begin direct put");
        let direct_put = begin.direct_put.expect("direct-put access");
        assert_eq!(direct_put.content_ref, content_ref);

        harness
            .client
            .upload_via_presigned_url(&direct_put.access, bytes)
            .expect("upload bytes through presigned provider URL");

        let complete = harness
            .client
            .complete_upload(
                namespace,
                &begin.upload_id,
                &CompleteUploadRequest {
                    content_ref: content_ref.clone(),
                },
            )
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
                content_tokens: vec![ValidatedContentToken {
                    content_ref: content_ref.clone(),
                    token: validated_content_token,
                }],
                operation: FilesystemOperation::PutFile {
                    path: target.absolute_path.clone(),
                    content_ref,
                    behavior: FilesystemPutBehavior::CreateOnly,
                },
            },
        );
        assert_eq!(response.committed_seq, ChangeSeq(1));

        let loaded = harness.client.read_file_bytes(&target).expect("read file");
        assert_eq!(loaded, bytes);
    })
    .await
    .expect("join blocking task");
}

struct TestServer {
    client: Client,
    server_url: String,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn start_server(config: ServerConfig) -> TestServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app(config).expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let server_url = format!("http://{addr}");

    TestServer {
        client: Client::new(ClientConfig {
            server_url: server_url.clone(),
            auth_token: Some(AUTH_TOKEN.to_owned()),
        }),
        server_url,
        server,
    }
}

fn post_filesystem_operation(
    server_url: &str,
    namespace: &str,
    request: &FilesystemOperationRequest,
) -> FilesystemOperationResponse {
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
