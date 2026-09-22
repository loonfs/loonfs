//! Hosted inline publication, retry identity, and admission contracts.

use super::*;
use loonfs_api::v0::FilesystemChange;
use loonfs_api::{
    Commit, ContentRef, FEATURE_COMMIT_INLINE_CONTENT,
    LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION,
};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_test_support::stores::{RecordedOperation, RecordingStore};
use serde_json::{json, Value};
use tower::ServiceExt;

struct Harness {
    router: axum::Router,
    state: AppState,
    store: Arc<RecordingStore<LocalFsStore>>,
    namespace: NamespaceId,
    _directory: tempfile::TempDir,
}

impl Harness {
    async fn new(threshold: Option<usize>, segment_budget: usize) -> Self {
        Self::with_policy(crate::config::InlineContentOverrides {
            inline_content_threshold_bytes: threshold,
            inline_content_segment_budget_bytes: Some(segment_budget),
            ..Default::default()
        })
        .await
    }

    async fn with_policy(inline_content: crate::config::InlineContentOverrides) -> Self {
        let directory = tempdir().expect("directory");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let mut config = test_config(directory.path(), "inline-host");
        config.inline_content = inline_content;
        config.maintenance = crate::config::MaintenanceMode::Disabled;
        config.grep = Default::default();
        let (router, state) = app(config, options_with_store(store.clone()))
            .await
            .expect("app");
        let namespace = namespace_id("hosted-inline");
        state
            .writer
            .create_namespace(
                &namespace,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("namespace");
        state
            .writer
            .create_directory(
                &namespace,
                "/warmup",
                loonfs::CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("acquire writer epoch");
        store.reset();
        Self {
            router,
            state,
            store,
            namespace,
            _directory: directory,
        }
    }

    async fn request(&self, method: &str, uri: &str, body: Value) -> (StatusCode, Bytes) {
        let response = self
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("authorization", "Bearer test-token")
                    .header("Loonfs-Actor", "inline-test")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, bytes)
    }

    async fn commit(&self, request: Value) -> (StatusCode, Value) {
        let (status, bytes) = self
            .request(
                "POST",
                &format!("/v0/namespaces/{}/commits", self.namespace),
                request,
            )
            .await;
        (
            status,
            serde_json::from_slice(&bytes).expect("JSON response"),
        )
    }

    async fn read(&self, path: &str, expected: &[u8]) {
        let (status, bytes) = self
            .request(
                "GET",
                &format!(
                    "/v0/namespaces/{}/filesystem/content?path={path}",
                    self.namespace
                ),
                Value::Null,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(bytes, expected);
    }

    fn family_requests(&self, family: DurableObjectFamily) -> usize {
        self.store
            .snapshot()
            .iter()
            .filter(|operation| {
                parse_object_key(operation.key()).is_some_and(|key| key.family() == family)
            })
            .count()
    }
}

fn inline_request(commit_id: &str, path: &str, encoded: &str) -> Value {
    json!({"commit_id": commit_id, "operations": [{"kind": "put_file", "path": path, "inline_content": encoded}]})
}

fn created_content(response: &Value) -> (InodeId, ContentRef) {
    let commit: Commit = serde_json::from_value(response.clone()).expect("commit");
    match &commit.events[0] {
        FilesystemChange::FileCreated {
            inode_id,
            content_ref,
            ..
        } => (*inode_id, content_ref.clone()),
        event => panic!("expected file creation, got {event:?}"),
    }
}

#[tokio::test]
async fn default_inline_commits_write_only_wal_and_replay_by_bytes() {
    let harness = Harness::with_policy(Default::default()).await;
    let (status, body) = harness
        .request("GET", "/v0/capabilities", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    let capabilities: CapabilityDocument = serde_json::from_slice(&body).expect("capabilities");
    assert_eq!(
        capabilities.features.get(FEATURE_COMMIT_INLINE_CONTENT),
        Some(&true)
    );
    assert_eq!(
        capabilities
            .limits
            .get(LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION),
        Some(&(64 * 1024))
    );
    let request = inline_request("small", "/file", "c2FtZQ==");
    let (status, first) = harness.commit(request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(harness.store.count(OperationClass::Put), 1);
    assert_eq!(harness.family_requests(DurableObjectFamily::WalSegment), 1);
    assert_eq!(
        harness.family_requests(DurableObjectFamily::UploadSession),
        0
    );
    assert_eq!(harness.family_requests(DurableObjectFamily::ContentBlob), 0);
    let (_, reference) = created_content(&first);
    assert_eq!(reference.size_bytes, 4);
    assert_eq!(reference.checksum, loonfs_api::Checksum::sha256(b"same"));
    assert_eq!(reference.owner_namespace_id, harness.namespace);
    harness.read("/file", b"same").await;
    harness.store.reset();
    let (status, replay) = harness.commit(request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, first);
    let (status, conflict) = harness
        .commit(inline_request("small", "/file", "ZGlmZg=="))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["code"], ErrorCode::CommitIdReuseConflict.as_str());
    assert_eq!(harness.store.count(OperationClass::Put), 0);

    let (_, upload) = harness
        .request(
            "POST",
            &format!("/v0/namespaces/{}/uploads", harness.namespace),
            json!({"mode": "service_proxied"}),
        )
        .await;
    let upload: Value = serde_json::from_slice(&upload).expect("upload");
    let upload_uri = format!(
        "/v0/namespaces/{}/uploads/{}",
        harness.namespace,
        upload["upload_id"].as_str().expect("upload id")
    );
    let response = harness
        .router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(format!("{upload_uri}/content"))
                .header("authorization", "Bearer test-token")
                .body(axum::body::Body::from("same"))
                .expect("upload request"),
        )
        .await
        .expect("upload response");
    assert_eq!(response.status(), StatusCode::OK);
    let (status, completed) = harness
        .request(
            "POST",
            &format!("{upload_uri}/complete"),
            json!({"mode": "service_proxied"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let completed: loonfs_api::v0::UploadSession =
        serde_json::from_slice(&completed).expect("completion");
    let loonfs_api::v0::UploadSessionStatus::Completed {
        content_ref,
        content_token,
        ..
    } = completed.status
    else {
        panic!("expected completion")
    };
    let mut staged = request;
    staged["operations"][0]
        .as_object_mut()
        .expect("operation")
        .remove("inline_content");
    staged["operations"][0]["content_ref"] = json!(content_ref);
    staged["content_tokens"] = json!(content_token.into_iter().collect::<Vec<_>>());
    harness.store.reset();
    let (status, conflict) = harness.commit(staged).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["code"], ErrorCode::CommitIdReuseConflict.as_str());
    assert_eq!(harness.store.count(OperationClass::Put), 0);
    harness.state.writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn inline_sources_and_capabilities_follow_the_policy_before_any_write() {
    for threshold in [Some(4), None] {
        let harness = Harness::new(threshold, 1).await;
        let (status, body) = harness
            .request("GET", "/v0/capabilities", Value::Null)
            .await;
        assert_eq!(status, StatusCode::OK);
        let capabilities: CapabilityDocument = serde_json::from_slice(&body).expect("capabilities");
        assert_eq!(
            capabilities.features.get(FEATURE_COMMIT_INLINE_CONTENT),
            threshold.map(|_| &true)
        );
        assert_eq!(
            capabilities
                .limits
                .get(LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION)
                .copied(),
            threshold.map(|limit| limit as u64)
        );
        for mut operation in [
            json!({"kind": "put_file", "path": "/file"}),
            json!({"kind": "create_file_by_inode", "parent_inode_id": "ino_1", "display_name": "file"}),
            json!({"kind": "put_file_revision_by_inode", "inode_id": "ino_2", "expected_revision_no": 1}),
        ] {
            let reference = loonfs_test_support::ids::content_ref(b"same");
            let missing = operation.clone();
            operation["inline_content"] = json!("c2FtZQ==");
            let inline = operation.clone();
            operation["content_ref"] = json!(reference);
            let both = operation;
            let mut oversized = inline.clone();
            oversized["inline_content"] = json!("bGFyZ2U=");
            let mut invalid_base64 = inline.clone();
            invalid_base64["inline_content"] = json!("!");
            let mut unknown = inline.clone();
            unknown["checksum"] = json!("client-drawn");
            let mut cases = vec![
                (missing, ErrorCode::InvalidRequest),
                (both, ErrorCode::InvalidRequest),
                (invalid_base64, ErrorCode::InvalidRequest),
                (unknown, ErrorCode::InvalidRequest),
            ];
            if threshold.is_some() {
                cases.push((oversized, ErrorCode::InvalidRequest));
            } else {
                cases.push((inline, ErrorCode::NotSupported));
            }
            for (operation, code) in cases {
                let (status, error) = harness
                    .commit(json!({"commit_id": "rejected", "operations": [operation]}))
                    .await;
                assert_eq!(status, status_for_core_error_code(code), "{error}");
                assert_eq!(error["code"], code.as_str());
                if code == ErrorCode::NotSupported {
                    assert_eq!(error["feature"], FEATURE_COMMIT_INLINE_CONTENT);
                }
            }
        }
        assert_eq!(harness.store.count(OperationClass::Put), 0);
        harness.state.writer.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn inode_inline_operations_and_segment_fallback_keep_retry_identity() {
    let harness = Harness::new(Some(4), 4).await;
    let request = json!({"commit_id": "two-files", "operations": [
        {"kind": "create_file_by_inode", "parent_inode_id": "ino_1", "display_name": "first", "inline_content": "c2FtZQ=="},
        {"kind": "put_file", "path": "/second", "inline_content": "c2FtZQ=="}
    ]});
    let (status, first) = harness.commit(request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(harness.family_requests(DurableObjectFamily::ContentBlob), 1);
    let wal_keys: Vec<_> = harness
        .store
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put { key, .. }
                if parse_object_key(&key)
                    .is_some_and(|key| key.family() == DurableObjectFamily::WalSegment) =>
            {
                Some(key)
            }
            _ => None,
        })
        .collect();
    assert_eq!(wal_keys.len(), 1);
    let wal = harness
        .store
        .get(&wal_keys[0], None)
        .await
        .expect("get")
        .expect("WAL");
    let decoded = loonfs_api::wire::wal::decode_wal_segment_envelope_zstd(&wal).expect("decode");
    assert_eq!(decoded.payload().records[0].inline_content.len(), 1);
    harness.read("/first", b"same").await;
    harness.read("/second", b"same").await;
    let (status, replay) = harness.commit(request).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(first, replay);
    let (inode_id, _) = created_content(&first);
    let revision = json!({"commit_id": "revision", "operations": [{"kind": "put_file_revision_by_inode", "inode_id": format!("ino_{inode_id}"), "expected_revision_no": 1, "inline_content": ""}]});
    let (status, first) = harness.commit(revision.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    harness.read("/first", b"").await;
    let (status, replay) = harness.commit(revision).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(first, replay);
    harness.state.writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn rust_client_small_puts_use_one_request_only_when_inline_is_advertised() {
    for threshold in [Some(4), None] {
        let harness = Harness::new(threshold, 1024).await;
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let router = harness.router.clone().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                observed.fetch_add(1, Ordering::SeqCst);
                next.run(request)
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let address = listener.local_addr().expect("address");
        let server =
            tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });
        let client = Client::new(ClientConfig {
            server_url: format!("http://{address}"),
            auth_token: Some("test-token".into()),
            request_timeout_ms: None,
            disable_transient_retry: true,
            ca_cert_path: None,
        })
        .expect("client");
        client.get_capabilities().await.expect("capabilities");
        requests.store(0, Ordering::SeqCst);
        let spec = NamespacePath::parse(harness.namespace.as_str(), "/file").expect("path");
        let mut options = PutFileOptions::new(loonfs_test_support::test_actor());
        options.commit.commit_id = Some(CommitId::parse("client-small").expect("commit id"));
        let first = client
            .put_file_bytes(&spec, b"same", &options)
            .await
            .expect("put");
        assert_eq!(
            requests.swap(0, Ordering::SeqCst),
            if threshold.is_some() { 1 } else { 4 }
        );
        let retry = client.put_file_bytes(&spec, b"same", &options).await;
        if threshold.is_some() {
            assert_eq!(retry.expect("replay"), first);
            assert_eq!(requests.load(Ordering::SeqCst), 1);
        } else {
            assert!(
                matches!(retry, Err(ClientError::Api { code, .. }) if code == ErrorCode::CommitIdReuseConflict.as_str())
            );
        }
        harness.state.writer.shutdown().await.expect("shutdown");
        server.abort();
    }
}
