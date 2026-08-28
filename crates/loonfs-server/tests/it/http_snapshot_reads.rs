//! HTTP reads pinned to snapshot leases.

#![allow(clippy::panic)]

use crate::common::http_split_support::{replace_file_options, test_config};
use async_trait::async_trait;
use loonfs_api::v0::{BeginDownloadResponse, ListChangesResponse};
use loonfs_api::{
    ApiError, ChangeSeq, CreateCheckpointRequest, ListPathEntriesResponse, PathEntry,
    SnapshotSummary,
};
use loonfs_client::{Client, ClientConfig, DeleteOptions, NamespacePath, PutFileOptions};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::presign::{
    DirectGetIssuer, DirectTransferIssuers, PresignedGetRequest, PresignedUrl,
};
use loonfs_objectstore::{ObjectStoreError, SharedObjectStore};
use loonfs_server::app_with_test_transfers;
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::namespace_id;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::tempdir;

type ApiResult<T> = Result<T, (u16, ApiError)>;

struct SnapshotServer {
    client: Client,
    server_url: String,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Debug)]
struct StubGetIssuer;

#[async_trait]
impl DirectGetIssuer for StubGetIssuer {
    async fn presign_get(
        &self,
        request: PresignedGetRequest<'_>,
        _now: SystemTime,
    ) -> Result<PresignedUrl, ObjectStoreError> {
        Ok(PresignedUrl {
            method: "GET".to_owned(),
            url: format!("https://objects.invalid/{}", request.object_key),
            headers: BTreeMap::new(),
            expires_at_ms: u64::MAX,
        })
    }
}

async fn start_snapshot_server(root: &Path, writer_id: &str) -> SnapshotServer {
    let config = test_config(root.to_path_buf(), writer_id, writer_id);
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(root).expect("construct local store"));
    let transfers = DirectTransferIssuers::read_only(Arc::new(StubGetIssuer));
    let router = app_with_test_transfers(config, store, transfers)
        .await
        .expect("build app");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let server_url = format!("http://{addr}");
    let client = Client::new(ClientConfig {
        server_url: server_url.clone(),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("client config");
    SnapshotServer {
        client,
        server_url,
        server,
    }
}

fn query_url(base: &str, route: &str, params: &[(&str, &str)]) -> String {
    let query = params
        .iter()
        .fold(
            form_urlencoded::Serializer::new(String::new()),
            |mut query, (key, value)| {
                query.append_pair(key, value);
                query
            },
        )
        .finish();
    format!("{base}{route}?{query}")
}

fn get_json<T: DeserializeOwned>(url: &str) -> ApiResult<T> {
    retry_result_on_macos_teardown_einval(|| {
        decode_json_response(
            raw_agent()
                .get(url)
                .set("authorization", "Bearer test-token")
                .call(),
        )
    })
}

fn get_bytes(url: &str) -> ApiResult<Vec<u8>> {
    retry_result_on_macos_teardown_einval(|| {
        match raw_agent()
            .get(url)
            .set("authorization", "Bearer test-token")
            .call()
        {
            Ok(response) => {
                let mut bytes = Vec::new();
                response
                    .into_reader()
                    .read_to_end(&mut bytes)
                    .expect("read byte response");
                Ok(bytes)
            }
            Err(ureq::Error::Status(status, response)) => Err((
                status,
                serde_json::from_reader(response.into_reader()).expect("decode error response"),
            )),
            Err(ureq::Error::Transport(error)) => panic!("HTTP transport failed: {error}"),
        }
    })
}

fn post_json<T: DeserializeOwned>(url: &str, body: serde_json::Value) -> ApiResult<T> {
    retry_result_on_macos_teardown_einval(|| {
        decode_json_response(
            raw_agent()
                .post(url)
                .set("authorization", "Bearer test-token")
                .send_json(body.clone()),
        )
    })
}

fn decode_json_response<T: DeserializeOwned>(
    response: Result<ureq::Response, ureq::Error>,
) -> ApiResult<T> {
    match response {
        Ok(response) => {
            Ok(serde_json::from_reader(response.into_reader()).expect("decode success response"))
        }
        Err(ureq::Error::Status(status, response)) => Err((
            status,
            serde_json::from_reader(response.into_reader()).expect("decode error response"),
        )),
        Err(ureq::Error::Transport(error)) => panic!("HTTP transport failed: {error}"),
    }
}

fn create_snapshot(server_url: &str, namespace: &str, name: &str, ttl_ms: u64) -> SnapshotSummary {
    post_json(
        &format!("{server_url}/v0/namespaces/{namespace}/snapshots"),
        serde_json::json!({"name": name, "ttl_ms": ttl_ms}),
    )
    .expect("create snapshot")
}

fn release_snapshot(server_url: &str, namespace: &str, snapshot_id: &str) {
    post_json::<loonfs_api::ReleaseSnapshotResponse>(
        &format!("{server_url}/v0/namespaces/{namespace}/snapshots/{snapshot_id}/release"),
        serde_json::json!({}),
    )
    .expect("release snapshot");
}

fn stat_url(server_url: &str, namespace: &str, snapshot_id: Option<&str>) -> String {
    let route = format!("/v0/namespaces/{namespace}/filesystem/entry");
    let mut params = vec![("path", "/keep.txt")];
    if let Some(snapshot_id) = snapshot_id {
        params.push(("snapshot_id", snapshot_id));
    }
    query_url(server_url, &route, &params)
}

fn listing_url(
    server_url: &str,
    namespace: &str,
    snapshot_id: Option<&str>,
    limit: Option<&str>,
    cursor: Option<&str>,
) -> String {
    let route = format!("/v0/namespaces/{namespace}/filesystem/entries");
    let mut params = vec![("path", "/")];
    if let Some(limit) = limit {
        params.push(("limit", limit));
    }
    if let Some(cursor) = cursor {
        params.push(("cursor", cursor));
    }
    if let Some(snapshot_id) = snapshot_id {
        params.push(("snapshot_id", snapshot_id));
    }
    query_url(server_url, &route, &params)
}

fn content_url(
    server_url: &str,
    namespace: &str,
    snapshot_id: Option<&str>,
    revision_no: Option<&str>,
) -> String {
    let route = format!("/v0/namespaces/{namespace}/filesystem/content");
    let mut params = vec![("path", "/keep.txt")];
    if let Some(revision_no) = revision_no {
        params.push(("revision_no", revision_no));
    }
    if let Some(snapshot_id) = snapshot_id {
        params.push(("snapshot_id", snapshot_id));
    }
    query_url(server_url, &route, &params)
}

fn changes_url(
    server_url: &str,
    namespace: &str,
    after_seq: ChangeSeq,
    limit: &str,
    snapshot_id: Option<&str>,
) -> String {
    let route = format!("/v0/namespaces/{namespace}/changes");
    let after_seq = after_seq.to_string();
    let mut params = vec![("after_seq", after_seq.as_str()), ("limit", limit)];
    if let Some(snapshot_id) = snapshot_id {
        params.push(("snapshot_id", snapshot_id));
    }
    query_url(server_url, &route, &params)
}

fn download_url(server_url: &str, namespace: &str, snapshot_id: Option<&str>) -> String {
    let route = format!("/v0/namespaces/{namespace}/filesystem/downloads");
    match snapshot_id {
        Some(snapshot_id) => query_url(server_url, &route, &[("snapshot_id", snapshot_id)]),
        None => format!("{server_url}{route}"),
    }
}

fn entry_paths(listing: &ListPathEntriesResponse) -> BTreeSet<String> {
    listing
        .entries
        .iter()
        .map(|entry| entry.path.as_str().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reads_answer_the_captured_namespace_and_download_target() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_snapshot_server(temp_dir.path(), "snapshot-read-state").await;
    let namespace = namespace_id("snapshot-read-state");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let keep = NamespacePath::parse(namespace.as_str(), "/keep.txt").expect("keep path");
    let deleted = NamespacePath::parse(namespace.as_str(), "/deleted.txt").expect("deleted path");
    harness
        .client
        .put_file_bytes(
            &keep,
            b"captured bytes",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create kept file");
    harness
        .client
        .put_file_bytes(
            &deleted,
            b"deleted after snapshot",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create deleted file");
    let captured_entry = harness
        .client
        .get_path_entry(&keep, &Default::default())
        .await
        .expect("stat captured file");
    let snapshot = create_snapshot(
        &harness.server_url,
        namespace.as_str(),
        "captured-state",
        10_000,
    );

    harness
        .client
        .put_file_bytes(&keep, b"current bytes", &replace_file_options())
        .await
        .expect("replace kept file");
    let added = NamespacePath::parse(namespace.as_str(), "/added.txt").expect("added path");
    harness
        .client
        .put_file_bytes(&added, b"added", &replace_file_options())
        .await
        .expect("add current file");
    harness
        .client
        .delete_path(
            &deleted,
            &DeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("delete captured file");

    let snapshot_id = snapshot.snapshot_id.as_str();
    let pinned_entry: PathEntry = get_json(&stat_url(
        &harness.server_url,
        namespace.as_str(),
        Some(snapshot_id),
    ))
    .expect("snapshot stat");
    assert_eq!(pinned_entry.revision_no(), captured_entry.revision_no());
    let pinned_listing: ListPathEntriesResponse = get_json(&listing_url(
        &harness.server_url,
        namespace.as_str(),
        Some(snapshot_id),
        None,
        None,
    ))
    .expect("snapshot listing");
    assert_eq!(
        entry_paths(&pinned_listing),
        BTreeSet::from(["/deleted.txt".to_owned(), "/keep.txt".to_owned()])
    );
    assert_eq!(
        get_bytes(&content_url(
            &harness.server_url,
            namespace.as_str(),
            Some(snapshot_id),
            None,
        ))
        .expect("snapshot content"),
        b"captured bytes"
    );

    let current_entry: PathEntry =
        get_json(&stat_url(&harness.server_url, namespace.as_str(), None)).expect("current stat");
    assert_ne!(current_entry.revision_no(), captured_entry.revision_no());
    let current_listing: ListPathEntriesResponse = get_json(&listing_url(
        &harness.server_url,
        namespace.as_str(),
        None,
        None,
        None,
    ))
    .expect("current listing");
    assert_eq!(
        entry_paths(&current_listing),
        BTreeSet::from(["/added.txt".to_owned(), "/keep.txt".to_owned()])
    );
    assert_eq!(
        get_bytes(&content_url(
            &harness.server_url,
            namespace.as_str(),
            None,
            None,
        ))
        .expect("current content"),
        b"current bytes"
    );

    let grant: BeginDownloadResponse = post_json(
        &download_url(&harness.server_url, namespace.as_str(), Some(snapshot_id)),
        serde_json::json!({"path": "/keep.txt"}),
    )
    .expect("snapshot download grant");
    assert_eq!(
        grant.revision_no,
        captured_entry.revision_no().expect("file revision")
    );
    assert_eq!(
        &grant.content_ref,
        captured_entry.content_ref().expect("captured content ref")
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_change_feed_stops_at_the_captured_sequence() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_snapshot_server(temp_dir.path(), "snapshot-change-feed").await;
    let namespace = namespace_id("snapshot-change-feed");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    for name in ["one", "two", "three", "four"] {
        let path =
            NamespacePath::parse(namespace.as_str(), &format!("/{name}.txt")).expect("file path");
        harness
            .client
            .put_file_bytes(&path, name.as_bytes(), &replace_file_options())
            .await
            .expect("seed change");
    }
    let snapshot = create_snapshot(&harness.server_url, namespace.as_str(), "feed", 10_000);
    let later = NamespacePath::parse(namespace.as_str(), "/later.txt").expect("later path");
    harness
        .client
        .put_file_bytes(&later, b"later", &replace_file_options())
        .await
        .expect("write after snapshot");

    let mut after_seq = ChangeSeq(0);
    let mut seen = Vec::new();
    loop {
        let page: ListChangesResponse = get_json(&changes_url(
            &harness.server_url,
            namespace.as_str(),
            after_seq,
            "2",
            Some(snapshot.snapshot_id.as_str()),
        ))
        .expect("snapshot change page");
        assert_eq!(page.through_seq, snapshot.head_seq);
        seen.extend(page.changes.iter().map(|change| change.committed_seq));
        match page.next_after_seq {
            Some(next) => {
                assert!(next < snapshot.head_seq);
                after_seq = next;
            }
            None => break,
        }
    }
    assert_eq!(
        seen,
        (1..=snapshot.head_seq.0).map(ChangeSeq).collect::<Vec<_>>()
    );

    let empty: ListChangesResponse = get_json(&changes_url(
        &harness.server_url,
        namespace.as_str(),
        snapshot.head_seq,
        "2",
        Some(snapshot.snapshot_id.as_str()),
    ))
    .expect("empty terminal page");
    assert!(empty.changes.is_empty());
    assert_eq!(empty.through_seq, snapshot.head_seq);
    assert_eq!(empty.next_after_seq, None);

    let above = ChangeSeq(snapshot.head_seq.0 + 1);
    let (status, error) = get_json::<ListChangesResponse>(&changes_url(
        &harness.server_url,
        namespace.as_str(),
        above,
        "2",
        Some(snapshot.snapshot_id.as_str()),
    ))
    .expect_err("cursor above snapshot must fail");
    assert_eq!(status, 400);
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains(&above.to_string()));
    assert!(error.message.contains(&snapshot.head_seq.to_string()));

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reads_enforce_lease_identity_and_revision_rules() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_snapshot_server(temp_dir.path(), "snapshot-read-errors").await;
    let namespace = namespace_id("snapshot-read-errors");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let keep = NamespacePath::parse(namespace.as_str(), "/keep.txt").expect("keep path");
    harness
        .client
        .put_file_bytes(&keep, b"kept", &replace_file_options())
        .await
        .expect("create file");
    let released = create_snapshot(&harness.server_url, namespace.as_str(), "released", 10_000);
    release_snapshot(
        &harness.server_url,
        namespace.as_str(),
        released.snapshot_id.as_str(),
    );

    let released_id = released.snapshot_id.as_str();
    let routes = [
        (
            "stat",
            stat_url(&harness.server_url, namespace.as_str(), Some(released_id)),
            None,
        ),
        (
            "listing",
            listing_url(
                &harness.server_url,
                namespace.as_str(),
                Some(released_id),
                None,
                None,
            ),
            None,
        ),
        (
            "content",
            content_url(
                &harness.server_url,
                namespace.as_str(),
                Some(released_id),
                None,
            ),
            None,
        ),
        (
            "download",
            download_url(&harness.server_url, namespace.as_str(), Some(released_id)),
            Some(serde_json::json!({"path": "/keep.txt"})),
        ),
        (
            "changes",
            changes_url(
                &harness.server_url,
                namespace.as_str(),
                ChangeSeq(0),
                "2",
                Some(released_id),
            ),
            None,
        ),
    ];
    for (name, url, body) in routes {
        let error = match body {
            Some(body) => post_json::<serde_json::Value>(&url, body),
            None => get_json::<serde_json::Value>(&url),
        }
        .expect_err("released snapshot read must fail");
        assert_eq!(error.0, 410, "route {name}");
        assert_eq!(error.1.code, "snapshot_gone", "route {name}");
        assert!(error.1.message.contains("released"), "route {name}");
    }

    let expired = create_snapshot(&harness.server_url, namespace.as_str(), "expired", 50);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (status, error) = get_json::<PathEntry>(&stat_url(
        &harness.server_url,
        namespace.as_str(),
        Some(expired.snapshot_id.as_str()),
    ))
    .expect_err("expired snapshot must fail");
    assert_eq!(status, 410);
    assert_eq!(error.code, "snapshot_gone");
    assert!(error.message.contains("expired"));

    let checkpoint = harness
        .client
        .create_checkpoint(
            &namespace,
            &CreateCheckpointRequest {
                name: "operator".to_owned(),
                ttl_ms: None,
            },
        )
        .await
        .expect("create user checkpoint");
    let (status, error) = get_json::<PathEntry>(&stat_url(
        &harness.server_url,
        namespace.as_str(),
        Some(checkpoint.checkpoint_id.as_str()),
    ))
    .expect_err("user checkpoint is not a snapshot");
    assert_eq!(status, 400);
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("checkpoint, not a snapshot"));

    let unknown = "chk_ffffffffffffffffffffffffffffffff";
    let (status, error) = get_json::<PathEntry>(&stat_url(
        &harness.server_url,
        namespace.as_str(),
        Some(unknown),
    ))
    .expect_err("unknown snapshot must fail");
    assert_eq!(status, 404);
    assert_eq!(error.code, "snapshot_not_found");

    let (status, error) = get_json::<PathEntry>(&stat_url(
        &harness.server_url,
        namespace.as_str(),
        Some("malformed"),
    ))
    .expect_err("malformed snapshot id must fail");
    assert_eq!(status, 400);
    assert_eq!(error.code, "invalid_request");
    assert_eq!(error.param.as_deref(), Some("snapshot_id"));

    let live = create_snapshot(
        &harness.server_url,
        namespace.as_str(),
        "revision-rule",
        10_000,
    );
    for result in [
        get_bytes(&content_url(
            &harness.server_url,
            namespace.as_str(),
            Some(live.snapshot_id.as_str()),
            Some("1"),
        ))
        .map(|_| serde_json::Value::Null),
        post_json::<serde_json::Value>(
            &download_url(
                &harness.server_url,
                namespace.as_str(),
                Some(live.snapshot_id.as_str()),
            ),
            serde_json::json!({"path": "/keep.txt", "revision_no": 1}),
        ),
    ] {
        let (status, error) = result.expect_err("revision and snapshot must conflict");
        assert_eq!(status, 400);
        assert_eq!(error.code, "invalid_request");
        assert!(error.message.contains("revision_no"));
        assert!(error.message.contains("snapshot_id"));
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_page_cursors_resume_one_pinned_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_snapshot_server(temp_dir.path(), "snapshot-pagination").await;
    let namespace = namespace_id("snapshot-pagination");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    for name in ["a", "c", "e", "g"] {
        let path =
            NamespacePath::parse(namespace.as_str(), &format!("/{name}.txt")).expect("file path");
        harness
            .client
            .put_file_bytes(&path, name.as_bytes(), &replace_file_options())
            .await
            .expect("seed file");
    }
    let snapshot = create_snapshot(
        &harness.server_url,
        namespace.as_str(),
        "pagination",
        10_000,
    );
    let first: ListPathEntriesResponse = get_json(&listing_url(
        &harness.server_url,
        namespace.as_str(),
        Some(snapshot.snapshot_id.as_str()),
        Some("2"),
        None,
    ))
    .expect("first snapshot page");
    assert_eq!(first.entries.len(), 2);
    let mut seen = entry_paths(&first);
    let mut cursor = first.next_cursor.expect("first page cursor");

    for name in ["a", "e"] {
        let removed = NamespacePath::parse(namespace.as_str(), &format!("/{name}.txt"))
            .expect("removed path");
        harness
            .client
            .delete_path(
                &removed,
                &DeleteOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("remove captured file");
    }
    for name in ["b", "d"] {
        let path =
            NamespacePath::parse(namespace.as_str(), &format!("/{name}.txt")).expect("added path");
        harness
            .client
            .put_file_bytes(&path, name.as_bytes(), &replace_file_options())
            .await
            .expect("add current file");
    }

    loop {
        let page: ListPathEntriesResponse = get_json(&listing_url(
            &harness.server_url,
            namespace.as_str(),
            Some(snapshot.snapshot_id.as_str()),
            Some("2"),
            Some(&cursor),
        ))
        .expect("resumed snapshot page");
        seen.extend(entry_paths(&page));
        match page.next_cursor {
            Some(next) => cursor = next,
            None => break,
        }
    }
    assert_eq!(
        seen,
        BTreeSet::from([
            "/a.txt".to_owned(),
            "/c.txt".to_owned(),
            "/e.txt".to_owned(),
            "/g.txt".to_owned(),
        ])
    );

    harness.server.abort();
}
