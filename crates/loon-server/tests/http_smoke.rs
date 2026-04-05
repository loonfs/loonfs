use loon_api::{
    sha256_digest,
    v0::{
        CommitAnnotations, CommitOp, CommitOpResult, CommitPrecondition,
        CommitRequest as V0CommitRequest, CompleteUploadRequest,
    },
    ChangeSeq, InodeId,
};
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loon_server::{app, ServerConfig, StoreConfig};
use serde_json::json;
use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_round_trip_supports_namespace_create_and_file_read_write() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loond-test",
        "http-smoke",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let target = NamespacePath::parse("demo:/notes/hello.txt").expect("parse namespace path");
        harness
            .client
            .write_file_bytes(&target, b"hello over http\n")
            .expect("write bytes");

        let entry = harness.client.stat_path(&target).expect("stat path");
        assert_eq!(entry.size_bytes, Some(16));

        let bytes = harness.client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello over http\n");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_create_only_and_copy_preserve_cli_semantics() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loond-test",
        "http-copy-smoke",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let source = NamespacePath::parse("demo:/docs/hello.txt").expect("source");
        harness
            .client
            .put_file_bytes(&source, b"hello over http\n", false)
            .expect("initial create");

        match harness.client.put_file_bytes(&source, b"conflict\n", false) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_conflict"),
            other => panic!("expected path_conflict, got {other:?}"),
        }

        harness
            .client
            .put_file_bytes(&source, b"forced overwrite\n", true)
            .expect("forced overwrite");

        let destination = NamespacePath::parse("demo:/docs/copy.txt").expect("destination");
        harness
            .client
            .copy_path(&source, &destination)
            .expect("copy path");

        let source_entry = harness.client.stat_path(&source).expect("source stat");
        let dest_entry = harness.client.stat_path(&destination).expect("dest stat");
        assert_ne!(source_entry.inode_id, dest_entry.inode_id);
        assert_eq!(
            source_entry.content_manifest_digest,
            dest_entry.content_manifest_digest
        );
        let dest_bytes = harness
            .client
            .read_file_bytes(&destination)
            .expect("read copied file");
        assert_eq!(dest_bytes, b"forced overwrite\n");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_upload_commit_and_change_feed_are_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loond-v0",
        "http-v0-smoke",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        let file_bytes = b"phase-2a over http\n";
        let target = NamespacePath::parse("demo:/uploaded.txt").expect("target");
        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");

        let begin = harness
            .client
            .begin_upload(namespace)
            .expect("begin upload");
        let first_block = harness
            .client
            .upload_block(namespace, &begin.upload_id, 0, file_bytes)
            .expect("upload block");
        let repeated_block = harness
            .client
            .upload_block(namespace, &begin.upload_id, 0, file_bytes)
            .expect("repeat upload block");
        assert_eq!(first_block, repeated_block);
        match harness
            .client
            .upload_block(namespace, &begin.upload_id, 0, b"different bytes")
        {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "upload_block_conflict"),
            other => panic!("expected upload_block_conflict, got {other:?}"),
        }
        let manifest_digest = stage_uploaded_manifest(&harness.client, namespace, file_bytes);

        let mut annotations = CommitAnnotations::new();
        annotations.insert("source".to_owned(), json!("http-smoke"));
        annotations.insert("kind".to_owned(), json!("service-proxied"));
        let commit_request = V0CommitRequest {
            request_id: "req-phase-2a-create-file".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "uploaded.txt".to_owned(),
                content_manifest_digest: manifest_digest.clone(),
            }],
            message: Some("upload over http".to_owned()),
            annotations: Some(annotations.clone()),
        };
        let commit = harness
            .client
            .commit_operations(namespace, &commit_request)
            .expect("commit uploaded file");
        assert_eq!(commit.commit_id, "req-phase-2a-create-file");
        assert_eq!(commit.committed_seq, ChangeSeq(1));
        assert_eq!(
            commit.results,
            vec![CommitOpResult::CreateFile {
                op_index: 0,
                inode_id: InodeId(2),
                revision_no: loon_api::RevisionNo(1),
                content_manifest_digest: manifest_digest.clone(),
            }]
        );

        let repeated_commit = harness
            .client
            .commit_operations(namespace, &commit_request)
            .expect("repeat commit");
        assert_eq!(repeated_commit, commit);

        let stat = harness
            .client
            .stat_path(&target)
            .expect("stat committed file");
        assert_eq!(stat.inode_id, InodeId(2));
        assert_eq!(
            stat.content_manifest_digest.as_deref(),
            Some(manifest_digest.as_str())
        );
        let read_back = harness
            .client
            .read_file_bytes(&target)
            .expect("read committed file");
        assert_eq!(read_back, file_bytes);

        let changes = harness
            .client
            .list_changes(namespace, ChangeSeq(0))
            .expect("list changes");
        assert_eq!(changes.namespace_id.as_str(), namespace);
        assert_eq!(changes.from_exclusive_seq, ChangeSeq(0));
        assert_eq!(changes.through_seq, commit.committed_seq);
        assert_eq!(changes.changes.len(), 1);
        let change = &changes.changes[0];
        assert_eq!(change.seq, commit.committed_seq);
        assert_eq!(change.commit_id, commit.commit_id);
        assert_eq!(change.request_id, commit_request.request_id);
        assert_eq!(change.message.as_deref(), Some("upload over http"));
        assert_eq!(change.annotations, Some(annotations));
        assert_eq!(change.ops, commit.results);

        let empty = harness
            .client
            .list_changes(namespace, commit.committed_seq)
            .expect("list changes after head");
        assert_eq!(empty.changes, Vec::new());
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_commit_rejects_same_request_id_with_different_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loond-v0-conflict",
        "http-v0-conflict",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");

        let first_manifest =
            stage_uploaded_manifest(&harness.client, namespace, b"first payload\n");
        let first_request = V0CommitRequest {
            request_id: "req-phase-2a-conflict".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: vec![CommitPrecondition::HeadSeqIs {
                expected_seq: ChangeSeq(0),
            }],
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "first.txt".to_owned(),
                content_manifest_digest: first_manifest,
            }],
            message: Some("first commit".to_owned()),
            annotations: None,
        };
        harness
            .client
            .commit_operations(namespace, &first_request)
            .expect("first commit");

        let second_manifest =
            stage_uploaded_manifest(&harness.client, namespace, b"second payload\n");
        let mut changed_annotations = BTreeMap::new();
        changed_annotations.insert("source".to_owned(), json!("changed"));
        let conflicting_request = V0CommitRequest {
            request_id: first_request.request_id.clone(),
            planned_head_seq: ChangeSeq(0),
            preconditions: first_request.preconditions.clone(),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "second.txt".to_owned(),
                content_manifest_digest: second_manifest,
            }],
            message: Some("second commit".to_owned()),
            annotations: Some(changed_annotations),
        };

        match harness
            .client
            .commit_operations(namespace, &conflicting_request)
        {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "request_id_conflict"),
            other => panic!("expected request_id_conflict, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_put_request_id_is_idempotent_and_conflicts_on_different_bytes() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loond-v1-put",
        "http-v1-put",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let target = NamespacePath::parse("demo:/docs/retry.txt").expect("target");
        let request_id = "req-v1-put";

        let first = harness
            .client
            .put_file_bytes_with_request_id(&target, b"stable bytes\n", false, request_id)
            .expect("first put");
        assert!(first.committed_seq.0 >= 1);

        let repeated = harness
            .client
            .put_file_bytes_with_request_id(&target, b"stable bytes\n", false, request_id)
            .expect("repeat put");
        assert_eq!(repeated, first);

        let entry = harness.client.stat_path(&target).expect("stat path");
        assert_eq!(entry.authoritative_head_seq, first.committed_seq);
        let bytes = harness.client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"stable bytes\n");

        match harness.client.put_file_bytes_with_request_id(
            &target,
            b"different bytes\n",
            false,
            request_id,
        ) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "request_id_conflict"),
            other => panic!("expected request_id_conflict, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_delete_move_and_copy_request_ids_are_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loond-v1-ops",
        "http-v1-ops",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let source = NamespacePath::parse("demo:/docs/source.txt").expect("source");
        harness
            .client
            .write_file_bytes(&source, b"source bytes\n")
            .expect("seed source");

        let copied = NamespacePath::parse("demo:/docs/copied.txt").expect("copied");
        let copy_first = harness
            .client
            .copy_path_with_request_id(&source, &copied, "req-v1-copy")
            .expect("copy first");
        let copy_repeated = harness
            .client
            .copy_path_with_request_id(&source, &copied, "req-v1-copy")
            .expect("copy repeat");
        assert_eq!(copy_repeated, copy_first);
        let source_entry = harness.client.stat_path(&source).expect("source stat");
        let copied_entry = harness.client.stat_path(&copied).expect("copied stat");
        assert_ne!(source_entry.inode_id, copied_entry.inode_id);
        assert_eq!(
            source_entry.content_manifest_digest,
            copied_entry.content_manifest_digest
        );

        let moved = NamespacePath::parse("demo:/docs/moved.txt").expect("moved");
        let move_first = harness
            .client
            .move_path_with_request_id(&copied, &moved, "req-v1-move")
            .expect("move first");
        let move_repeated = harness
            .client
            .move_path_with_request_id(&copied, &moved, "req-v1-move")
            .expect("move repeat");
        assert_eq!(move_repeated, move_first);
        match harness.client.stat_path(&copied) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
            other => panic!("expected path_not_found for moved-from path, got {other:?}"),
        }
        let moved_entry = harness.client.stat_path(&moved).expect("moved stat");
        assert_eq!(moved_entry.inode_id, copied_entry.inode_id);

        let delete_first = harness
            .client
            .delete_path_with_request_id(&moved, "req-v1-delete")
            .expect("delete first");
        let delete_repeated = harness
            .client
            .delete_path_with_request_id(&moved, "req-v1-delete")
            .expect("delete repeat");
        assert_eq!(delete_repeated, delete_first);
        match harness.client.stat_path(&moved) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
            other => panic!("expected path_not_found for deleted path, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_servers_share_one_store_and_handoff_the_lease() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let server_a = start_server(test_config(
        store_root.clone(),
        "loond-a",
        "two-server-smoke",
        200,
    ))
    .await;
    let server_b = start_server(test_config(store_root, "loond-b", "two-server-smoke", 200)).await;
    let client_a = server_a.client.clone();
    let client_b = server_b.client.clone();

    tokio::task::spawn_blocking(move || {
        client_a.create_namespace("demo").expect("create namespace");
        let host_a_target = NamespacePath::parse("demo:/docs/host-a.txt").expect("host a target");
        client_a
            .write_file_bytes(&host_a_target, b"host a\n")
            .expect("host a write");

        let host_b_target = NamespacePath::parse("demo:/docs/host-b.txt").expect("host b target");
        match client_b.write_file_bytes(&host_b_target, b"host b\n") {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "lease_conflict"),
            other => panic!("expected lease_conflict, got {other:?}"),
        }

        let committed_seq = retry_until_lease_handoff(&client_b, &host_b_target);
        assert!(
            committed_seq >= 2,
            "expected later commit seq, got {committed_seq}"
        );

        let host_b_entry = client_a
            .stat_path(&host_b_target)
            .expect("stat host b file");
        assert_eq!(host_b_entry.authoritative_head_seq.0, committed_seq);
        let host_b_bytes = client_a
            .read_file_bytes(&host_b_target)
            .expect("read host b file");
        assert_eq!(host_b_bytes, b"host b\n");
    })
    .await
    .expect("join blocking task");

    server_a.server.abort();
    server_b.server.abort();
}

struct TestServer {
    client: Client,
    server: tokio::task::JoinHandle<()>,
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

    TestServer {
        client: Client::new(ClientConfig {
            server_url: format!("http://{}", addr),
            auth_token: Some("test-token".to_owned()),
        }),
        server,
    }
}

fn test_config(
    store_root: std::path::PathBuf,
    writer_id: &str,
    key_prefix: &str,
    lease_duration_ms: u64,
) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".to_owned()),
        writer_id: writer_id.to_owned(),
        writer_version: format!("{writer_id}/0.1.0"),
        lease_duration_ms,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: Some(key_prefix.to_owned()),
        },
    }
}

fn retry_until_lease_handoff(client: &Client, target: &NamespacePath) -> u64 {
    for _attempt in 0..20 {
        match client.write_file_bytes(target, b"host b\n") {
            Ok(result) => return result.committed_seq.0,
            Err(ClientError::Api { code, .. }) if code == "lease_conflict" => {
                thread::sleep(Duration::from_millis(50));
            }
            other => panic!("expected success or lease_conflict while waiting, got {other:?}"),
        }
    }

    panic!("timed out waiting for lease handoff");
}

fn stage_uploaded_manifest(client: &Client, namespace: &str, file_bytes: &[u8]) -> String {
    let begin = client.begin_upload(namespace).expect("begin upload");
    client
        .upload_block(namespace, &begin.upload_id, 0, file_bytes)
        .expect("upload single block");
    let complete_request = CompleteUploadRequest {
        file_size_bytes: file_bytes.len() as u64,
        file_digest_sha256: sha256_digest(file_bytes),
    };
    let complete = client
        .complete_upload(namespace, &begin.upload_id, &complete_request)
        .expect("complete upload");
    let repeated = client
        .complete_upload(namespace, &begin.upload_id, &complete_request)
        .expect("repeat complete upload");
    assert_eq!(repeated, complete);
    complete.content_manifest_digest
}
