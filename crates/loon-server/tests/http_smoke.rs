use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loon_server::{app, ServerConfig, StoreConfig};
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
