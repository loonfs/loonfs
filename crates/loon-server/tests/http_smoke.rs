use loon_client::{Client, ClientConfig, NamespacePath};
use loon_server::{app, ServerConfig, StoreConfig};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_round_trip_supports_namespace_create_and_file_read_write() {
    let temp_dir = tempdir().expect("tempdir");
    let config = ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".to_owned()),
        writer_id: "loond-test".to_owned(),
        writer_version: "loond-test/0.1.0".to_owned(),
        lease_duration_ms: 60_000,
        store: StoreConfig::LocalFs {
            root: temp_dir.path().join("store").display().to_string(),
            key_prefix: Some("http-smoke".to_owned()),
        },
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app(config).expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let client = Client::new(ClientConfig {
        server_url: format!("http://{}", addr),
        auth_token: Some("test-token".to_owned()),
    });

    tokio::task::spawn_blocking(move || {
        client.create_namespace("demo").expect("create namespace");
        let target = NamespacePath::parse("demo:/notes/hello.txt").expect("parse namespace path");
        client
            .write_file_bytes(&target, b"hello over http\n")
            .expect("write bytes");

        let entry = client.stat_path(&target).expect("stat path");
        assert_eq!(entry.size_bytes, Some(16));

        let bytes = client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello over http\n");
    })
    .await
    .expect("join blocking task");

    server.abort();
}
