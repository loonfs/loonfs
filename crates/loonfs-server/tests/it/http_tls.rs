//! TLS termination end to end: a real client over a real handshake, and the
//! two ways a connection can be wrong without taking the server with it.

use crate::common::http_split_support::{replace_file_options, test_config};
use crate::common::{start_tls_server, TlsTestServer};
use loonfs_client::{Client, ClientConfig, NamespacePath};
use loonfs_test_support::ids::namespace_id;
use std::io::{Read, Write};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_trusting_the_server_certificate_round_trips_over_tls() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_tls_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-tls",
        "http-tls",
    ))
    .await;

    assert!(
        harness.server_url.starts_with("https://"),
        "the harness must exercise https, got {}",
        harness.server_url
    );

    let namespace = namespace_id("over-tls");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("over-tls", "/note.txt").expect("parse path");
    harness
        .client
        .put_file_bytes(&target, b"ciphertext in flight", &replace_file_options())
        .await
        .expect("write file");

    let stat = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat file");
    assert_eq!(
        stat.size_bytes(),
        Some(b"ciphertext in flight".len() as u64)
    );

    let bytes = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read file");
    assert_eq!(bytes, b"ciphertext in flight");

    shut_down(harness).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_without_the_certificate_authority_is_refused_at_the_handshake() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_tls_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-tls",
        "http-tls-untrusted",
    ))
    .await;

    let untrusting = Client::new(ClientConfig {
        server_url: harness.server_url.clone(),
        auth_token: harness.auth_token.clone().map(Into::into),
        request_timeout_ms: None,
        disable_transient_retry: true,
        ca_cert_path: None,
    })
    .expect("valid client config");

    // A self-signed certificate the platform roots do not vouch for is a
    // transport failure, reported rather than panicked through.
    let error = untrusting
        .get_namespace(&namespace_id("over-tls"))
        .await
        .expect_err("untrusted certificate");
    let message = error.to_string();
    assert!(
        message.contains("invalid peer certificate"),
        "the failure must name the certificate rather than read as a generic \
         network error, got: {message}"
    );

    // The refused handshake was that connection's alone.
    harness
        .client
        .create_namespace(&namespace_id("still-serving"))
        .await
        .expect("the server still serves trusted clients");

    shut_down(harness).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plaintext_request_to_the_tls_port_loses_only_its_own_connection() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_tls_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-tls",
        "http-tls-plaintext",
    ))
    .await;

    let addr = harness.addr;
    let plaintext = tokio::task::spawn_blocking(move || {
        let mut socket = std::net::TcpStream::connect(addr).expect("connect to the tls port");
        socket
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .expect("write a plaintext request");
        let mut response = Vec::new();
        socket.read_to_end(&mut response).map(|_| response)
    })
    .await
    .expect("plaintext probe task");

    // rustls answers the malformed ClientHello with an alert and closes, so
    // the probe either reads no HTTP response or fails outright. Both are a
    // refused connection; what matters is that neither is a served request.
    if let Ok(response) = plaintext {
        assert!(
            !response.starts_with(b"HTTP/"),
            "the tls port must not answer plaintext HTTP, got: {response:?}"
        );
    }

    harness
        .client
        .create_namespace(&namespace_id("after-plaintext"))
        .await
        .expect("the server still serves tls clients");

    shut_down(harness).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_settles_background_work_on_the_tls_path() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_tls_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-tls",
        "http-tls-shutdown",
    ))
    .await;

    let namespace = namespace_id("drained");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("drained", "/note.txt").expect("parse path");
    harness
        .client
        .put_file_bytes(&target, b"settle me", &replace_file_options())
        .await
        .expect("write file");

    // `serve_with_shutdown` returns only after the listener drains and the
    // writer settles; unsettled background work surfaces here as an error.
    shut_down(harness).await;
}

async fn shut_down(harness: TlsTestServer) {
    let TlsTestServer {
        client,
        shutdown,
        server,
        ..
    } = harness;
    drop(client);
    shutdown.send(()).expect("signal shutdown");
    server
        .await
        .expect("server task")
        .expect("graceful shutdown settles");
}
