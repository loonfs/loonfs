//! How long a connection may stay open without sending a request.

use crate::common::http_split_support::test_config;
use crate::common::start_server;
use std::num::NonZeroU64;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

/// The idle timeout the tests configure, short enough to wait out.
const IDLE_TIMEOUT_MS: u64 = 200;

/// How long a test waits for the server to close an idle connection, far
/// past [`IDLE_TIMEOUT_MS`] so a slow machine doesn't fail the test.
const CLOSE_BOUND: Duration = Duration::from_secs(10);

const HEALTH_REQUEST: &[u8] = b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_that_sends_nothing_is_closed_after_the_idle_timeout() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(temp_dir.path().join("store"), "idle-silent", "idle-silent");
    config.connection_idle_timeout_ms = NonZeroU64::new(IDLE_TIMEOUT_MS).expect("nonzero");
    let server = start_server(config).await;
    let address = server
        .server_url
        .strip_prefix("http://")
        .expect("a plaintext server url")
        .to_owned();

    let mut silent = TcpStream::connect(&address).await.expect("connect");
    let mut received = Vec::new();
    let read = loonfs_test_support::clock::bounded(CLOSE_BOUND, silent.read_to_end(&mut received))
        .await
        .expect("the server closes a connection that sends nothing");
    assert_closed(read, &received);

    // Closing the silent connection costs no one else theirs.
    let mut fresh = TcpStream::connect(&address).await.expect("connect again");
    fresh
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("send a request");
    let mut response = Vec::new();
    loonfs_test_support::clock::bounded(CLOSE_BOUND, fresh.read_to_end(&mut response))
        .await
        .expect("the request is answered")
        .expect("read the response");
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "a fresh connection is served, got: {}",
        String::from_utf8_lossy(&response)
    );

    server.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keep_alive_connection_is_closed_after_the_idle_timeout() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "idle-keep-alive",
        "idle-keep-alive",
    );
    config.connection_idle_timeout_ms = NonZeroU64::new(IDLE_TIMEOUT_MS).expect("nonzero");
    let server = start_server(config).await;
    let address = server
        .server_url
        .strip_prefix("http://")
        .expect("a plaintext server url")
        .to_owned();

    let mut stream = TcpStream::connect(&address).await.expect("connect");
    stream
        .write_all(HEALTH_REQUEST)
        .await
        .expect("send a request");
    // The request keeps the connection alive, so reading to the end
    // finishes only if the server closes the connection after the response.
    let mut received = Vec::new();
    let read = loonfs_test_support::clock::bounded(CLOSE_BOUND, stream.read_to_end(&mut received))
        .await
        .expect("the server closes the idle keep-alive connection");
    assert!(
        received.starts_with(b"HTTP/1.1 200"),
        "the request is answered before the close, got: {}",
        String::from_utf8_lossy(&received)
    );
    read.expect("the close is an orderly end of the stream after the response");

    server.server.abort();
}

/// Asserts that a read from a connection the server closed ended without
/// any bytes, either at the end of the stream or with a reset.
pub(crate) fn assert_closed(read: std::io::Result<usize>, received: &[u8]) {
    match read {
        Ok(_) => assert!(
            received.is_empty(),
            "the server sent bytes before closing: {received:?}"
        ),
        Err(error) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "the connection ends with a reset or the end of the stream, got: {error}"
        ),
    }
}
