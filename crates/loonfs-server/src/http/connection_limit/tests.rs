//! Accept ordering without scheduling delays.

use super::*;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::DuplexStream;

struct ReadyListener {
    streams: VecDeque<DuplexStream>,
    accepts: Arc<AtomicUsize>,
}

impl Listener for ReadyListener {
    type Io = DuplexStream;
    type Addr = ();

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.accepts.fetch_add(1, Ordering::SeqCst);
        (self.streams.pop_front().expect("queued connection"), ())
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(())
    }
}

#[tokio::test]
async fn a_full_connection_limit_stops_accept_until_the_first_connection_drops() {
    let (first, _first_peer) = tokio::io::duplex(64);
    let (second, _second_peer) = tokio::io::duplex(64);
    let accepts = Arc::new(AtomicUsize::new(0));
    let recorder = DefaultMetricsRecorder::new();
    let mut listener = ConnectionLimit::new(
        ReadyListener {
            streams: VecDeque::from([first, second]),
            accepts: Arc::clone(&accepts),
        },
        1,
        &recorder,
    );
    let (first, ()) = listener.accept().await;
    assert_eq!(accepts.load(Ordering::SeqCst), 1);
    assert_gauge(&recorder, "loonfs.server.connections_open", 1);
    {
        let mut waiting = Box::pin(listener.accept());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        assert_waiting(&recorder, 1);
    }
    assert_waiting(&recorder, 0);
    let mut waiting = Box::pin(listener.accept());
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    drop(first);
    let (second, ()) = waiting.await;
    assert_eq!(accepts.load(Ordering::SeqCst), 2);
    assert_waiting(&recorder, 0);
    assert_gauge(&recorder, "loonfs.server.connections_open", 1);
    drop(second);
    assert_eq!(listener.permits.available_permits(), 1);
    assert_gauge(&recorder, "loonfs.server.connections_open", 0);
}

#[tokio::test]
async fn the_open_connections_gauge_counts_accepted_connections_until_they_drop() {
    let (first, _first_peer) = tokio::io::duplex(64);
    let (second, _second_peer) = tokio::io::duplex(64);
    let recorder = DefaultMetricsRecorder::new();
    let mut listener = ConnectionLimit::new(
        ReadyListener {
            streams: VecDeque::from([first, second]),
            accepts: Arc::new(AtomicUsize::new(0)),
        },
        2,
        &recorder,
    );
    assert_gauge(&recorder, "loonfs.server.connections_open", 0);
    let (first, ()) = listener.accept().await;
    let (second, ()) = listener.accept().await;
    assert_gauge(&recorder, "loonfs.server.connections_open", 2);
    drop(first);
    assert_gauge(&recorder, "loonfs.server.connections_open", 1);
    drop(second);
    assert_gauge(&recorder, "loonfs.server.connections_open", 0);
}

#[tokio::test]
async fn a_second_tcp_connection_is_accepted_only_after_the_first_closes() {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = tcp.local_addr().expect("address");
    let recorder = DefaultMetricsRecorder::new();
    let mut listener = ConnectionLimit::new(tcp, 1, &recorder);
    let first_peer = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    let (first, _) = listener.accept().await;
    let _second_peer = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    let mut waiting = Box::pin(listener.accept());
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    assert_waiting(&recorder, 1);
    drop(first_peer);
    drop(first);
    let (_second, _) = waiting.await;
    assert_waiting(&recorder, 0);
}

fn assert_waiting(recorder: &DefaultMetricsRecorder, expected: i64) {
    assert_gauge(
        recorder,
        "loonfs.server.connection_accept_waiting",
        expected,
    );
}

fn assert_gauge(recorder: &DefaultMetricsRecorder, name: &str, expected: i64) {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .all()
        .iter()
        .find(|entry| entry.name == name)
        .expect("registered gauge");
    assert_eq!(entry.value, MetricValue::Gauge(expected), "{name}");
}
