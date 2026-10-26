//! A deterministic run: the simulated server assigns the same IDs to the
//! same requests, and the engine's control task is a state machine over its
//! messages, so the same commits and reads make the same requests.

use crate::support::{
    append, fixture_with, move_path, open, operations, pattern, put, put_replacing, read, refresh,
    MIB,
};
use loonfs_mount_sim::{Operation, RequestRecord, SimulatedServerConfig};

/// Mounts a namespace, reads a small file and a large one, applies an
/// append, a directory moved into the mount, and a replacement of the large
/// file while it's open, reads again, and returns every request that the
/// server received, up to the deletion of the moved-in directory's
/// snapshot.
async fn run() -> Vec<RequestRecord> {
    let fixture = fixture_with(SimulatedServerConfig {
        max_download_range_bytes: MIB,
        ..SimulatedServerConfig::default()
    });
    fixture.commit(vec![
        put("log.txt", b"line 0\n"),
        put("/elsewhere/incoming/a.txt", b"a"),
    ]);
    let data = pattern(20 * MIB);
    fixture.upload("data.bin", &data).await;
    let engine = fixture.mount().await;
    let mut trace = fixture.server.take_trace();
    let log = open(&engine, "log.txt").await;
    read(&log, 0, 4096).await;
    let large = open(&engine, "data.bin").await;
    for offset in (0..4 * MIB).step_by(256 * 1024) {
        read(&large, offset, 256 * 1024).await;
    }
    fixture.commit(vec![append("log.txt", b"line 1\n")]);
    fixture.commit(vec![move_path("/elsewhere/incoming", "incoming")]);
    let last = fixture.commit(vec![put_replacing("data.bin", b"replaced")]);
    refresh(&engine, last.committed_seq).await;
    read(&log, 7, 4096).await;
    read(&large, 19 * MIB, 4096).await;
    fixture.wait_for_request(Operation::DeleteSnapshot).await;
    trace.extend(fixture.server.take_trace());
    trace
}

/// Checks that two runs of the same commits and reads, each against a new
/// server, make the same requests in the same order, with the same
/// parameters, responses, and byte ranges.
#[tokio::test]
async fn the_same_commits_and_reads_make_the_same_requests() {
    let first = run().await;
    let second = run().await;
    assert!(first.len() > 20, "{} requests", first.len());
    assert_eq!(operations(&first), operations(&second));
    for (first, second) in first.iter().zip(&second) {
        assert_eq!(first, second);
    }
}
