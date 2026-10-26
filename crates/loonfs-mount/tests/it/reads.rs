//! Reading files: whole and windowed fetches and the bytes they cost,
//! verification of cache files, retries, deployments without grants, and
//! eviction.

use crate::support::{
    content_reads, eventually, fetched_bytes, fixture, fixture_with, open, pattern, put, read,
    wait_until_handled, Fixture, UNIT,
};
use loonfs_mount::ContentState;
use loonfs_mount_sim::{
    ByteRange, CapabilitySwitches, ContentReadFault, InjectedFault, Operation,
    SimulatedServerConfig,
};
use loonfs_types::api::v0::PathEntryKind;
use loonfs_types::ContentRef;

/// The size of each read call in the sequential reads: an eighth of a
/// window, as a program that reads in 128 KiB calls makes them with the
/// mount's default 1 MiB first window.
const CHUNK: u64 = UNIT / 8;

async fn content_ref(fixture: &Fixture, relative: &str) -> ContentRef {
    match fixture.stat(relative).await.kind {
        PathEntryKind::File { content_ref, .. } => content_ref,
        kind => panic!("`{relative}` is not a file: {kind:?}"),
    }
}

/// Returns the range from `start` to `end` units.
fn range_units(start: u64, end: u64) -> ByteRange {
    ByteRange {
        start_offset: start * UNIT,
        end_offset: end * UNIT,
    }
}

/// Checks that a small file is fetched whole with one read that the server
/// verifies, and that a second handle reads it from the cache.
#[tokio::test]
async fn a_small_file_is_fetched_whole_once_and_then_read_from_the_cache() {
    let fixture = fixture();
    let bytes = pattern(5 * 1024);
    fixture.commit(vec![put("small.bin", &bytes)]);
    let content_ref = content_ref(&fixture, "small.bin").await;
    let engine = fixture.mount().await;
    fixture.server.take_trace();
    let first = open(&engine, "small.bin").await;
    assert_eq!(first.content_state(), ContentState::Absent);
    assert_eq!(read(&first, 100, 50).await, bytes[100..150]);
    assert_eq!(first.content_state(), ContentState::Complete);
    let second = open(&engine, "small.bin").await;
    assert_eq!(read(&second, 0, 10 * 1024).await, bytes);

    let trace = fixture.server.take_trace();
    let reads = content_reads(&trace);
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].operation, Operation::GetFileRevisionContentByInode);
    assert_eq!(reads[0].status, 200, "a whole read, not a range");
    assert_eq!(fetched_bytes(&trace), bytes.len() as u64);
    let cached = std::fs::read(fixture.cache_file(&content_ref)).expect("the cache file");
    assert_eq!(cached, bytes);
}

/// The size of each signed range of a grant in the sequential read, which is
/// the most a grant fetches past the window that asked for it.
const GRANT_RANGE_BYTES: u64 = UNIT;

/// Returns the range from `start` to `end` quarter units.
fn range_quarter_units(start: u64, end: u64) -> ByteRange {
    ByteRange {
        start_offset: start * UNIT / 4,
        end_offset: end * UNIT / 4,
    }
}

/// Checks that a sequential reader of a 32-unit file fetches windows that
/// grow from 1 unit, each the smallest of twice the last window, 64 units,
/// and the bytes read less the bytes fetched but not read yet, with one
/// window ahead once that's at least half the current window. Windows of at
/// most 8 units are ranged reads, and the larger one is a grant that fetches at
/// most one signed range past its window. After every read, the engine has
/// fetched at most twice the bytes read plus the first window (scaling
/// invariant 2) plus that one range, and no byte is fetched twice.
#[tokio::test]
async fn a_sequential_read_fetches_growing_windows_within_twice_the_bytes_read() {
    let fixture = fixture_with(SimulatedServerConfig {
        max_download_range_bytes: GRANT_RANGE_BYTES,
        ..SimulatedServerConfig::default()
    });
    let size = 32 * UNIT;
    let bytes = pattern(size);
    fixture.upload("data.bin", &bytes).await;
    let engine = fixture.mount().await;
    fixture.server.take_trace();
    let handle = open(&engine, "data.bin").await;
    let mut offset = 0;
    while offset < size {
        let chunk = read(&handle, offset, CHUNK).await;
        let start = usize::try_from(offset).expect("an offset");
        assert_eq!(chunk, bytes[start..start + chunk.len()], "at {offset}");
        offset += chunk.len() as u64;
        let fetched = fetched_bytes(&fixture.server.trace());
        assert!(
            fetched <= 2 * offset + UNIT + GRANT_RANGE_BYTES,
            "{fetched} bytes fetched after {offset} read"
        );
    }

    let trace = fixture.server.take_trace();
    assert_eq!(fetched_bytes(&trace), size, "every byte once");
    let ranged: Vec<_> = trace
        .iter()
        .filter(|record| record.operation == Operation::GetFileRevisionContentByInode)
        .map(|record| record.byte_range.expect("a range"))
        .collect();
    let windows = [
        (0, 4),
        (4, 6),
        (6, 10),
        (10, 16),
        (16, 26),
        (26, 42),
        (42, 68),
        (112, 128),
    ];
    let expected: Vec<_> = windows
        .into_iter()
        .map(|(start, end)| range_quarter_units(start, end))
        .collect();
    assert_eq!(ranged, expected);
    let granted: Vec<_> = trace
        .iter()
        .filter(|record| record.operation == Operation::ReadDownloadRange)
        .map(|record| record.byte_range.expect("a range"))
        .collect();
    let expected: Vec<_> = (17..28)
        .map(|start| range_units(start, start + 1))
        .collect();
    assert_eq!(
        granted, expected,
        "one grant for the window from 17 to 27.5 units, in 1-unit ranges"
    );
    let window_end = 27 * UNIT + UNIT / 2;
    let past_window = granted.last().expect("a granted range").end_offset - window_end;
    assert!(
        past_window <= GRANT_RANGE_BYTES,
        "{past_window} bytes past the window"
    );
    let grants = trace
        .iter()
        .filter(|record| record.operation == Operation::CreateRevisionDownloadByInode)
        .count();
    assert_eq!(grants, 1);
}

/// Checks that a read far from where a sequential run ended starts again
/// with a 1-unit window and no read-ahead, and that the windows that follow
/// continue from there, none smaller than half the one before, within twice
/// the bytes read since the seek.
#[tokio::test]
async fn a_read_at_a_far_offset_resets_the_window() {
    let fixture = fixture();
    let size = 24 * UNIT;
    let bytes = pattern(size);
    fixture.upload("data.bin", &bytes).await;
    let engine = fixture.mount().await;
    let handle = open(&engine, "data.bin").await;
    for offset in (0..=UNIT).step_by(usize::try_from(CHUNK).expect("a chunk")) {
        read(&handle, offset, CHUNK).await;
    }
    eventually(|| fetched_bytes(&fixture.server.trace()) >= 3 * UNIT / 2).await;
    fixture.server.take_trace();
    let far = 20 * UNIT + 7;
    let start = usize::try_from(far).expect("an offset");
    assert_eq!(
        read(&handle, far, CHUNK).await,
        bytes[start..start + usize::try_from(CHUNK).expect("a chunk")]
    );
    assert_eq!(
        content_reads(&fixture.server.trace())
            .iter()
            .map(|record| record.byte_range.expect("a range"))
            .collect::<Vec<_>>(),
        [range_units(20, 21)],
        "a 1-unit window and nothing ahead"
    );
    let mut offset = far + CHUNK;
    while offset < far + 2 * UNIT {
        read(&handle, offset, CHUNK).await;
        offset += CHUNK;
        let fetched = fetched_bytes(&fixture.server.trace());
        assert!(
            fetched <= 2 * (offset - far) + UNIT,
            "{fetched} after {offset}"
        );
    }
    let fetched: Vec<_> = content_reads(&fixture.server.take_trace())
        .iter()
        .map(|record| record.byte_range.expect("a range"))
        .collect();
    let grows = fetched.windows(2).all(|pair| {
        pair[0].end_offset == pair[1].start_offset && pair[1].length() >= pair[0].length() / 2
    });
    assert!(grows && fetched.len() > 1, "{fetched:?}");
}

/// Checks that a cache file whose bytes don't match the revision's checksum
/// is deleted and fetched again once, after which reads return the right
/// bytes and the file is complete.
#[tokio::test]
async fn a_corrupted_range_is_fetched_again_once_and_then_reads_correctly() {
    let fixture = fixture();
    let size = 9 * UNIT;
    let bytes = pattern(size);
    fixture.upload("data.bin", &bytes).await;
    let engine = fixture.mount().await;
    fixture.server.take_trace();
    fixture
        .server
        .fail_next_content_read(ContentReadFault::CorruptedByte);
    let handle = open(&engine, "data.bin").await;
    let unverified = read(&handle, 0, size).await;
    assert_ne!(unverified, bytes, "a range isn't verified as it arrives");
    eventually(|| engine.status().cache.mismatched_files == 1).await;
    eventually(|| handle.content_state() == ContentState::Complete).await;
    assert_eq!(read(&handle, 0, size).await, bytes);
    assert_eq!(engine.status().cache.mismatched_files, 1);

    let trace = fixture.server.take_trace();
    let faults: Vec<_> = trace.iter().filter_map(|record| record.fault).collect();
    assert_eq!(
        faults,
        [InjectedFault::ContentRead(ContentReadFault::CorruptedByte)]
    );
    let grants = trace
        .iter()
        .filter(|record| record.operation == Operation::CreateRevisionDownloadByInode)
        .count();
    assert_eq!(grants, 2, "the first fetch and one refetch");
    assert_eq!(fetched_bytes(&trace), 2 * size);
}

/// Checks that a content read that fails in transport is tried again, and
/// that the read returns the right bytes.
#[tokio::test]
async fn a_content_read_that_fails_in_transport_is_tried_again() {
    let fixture = fixture();
    fixture.commit(vec![put("small.txt", b"small")]);
    let engine = fixture.mount().await;
    fixture.server.take_trace();
    fixture
        .server
        .fail_next_content_read(ContentReadFault::TransportFailure);
    let handle = open(&engine, "small.txt").await;
    assert_eq!(read(&handle, 0, 100).await, "small");
    let faults: Vec<_> = content_reads(&fixture.server.take_trace())
        .iter()
        .map(|record| record.fault)
        .collect();
    assert_eq!(
        faults,
        [
            Some(InjectedFault::ContentRead(
                ContentReadFault::TransportFailure
            )),
            None
        ]
    );
}

/// Checks that a deployment that doesn't advertise grants gets every fetch
/// as a ranged read of at most the ranged-read limit, 8 units.
#[tokio::test]
async fn without_direct_get_every_fetch_is_a_ranged_read() {
    let fixture = fixture_with(SimulatedServerConfig {
        capabilities: CapabilitySwitches {
            advertise_direct_get: false,
            ..CapabilitySwitches::default()
        },
        ..SimulatedServerConfig::default()
    });
    let size = 9 * UNIT;
    let bytes = pattern(size);
    fixture.upload("data.bin", &bytes).await;
    let engine = fixture.mount().await;
    assert!(!engine.status().transports.direct_get);
    fixture.server.take_trace();
    let handle = open(&engine, "data.bin").await;
    assert_eq!(read(&handle, 0, size).await, bytes);
    let trace = fixture.server.take_trace();
    let reads: Vec<_> = content_reads(&trace)
        .iter()
        .map(|record| (record.operation, record.byte_range.expect("a range")))
        .collect();
    let ranged = Operation::GetFileRevisionContentByInode;
    assert_eq!(
        reads,
        [(ranged, range_units(0, 8)), (ranged, range_units(8, 9))]
    );
}

/// Checks that a partial file whose handle closed is evicted whole under
/// `--cache-max-bytes`, that a partial file with an open handle stays and
/// counts as pinned, and that a later read fetches the evicted window again.
#[tokio::test]
async fn an_abandoned_partial_file_is_evicted_and_an_open_one_is_not() {
    let fixture = fixture();
    let first = pattern(20 * UNIT);
    let second = pattern(21 * UNIT);
    fixture.upload("abandoned.bin", &first).await;
    fixture.upload("open.bin", &second).await;
    let abandoned = content_ref(&fixture, "abandoned.bin").await;
    let kept_ref = content_ref(&fixture, "open.bin").await;
    let mut config = fixture.config();
    config.max_cache_bytes = UNIT / 2;
    let engine = fixture.mount_with(config).await;
    let partial = |content_ref: &ContentRef| {
        let mut path = fixture.cache_file(content_ref).into_os_string();
        path.push(".partial");
        std::path::PathBuf::from(path)
    };

    let handle = open(&engine, "abandoned.bin").await;
    assert_eq!(read(&handle, 0, 4096).await, first[..4096]);
    let kept = open(&engine, "open.bin").await;
    assert_eq!(read(&kept, 0, 4096).await, second[..4096]);
    assert_eq!(engine.status().cache.pinned_bytes, 2 * UNIT);
    drop(handle);
    wait_until_handled(&engine).await;
    let cache = engine.status().cache;
    assert_eq!((cache.evictable_bytes, cache.pinned_bytes), (0, UNIT));
    assert!(
        !partial(&abandoned).exists(),
        "the abandoned partial file is evicted"
    );
    assert!(partial(&kept_ref).exists(), "the open partial file stays");

    fixture.server.take_trace();
    let handle = open(&engine, "abandoned.bin").await;
    assert_eq!(read(&handle, 0, 4096).await, first[..4096]);
    assert_eq!(
        content_reads(&fixture.server.take_trace())
            .iter()
            .map(|record| record.byte_range.expect("a range"))
            .collect::<Vec<_>>(),
        [range_units(0, 1)],
        "the evicted window is fetched again"
    );
}

/// Checks that once unused complete files pass `--cache-max-bytes`, the
/// engine evicts the least recently used one that nothing has open, keeps
/// an open one, and fetches an evicted file again when it's read.
#[tokio::test]
async fn eviction_removes_an_unused_complete_file_and_keeps_an_open_one() {
    let fixture = fixture();
    fixture.commit(vec![
        put("open.txt", b"kept open"),
        put("first.txt", b"first one"),
        put("second.txt", b"second one"),
    ]);
    let refs = [
        content_ref(&fixture, "open.txt").await,
        content_ref(&fixture, "first.txt").await,
        content_ref(&fixture, "second.txt").await,
    ];
    let mut config = fixture.config();
    config.max_cache_bytes = 12;
    let engine = fixture.mount_with(config).await;
    let kept = open(&engine, "open.txt").await;
    assert_eq!(read(&kept, 0, 100).await, "kept open");
    for name in ["first.txt", "second.txt"] {
        let handle = open(&engine, name).await;
        read(&handle, 0, 100).await;
    }
    wait_until_handled(&engine).await;
    assert_eq!(engine.status().cache.evictable_bytes, 10);
    assert_eq!(engine.status().cache.pinned_bytes, 9, "the open file");
    let present: Vec<bool> = refs
        .iter()
        .map(|content_ref| fixture.cache_file(content_ref).exists())
        .collect();
    assert_eq!(present, [true, false, true]);

    fixture.server.take_trace();
    assert_eq!(read(&kept, 0, 100).await, "kept open");
    assert_eq!(
        read(&open(&engine, "first.txt").await, 0, 100).await,
        "first one"
    );
    let reads = content_reads(&fixture.server.take_trace()).len();
    assert_eq!(reads, 1, "the evicted file is fetched again");
}
