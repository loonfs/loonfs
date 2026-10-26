//! Content changes from the feed: a replacement makes a new item while open
//! handles keep the revision they opened, an append extends the file that
//! open handles read, an append that starts a new chain keeps the cached
//! bytes, and two revisions of one chain share a cache file.

use crate::support::{
    append, content_reads, eventually, fetched_bytes, fixture, item, open, pattern, put,
    put_replacing, range_header, read, refresh, wait_until_handled, Fixture, UNIT,
};
use loonfs_mount::{CachedPages, Invalidation};
use loonfs_mount_sim::{ByteRange, Operation};

/// Checks that a replacement makes the name refer to a new item, that a
/// handle opened before it keeps reading the old revision, and that the
/// engine fetches the rest of that revision through one grant.
#[tokio::test]
async fn a_replacement_makes_a_new_item_and_an_open_handle_keeps_reading_the_old_bytes() {
    let fixture = fixture();
    let old = pattern(10 * UNIT);
    fixture.upload("data.bin", &old).await;
    let engine = fixture.mount().await;
    let handle = open(&engine, "data.bin").await;
    assert_eq!(read(&handle, 0, 4096).await, old[..4096]);
    let old_content = fixture.content_ref("data.bin").await.content_id;
    fixture.server.take_trace();
    let replaced = fixture.commit(vec![put_replacing("data.bin", b"replaced")]);
    refresh(&engine, replaced.committed_seq).await;

    let current = item(&engine, "data.bin").expect("the replacement");
    assert_ne!(current, handle.item(), "a replacement is a new item");
    assert_eq!(engine.getattr(current).expect("the new item").size_bytes, 8);
    let offset = 9 * UNIT;
    let start = usize::try_from(offset).expect("an offset");
    assert_eq!(read(&handle, offset, 16).await, old[start..start + 16]);
    let trace = fixture.server.take_trace();
    let grants: Vec<_> = trace
        .iter()
        .filter(|record| record.operation == Operation::CreateRevisionDownloadByInode)
        .collect();
    assert_eq!(grants.len(), 1, "one grant for the rest of the revision");
    assert!(grants[0].path.ends_with("/revisions/1/downloads"));
    assert!(
        content_reads(&trace)
            .iter()
            .all(|record| record.content_id.as_ref() == Some(&old_content)),
        "every read is of the old revision's content"
    );
    let ranges: Vec<_> = trace
        .iter()
        .filter_map(|record| record.byte_range)
        .collect();
    assert_eq!(ranges.first().map(|range| range.start_offset), Some(UNIT));
    assert!(ranges
        .windows(2)
        .all(|pair| pair[0].end_offset == pair[1].start_offset));
    assert_eq!(
        fetched_bytes(&trace),
        9 * UNIT,
        "the rest of the revision, once"
    );

    let new_handle = open(&engine, "data.bin").await;
    assert_eq!(read(&new_handle, 0, 100).await, "replaced");
}

/// Checks the `tail -f` case: each append extends the item that a handle
/// has open, the handle reads the new bytes at the old end, the engine
/// fetches exactly the appended range with one ranged read, and the kernel
/// drops only the appended pages.
#[tokio::test]
async fn each_append_extends_an_open_file_and_fetches_only_the_appended_range() {
    let fixture = fixture();
    fixture.commit(vec![put("app.log", b"line 0\n")]);
    let engine = fixture.mount().await;
    let mut invalidations = engine.subscribe_invalidations();
    let handle = open(&engine, "app.log").await;
    let mut log = b"line 0\n".to_vec();
    assert_eq!(read(&handle, 0, 4096).await, log);
    for line in 1..4_u64 {
        let appended = format!("line {line}\n");
        let end = log.len() as u64;
        fixture.server.take_trace();
        let commit = fixture.commit(vec![append("app.log", appended.as_bytes())]);
        refresh(&engine, commit.committed_seq).await;
        log.extend_from_slice(appended.as_bytes());
        let size = log.len() as u64;

        assert_eq!(
            item(&engine, "app.log"),
            Some(handle.item()),
            "an append keeps the item"
        );
        assert_eq!(
            handle.size_bytes(),
            size,
            "the open handle follows the append"
        );
        assert_eq!(
            engine.getattr(handle.item()).expect("the log").size_bytes,
            size
        );
        assert_eq!(read(&handle, end, 4096).await, appended);
        let trace = fixture.server.take_trace();
        let reads = content_reads(&trace);
        assert_eq!(reads.len(), 1, "after line {line}");
        let revision = format!("/revisions/{}/content", line + 1);
        assert!(reads[0].path.ends_with(&revision), "{}", reads[0].path);
        assert_eq!(reads[0].status, 206);
        let appended_range = ByteRange {
            start_offset: end,
            end_offset: size,
        };
        assert_eq!(reads[0].byte_range, Some(appended_range));
        assert_eq!(
            reads[0].range_header,
            Some(range_header(appended_range)),
            "the engine asks for the appended range only"
        );
        let mut received = Vec::new();
        while let Ok(invalidation) = invalidations.try_recv() {
            received.push(invalidation);
        }
        assert_eq!(
            received,
            [Invalidation::Inode {
                item: handle.item(),
                pages: CachedPages::Range {
                    offset: end,
                    length: size - end,
                },
            }]
        );
    }
}

/// Puts the same uploaded content at `a.bin` and `b.bin`, so the two files
/// share one chain.
async fn two_files_on_one_chain(fixture: &Fixture, bytes: &[u8]) {
    let prepared = fixture
        .client
        .prepare_content(&fixture.namespace_id, bytes)
        .await
        .expect("the upload");
    for relative in ["a.bin", "b.bin"] {
        fixture
            .client
            .put_file_prepared(
                &fixture.namespace_path(relative),
                prepared.clone(),
                &fixture.actor,
            )
            .await
            .expect("the put");
    }
}

/// Checks that an append that starts a new chain, because another file
/// extended the shared chain first, moves the complete cache file to the new
/// chain, so a read of the new revision fetches only the appended bytes.
#[tokio::test]
async fn an_append_that_starts_a_new_chain_keeps_the_cached_bytes() {
    let fixture = fixture();
    let bytes = pattern(2 * UNIT);
    two_files_on_one_chain(&fixture, &bytes).await;
    let shared = fixture.content_ref("b.bin").await;
    let engine = fixture.mount().await;
    let handle = open(&engine, "b.bin").await;
    assert_eq!(read(&handle, 0, 4 * UNIT).await, bytes);
    drop(handle);
    eventually(|| fixture.cache_file(&shared).exists()).await;
    wait_until_handled(&engine).await;

    fixture.commit(vec![append("a.bin", b"more of a")]);
    let last = fixture.commit(vec![append("b.bin", b"more of b")]);
    refresh(&engine, last.committed_seq).await;
    let current = fixture.content_ref("b.bin").await;
    assert_ne!(current.content_id, shared.content_id, "a new chain");

    fixture.server.take_trace();
    let handle = open(&engine, "b.bin").await;
    let mut expected = bytes.to_vec();
    expected.extend_from_slice(b"more of b");
    assert_eq!(read(&handle, 0, 4 * UNIT).await, expected);
    let trace = fixture.server.take_trace();
    let reads: Vec<_> = content_reads(&trace)
        .iter()
        .map(|record| record.byte_range.expect("a range"))
        .collect();
    assert_eq!(
        reads,
        [ByteRange {
            start_offset: 2 * UNIT,
            end_offset: 2 * UNIT + 9,
        }],
        "only the appended bytes"
    );
    eventually(|| fixture.cache_file(&current).exists()).await;
}

/// Checks that two handles that read two revisions of one chain, a shorter
/// one through `b.bin` and a longer one through `a.bin`, share one cache
/// file: each byte is fetched once, both reads return their revision's
/// bytes, and the file is complete and verified once it has every byte.
#[tokio::test]
async fn two_handles_on_one_chain_share_the_cache_file() {
    let fixture = fixture();
    let bytes = pattern(2 * UNIT);
    two_files_on_one_chain(&fixture, &bytes).await;
    let last = fixture.commit(vec![append("a.bin", b"more of a")]);
    let engine = fixture.mount().await;
    refresh(&engine, last.committed_seq).await;
    let longer = fixture.content_ref("a.bin").await;

    fixture.server.take_trace();
    let shorter_handle = open(&engine, "b.bin").await;
    let longer_handle = open(&engine, "a.bin").await;
    assert_eq!(read(&shorter_handle, 0, 4 * UNIT).await, bytes);
    let mut expected = bytes.to_vec();
    expected.extend_from_slice(b"more of a");
    assert_eq!(read(&longer_handle, 0, 4 * UNIT).await, expected);
    let trace = fixture.server.take_trace();
    assert_eq!(fetched_bytes(&trace), 2 * UNIT + 9, "each byte once");
    eventually(|| fixture.cache_file(&longer).exists()).await;
    assert_eq!(engine.status().cache.mismatched_files, 0);
}
