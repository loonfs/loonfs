//! Content changes from the feed: a replacement makes a new item while open
//! handles keep the revision they opened, and an append extends the file
//! that open handles read.

use crate::support::{
    append, content_reads, fetched_bytes, fixture, item, open, pattern, put, put_replacing, read,
    refresh, MIB,
};
use loonfs_mount::{CachedPages, Invalidation};
use loonfs_mount_sim::{ByteRange, Operation};

/// Checks that a replacement makes the name refer to a new item, that a
/// handle opened before it keeps reading the old revision, and that the
/// engine fetches the rest of that revision through one grant.
#[tokio::test]
async fn a_replacement_makes_a_new_item_and_an_open_handle_keeps_reading_the_old_bytes() {
    let fixture = fixture();
    let old = pattern(10 * MIB);
    fixture.upload("data.bin", &old).await;
    let engine = fixture.mount().await;
    let handle = open(&engine, "data.bin").await;
    assert_eq!(read(&handle, 0, 4096).await, old[..4096]);
    fixture.server.take_trace();
    let replaced = fixture.commit(vec![put_replacing("data.bin", b"replaced")]);
    refresh(&engine, replaced.committed_seq).await;

    let current = item(&engine, "data.bin").expect("the replacement");
    assert_ne!(current, handle.item(), "a replacement is a new item");
    assert_eq!(engine.getattr(current).expect("the new item").size_bytes, 8);
    let offset = 9 * MIB;
    let start = usize::try_from(offset).expect("an offset");
    assert_eq!(read(&handle, offset, 16).await, old[start..start + 16]);
    let trace = fixture.server.take_trace();
    let grants: Vec<_> = trace
        .iter()
        .filter(|record| record.operation == Operation::CreateRevisionDownloadByInode)
        .collect();
    assert_eq!(grants.len(), 1, "one grant for the rest of the revision");
    assert!(grants[0].path.ends_with("/revisions/1/downloads"));
    let ranges: Vec<_> = trace
        .iter()
        .filter_map(|record| record.byte_range)
        .collect();
    assert_eq!(ranges.first().map(|range| range.start_offset), Some(MIB));
    assert!(ranges
        .windows(2)
        .all(|pair| pair[0].end_offset == pair[1].start_offset));
    assert_eq!(
        fetched_bytes(&trace),
        9 * MIB,
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
        assert_eq!(
            reads[0].byte_range,
            Some(ByteRange {
                start_offset: end,
                end_offset: size,
            })
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
