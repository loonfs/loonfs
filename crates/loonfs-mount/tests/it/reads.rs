//! Reading files: whole and windowed fetches, appends and replacements of
//! open files, verification of cache files, and eviction.

use crate::namespace::{commit, content, eventually, file, refresh, ScriptedNamespace, ROOT, WAIT};
use bytes::Bytes;
use loonfs_mount::{CachedPages, ContentState, Engine, Invalidation, ItemId, ReadHandle};
use loonfs_test_support::clock::bounded;
use loonfs_types::api::v0::FilesystemChange;
use loonfs_types::{ContentId, ContentRef, InodeId, RevisionNo};
use std::ffi::OsStr;

const MIB: u64 = 1 << 20;

/// Returns `size` bytes that change every eight bytes, so a read
/// from the wrong offset can't pass.
fn pattern(size: u64) -> Bytes {
    (0..size)
        .map(|offset| u8::try_from(offset / 8 % 251).expect("a byte"))
        .collect::<Vec<_>>()
        .into()
}

/// A namespace with one file, `data.bin` (inode 14), of `bytes`.
fn one_file(bytes: &Bytes) -> (ScriptedNamespace, ContentRef) {
    let content_ref = content(&ContentId::generate(), bytes);
    let namespace = ScriptedNamespace::new(
        vec![file(14, ROOT, "/work/project/data.bin", &content_ref)],
        vec![(14, bytes.clone())],
    );
    (namespace, content_ref)
}

async fn open(engine: &Engine, entry_name: &str) -> ReadHandle {
    let entry = engine
        .lookup(ItemId::ROOT, OsStr::new(entry_name))
        .expect("the file");
    bounded(WAIT, engine.open_read(entry.item))
        .await
        .expect("the open ends in time")
        .expect("the open")
}

async fn read(handle: &ReadHandle, offset: u64, length: u64) -> Bytes {
    bounded(WAIT, handle.read(offset, length))
        .await
        .expect("the read ends in time")
        .expect("the read")
}

/// Checks that a small file is fetched whole on its first read, and that a
/// second handle reads it from the cache.
#[tokio::test]
async fn a_small_file_is_fetched_whole_and_then_read_from_the_cache() {
    let bytes = Bytes::from_static(b"a small file");
    let (namespace, content_ref) = one_file(&bytes);
    let engine = namespace.mount().await;
    let first = open(&engine, "data.bin").await;
    assert_eq!(first.content_state(), ContentState::Absent);
    assert_eq!(read(&first, 2, 5).await, &bytes[2..7]);
    assert_eq!(first.content_state(), ContentState::Complete);
    let second = open(&engine, "data.bin").await;
    assert_eq!(read(&second, 0, 100).await, bytes);

    let fetches = namespace.requests_to("/inodes/ino_14/revisions/1/content");
    assert_eq!(fetches.len(), 1);
    assert_eq!(
        fetches[0].header("range"),
        None,
        "a small file is read whole"
    );
    let cached = namespace
        .state_directory
        .path()
        .join("cache")
        .join(content_ref.content_id.as_str());
    assert_eq!(
        std::fs::read(cached).expect("the complete cache file"),
        bytes
    );
}

/// Checks that sequential reads of a large file fetch growing windows from
/// 1 MiB, each the smallest of twice the last window and the bytes consumed
/// less the bytes fetched but not read, that windows of at most 8 MiB are
/// ranged reads and a larger one is a grant, and that every byte is right.
#[tokio::test]
async fn sequential_reads_fetch_growing_windows() {
    let bytes = pattern(40 * MIB);
    let (namespace, _) = one_file(&bytes);
    let engine = namespace.mount().await;
    let handle = open(&engine, "data.bin").await;
    let chunk = 256 * 1024;
    let mut offset = 0;
    while offset < bytes.len() as u64 {
        let read_bytes = read(&handle, offset, chunk).await;
        let start = usize::try_from(offset).expect("an offset");
        assert_eq!(
            read_bytes,
            bytes[start..start + read_bytes.len()],
            "at {offset}"
        );
        offset += read_bytes.len() as u64;
    }

    let ranged: Vec<String> = namespace
        .requests_to("/inodes/ino_14/revisions/1/content")
        .iter()
        .map(|request| request.header("range").expect("a ranged read").to_owned())
        .collect();
    let half_mib = |value: u64| value * MIB / 2;
    let expected: Vec<String> = [(0, 2), (2, 3), (3, 5), (5, 8), (8, 13), (13, 21), (21, 34)]
        .into_iter()
        .map(|(start, end)| format!("bytes={}-{}", half_mib(start), half_mib(end) - 1))
        .collect();
    assert_eq!(ranged, expected);
    let grants = namespace.requests_to("/inodes/ino_14/revisions/1/downloads");
    assert_eq!(grants.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&grants[0].body).expect("a grant body");
    assert_eq!(body["start_offset"], half_mib(34));
}

/// Checks that an append extends the cached file in place: the item and its
/// open handle follow the new revision, the engine fetches only the appended
/// range, and the kernel drops only the appended pages.
#[tokio::test]
async fn an_append_extends_the_cached_file_and_fetches_the_appended_range() {
    let first = Bytes::from_static(b"line one\n");
    let (namespace, content_ref) = one_file(&first);
    let engine = namespace.mount().await;
    let mut invalidations = engine.subscribe_invalidations();
    let handle = open(&engine, "data.bin").await;
    assert_eq!(read(&handle, 0, 100).await, first);
    let appended = Bytes::from_static(b"line one\nline two\n");
    let extended = content(&content_ref.content_id, &appended);
    namespace.edit(|script| {
        script.contents.insert((14, 2), appended.clone());
    });
    namespace.push_commit(commit(
        6,
        vec![FilesystemChange::ContentChanged {
            inode_id: InodeId(14),
            revision_no: RevisionNo(2),
            content_ref: extended,
            append_offset: Some(9),
        }],
    ));
    refresh(&engine, 6).await;

    let entry = engine
        .lookup(ItemId::ROOT, OsStr::new("data.bin"))
        .expect("the file");
    assert_eq!(entry.item, handle.item(), "an append keeps the item");
    assert_eq!(entry.attr.size_bytes, 18);
    assert_eq!(
        handle.size_bytes(),
        18,
        "the open handle follows the append"
    );
    assert_eq!(read(&handle, 9, 100).await, &appended[9..]);
    let fetches = namespace.requests_to("/inodes/ino_14/revisions/2/content");
    assert_eq!(fetches.len(), 1);
    assert_eq!(fetches[0].header("range"), Some("bytes=9-17"));
    assert_eq!(
        invalidations.try_recv().expect("an invalidation"),
        Invalidation::Inode {
            item: entry.item,
            pages: CachedPages::Range {
                offset: 9,
                length: 9
            },
        }
    );
}

/// Checks that a replacement makes the name refer to a new item, that an
/// open handle keeps reading the revision it opened, and that the engine
/// fetches the rest of that revision through one grant.
#[tokio::test]
async fn a_replacement_makes_a_new_item_and_an_open_handle_keeps_its_revision() {
    let old = pattern(10 * MIB);
    let (namespace, _) = one_file(&old);
    let engine = namespace.mount().await;
    let handle = open(&engine, "data.bin").await;
    assert_eq!(read(&handle, 0, 4096).await, old[..4096]);
    let new = Bytes::from_static(b"replaced");
    namespace.edit(|script| {
        script.contents.insert((14, 2), new.clone());
    });
    namespace.push_commit(commit(
        6,
        vec![FilesystemChange::ContentChanged {
            inode_id: InodeId(14),
            revision_no: RevisionNo(2),
            content_ref: content(&ContentId::generate(), &new),
            append_offset: None,
        }],
    ));
    refresh(&engine, 6).await;

    let replaced = engine
        .lookup(ItemId::ROOT, OsStr::new("data.bin"))
        .expect("the file");
    assert_ne!(replaced.item, handle.item(), "a replacement is a new item");
    assert_eq!(replaced.attr.size_bytes, 8);
    let offset = 9 * MIB;
    let start = usize::try_from(offset).expect("an offset");
    assert_eq!(read(&handle, offset, 16).await, old[start..start + 16]);
    let grants = namespace.requests_to("/inodes/ino_14/revisions/1/downloads");
    assert_eq!(grants.len(), 1, "one grant for the rest of the revision");
    let body: serde_json::Value = serde_json::from_slice(&grants[0].body).expect("a grant body");
    assert_eq!(body["start_offset"], MIB);
    let new_handle = bounded(WAIT, engine.open_read(replaced.item))
        .await
        .expect("the open ends in time")
        .expect("the open");
    assert_eq!(read(&new_handle, 0, 100).await, new);
}

/// Checks that a cache file whose bytes don't match the revision's checksum
/// is deleted and fetched again once, after which reads return the right
/// bytes and the file is complete.
#[tokio::test]
async fn a_cache_file_that_fails_verification_is_fetched_again_once() {
    let bytes = pattern(9 * MIB);
    let (namespace, content_ref) = one_file(&bytes);
    namespace.edit(|script| {
        script.direct_get = false;
        script.corrupt_ranged_reads = 2;
    });
    let engine = namespace.mount().await;
    let handle = open(&engine, "data.bin").await;
    let unverified = read(&handle, 0, 9 * MIB).await;
    assert_ne!(
        unverified, bytes,
        "a ranged read isn't verified as it arrives"
    );
    eventually(|| engine.status().cache.mismatched_files == 1).await;
    assert_eq!(read(&handle, 0, 9 * MIB).await, bytes);
    let complete = namespace
        .state_directory
        .path()
        .join("cache")
        .join(content_ref.content_id.as_str());
    eventually(|| complete.exists()).await;
    assert_eq!(engine.status().cache.mismatched_files, 1);
    assert_eq!(
        namespace
            .requests_to("/inodes/ino_14/revisions/1/content")
            .len(),
        4,
        "two ranged reads, then two again"
    );
}

/// Checks that closing handles evicts the least recently used complete file
/// once unused files pass `--cache-max-bytes`, and that a later read fetches
/// it again.
#[tokio::test]
async fn eviction_keeps_unused_cache_files_under_the_byte_limit() {
    let small = content(&ContentId::generate(), b"12345");
    let other = content(&ContentId::generate(), b"1234567");
    let namespace = ScriptedNamespace::new(
        vec![
            file(14, ROOT, "/work/project/small.txt", &small),
            file(15, ROOT, "/work/project/other.txt", &other),
        ],
        vec![
            (14, Bytes::from_static(b"12345")),
            (15, Bytes::from_static(b"1234567")),
        ],
    );
    let mut config = namespace.config();
    config.max_cache_bytes = 10;
    let engine = namespace.mount_with(config).await.expect("the mount");
    let cache = namespace.state_directory.path().join("cache");

    let handle = open(&engine, "small.txt").await;
    assert_eq!(read(&handle, 0, 10).await, &b"12345"[..]);
    drop(handle);
    let handle = open(&engine, "other.txt").await;
    assert_eq!(read(&handle, 0, 10).await, &b"1234567"[..]);
    drop(handle);
    eventually(|| engine.status().cache.evictable_bytes == 7).await;
    assert!(!cache.join(small.content_id.as_str()).exists());
    assert!(cache.join(other.content_id.as_str()).exists());

    let handle = open(&engine, "small.txt").await;
    assert_eq!(read(&handle, 0, 10).await, &b"12345"[..]);
    assert_eq!(
        namespace
            .requests_to("/inodes/ino_14/revisions/1/content")
            .len(),
        2
    );
}
