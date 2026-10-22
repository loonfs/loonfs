//! The tiered block cache: what the SSD tier writes, what it reads back
//! after memory evicts a block, what it evicts, and what it finds after a
//! restart.

use super::super::ssd_segment_blocks::{
    SsdBlockKey, SsdBlockWriter, SsdSegmentBlocks, ASSUMED_STORED_BLOCK_BYTES,
};
use super::TieredSegmentBlocks;
use bytes::Bytes;
use futures::poll;
use loonfs_core::cache::{
    decode_stored_block, DecodedMetadataSegmentBlock, LoadedSegmentBlock, MemorySegmentBlocks,
    MetadataSegmentCacheKey, ObjectStoreScope, SegmentBlockCache, SegmentBlockKind,
    SharedSegmentBlocks,
};
use loonfs_core::ManifestLoadError;
use loonfs_objectstore::StoreIdentity;
use loonfs_types::format::manifest::{InodeRecord, MetadataRow, MetadataRowFamily};
use loonfs_types::format::segment_blocks::{decode_index_block, BlockHandle, SegmentBlocksBuilder};
use loonfs_types::{ChangeSeq, CommitId, InodeId, InodeKind};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

const MIB: usize = 1024 * 1024;

/// One data block of a test segment.
struct TestBlock {
    key: MetadataSegmentCacheKey,
    handle: BlockHandle,
    block: DecodedMetadataSegmentBlock,
    stored: Bytes,
}

impl TestBlock {
    /// Returns the load of this block from the store, which counts itself in
    /// `loads`.
    fn load(&self, loads: &AtomicUsize) -> Result<LoadedSegmentBlock, ManifestLoadError> {
        loads.fetch_add(1, Ordering::SeqCst);
        Ok(LoadedSegmentBlock {
            block: self.block.clone(),
            stored: self.stored.clone(),
        })
    }
}

fn section(handle: &BlockHandle) -> Range<usize> {
    let start = usize::try_from(handle.offset).expect("a test segment is small");
    start..start + handle.stored_bytes as usize
}

/// Returns the data blocks of a segment of twelve inode rows, one row in
/// each block.
fn test_blocks() -> Vec<TestBlock> {
    let mut builder = SegmentBlocksBuilder::new(NonZeroUsize::MIN);
    for inode in 0..12 {
        let row = MetadataRow::Inode(InodeRecord {
            inode_id: InodeId(inode + 2),
            inode_kind: InodeKind::File,
            committed_seq: ChangeSeq(inode + 1),
            commit_id: CommitId::parse(format!("c_ssd_{inode:02}")).expect("commit id"),
            committed_by: loonfs_test_support::test_actor(),
            committed_at_ms: 1_000 + inode,
        });
        let family = MetadataRowFamily::Inodes;
        builder
            .push(
                &row.row_key_for_family(family),
                &row.filter_key_for_family(family),
                &row,
            )
            .expect("encode a row");
    }
    let built = builder.build().expect("build the segment");
    let index = decode_index_block(&built.bytes[section(&built.index)], &built.index)
        .expect("decode the index");
    index
        .iter()
        .map(|entry| {
            let handle = entry.block;
            let stored = Bytes::copy_from_slice(&built.bytes[section(&handle)]);
            let key = MetadataSegmentCacheKey {
                identity: "namespaces/demo/segments/seg_ssd".to_owned(),
                block_kind: SegmentBlockKind::Data,
                block_offset: handle.offset,
            };
            let block = decode_stored_block(&key.identity, key.block_kind, &handle, &stored)
                .expect("a stored block decodes");
            TestBlock {
                key,
                handle,
                block,
                stored,
            }
        })
        .collect()
}

fn store(location: &str) -> StoreIdentity {
    StoreIdentity {
        provider: "local_fs",
        location: location.to_owned(),
        key_prefix: None,
    }
}

/// Opens an SSD tier in `directory` with a budget of `max_segment_bytes`
/// and a write queue of `max_queued_bytes`, whose writer writes when the
/// test calls it.
fn open_tier(
    directory: &Path,
    max_segment_bytes: usize,
    max_queued_bytes: usize,
) -> (Arc<SsdSegmentBlocks>, SsdBlockWriter) {
    let budget = NonZeroUsize::new(max_segment_bytes).expect("a nonzero budget");
    let (ssd, writer) =
        SsdSegmentBlocks::open_paused(directory, budget, max_queued_bytes).expect("open the tier");
    (Arc::new(ssd), writer)
}

/// A tiered block cache over a new memory tier, and a view of that memory
/// tier alone.
struct Tiered {
    blocks: TieredSegmentBlocks,
    memory: MemorySegmentBlocks,
}

impl Tiered {
    /// Returns a tiered cache over `ssd` and a new memory tier of
    /// `max_memory_bytes`, for the store `store` under the scope `scope`.
    fn new(
        ssd: &Arc<SsdSegmentBlocks>,
        store: &StoreIdentity,
        scope: u64,
        max_memory_bytes: usize,
    ) -> Self {
        let shared = Arc::new(SharedSegmentBlocks::new(max_memory_bytes));
        let scope = ObjectStoreScope::new(scope);
        Self {
            blocks: TieredSegmentBlocks::new(
                MemorySegmentBlocks::new(Arc::clone(&shared), scope),
                Arc::clone(ssd),
                store.clone(),
            ),
            memory: MemorySegmentBlocks::new(shared, scope),
        }
    }

    fn insert(&self, block: &TestBlock) {
        self.blocks
            .insert_block(block.key.clone(), block.block.clone(), &block.stored);
    }

    async fn in_memory(&self, block: &TestBlock) -> bool {
        self.memory
            .get_block(&block.key, &block.handle)
            .await
            .is_some()
    }
}

/// Returns the bytes that a memory tier charges `block`.
fn memory_charge(block: &TestBlock) -> usize {
    let shared = Arc::new(SharedSegmentBlocks::new(usize::MAX));
    MemorySegmentBlocks::new(Arc::clone(&shared), ObjectStoreScope::new(0)).insert_block(
        block.key.clone(),
        block.block.clone(),
        &block.stored,
    );
    shared.stats().cached_decoded_bytes
}

/// Returns the path of the file that keeps `block` of `store`.
fn file_of(ssd: &SsdSegmentBlocks, store: &StoreIdentity, block: &TestBlock) -> PathBuf {
    ssd.block_path(
        &SsdBlockKey::new(store, &block.key),
        block.handle.stored_bytes,
    )
}

fn rows(block: &DecodedMetadataSegmentBlock) -> Vec<MetadataRow> {
    block
        .clone()
        .data()
        .expect("a test block is a data block")
        .rows
        .clone()
}

/// Checks that an inserted block goes into memory, and into a file with
/// exactly the block's stored bytes once the writer runs, and that an insert
/// of a block that the disk already keeps writes nothing.
#[tokio::test]
async fn an_inserted_block_goes_to_memory_and_to_a_file_of_its_stored_bytes() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let block = &blocks[0];
    let directory = tempdir().expect("tempdir");
    let (ssd, writer) = open_tier(directory.path(), MIB, MIB);
    let cache = Tiered::new(&ssd, &store, 0, MIB);

    cache.insert(block);
    assert!(cache.in_memory(block).await, "the block is in memory");
    let path = file_of(&ssd, &store, block);
    assert!(!path.exists(), "the writer hasn't run yet");
    assert_eq!(writer.write_queued(), 1);
    assert_eq!(
        std::fs::read(&path).expect("the block's file"),
        block.stored
    );
    let stats = ssd.stats();
    assert_eq!(
        (stats.inserts, stats.stored_bytes),
        (
            1,
            SsdSegmentBlocks::charged_bytes(block.handle.stored_bytes)
        )
    );

    cache.insert(block);
    assert_eq!(writer.write_queued(), 0, "the disk keeps the block already");
}

/// Checks that a get of a block that memory evicted reads the block from
/// disk, decodes the same rows, and puts the block back in memory.
#[tokio::test]
async fn a_block_that_memory_evicted_is_a_disk_hit_that_decodes_the_same_block() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let (first, second) = (&blocks[0], &blocks[1]);
    let directory = tempdir().expect("tempdir");
    let (ssd, writer) = open_tier(directory.path(), MIB, MIB);
    // Memory fits one of the two blocks.
    let budget = memory_charge(first).max(memory_charge(second));
    let cache = Tiered::new(&ssd, &store, 0, budget);
    cache.insert(first);
    cache.insert(second);
    assert_eq!(writer.write_queued(), 2);
    assert!(
        !cache.in_memory(first).await,
        "memory evicted the first block"
    );

    let hit = cache
        .blocks
        .get_block(&first.key, &first.handle)
        .await
        .expect("a disk hit");
    assert_eq!(rows(&hit), rows(&first.block));
    let stats = ssd.stats();
    assert_eq!((stats.hits, stats.misses), (1, 0));
    assert!(
        cache.in_memory(first).await,
        "the disk hit is in memory again"
    );
}

/// Checks that the tier over its budget evicts its least recently used
/// block and deletes the block's file, and that a hit in memory marks the
/// block recently used on disk too, so the disk evicts another block.
#[tokio::test]
async fn the_tier_evicts_its_least_recently_used_block_and_a_memory_hit_refreshes_it() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let [a, b, c] = [&blocks[0], &blocks[1], &blocks[2]];
    let charge = |block: &TestBlock| SsdSegmentBlocks::charged_bytes(block.handle.stored_bytes);
    for (hit_a, evicted) in [(false, a), (true, b)] {
        let directory = tempdir().expect("tempdir");
        // The disk fits any two of the three blocks.
        let budget = charge(a) + charge(b) + charge(c) - 1;
        let (ssd, writer) = open_tier(directory.path(), budget, MIB);
        let cache = Tiered::new(&ssd, &store, 0, MIB);
        cache.insert(a);
        cache.insert(b);
        assert_eq!(writer.write_queued(), 2);
        if hit_a {
            let hit = cache.blocks.get_block(&a.key, &a.handle).await;
            assert!(hit.is_some(), "a memory hit");
        }
        cache.insert(c);
        assert_eq!(writer.write_queued(), 1);

        let stats = ssd.stats();
        assert_eq!((stats.evictions, stats.hits), (1, 0), "{stats:?}");
        for block in [a, b, c] {
            assert_eq!(
                file_of(&ssd, &store, block).exists(),
                !std::ptr::eq(block, evicted),
                "only the evicted block's file is gone (memory hit first: {hit_a})"
            );
        }
    }
}

/// How a test damages a block's file.
#[derive(Debug, Clone, Copy)]
enum Damage {
    ChangeOneByte,
    Delete,
}

/// Checks that a block whose file was changed fails the check against its
/// handle, that a block whose file is gone can't be read, and that in both
/// cases the tier deletes the entry and the get is a miss, so the next get
/// or load loads the block from the store and writes it again.
#[tokio::test]
async fn a_damaged_or_missing_file_is_deleted_and_the_get_is_a_miss() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let block = &blocks[0];
    for damage in [Damage::ChangeOneByte, Damage::Delete] {
        let directory = tempdir().expect("tempdir");
        let (ssd, writer) = open_tier(directory.path(), MIB, MIB);
        // Memory keeps nothing, so every get reads the disk.
        let cache = Tiered::new(&ssd, &store, 0, 0);
        cache.insert(block);
        assert_eq!(writer.write_queued(), 1);
        let path = file_of(&ssd, &store, block);
        match damage {
            Damage::ChangeOneByte => {
                let mut stored = std::fs::read(&path).expect("the block's file");
                stored[0] ^= 0xff;
                std::fs::write(&path, stored).expect("change the file");
            }
            Damage::Delete => std::fs::remove_file(&path).expect("delete the file"),
        }

        let get = cache.blocks.get_block(&block.key, &block.handle).await;
        assert!(get.is_none(), "{damage:?}: the get is a miss");
        assert!(!path.exists(), "{damage:?}: the file is deleted");
        let stats = ssd.stats();
        assert_eq!((stats.hits, stats.misses), (0, 1), "{damage:?}");

        let loads = AtomicUsize::new(0);
        let loaded = cache
            .blocks
            .get_or_load_block(
                &block.key,
                &block.handle,
                Box::pin(async { block.load(&loads) }),
            )
            .await
            .expect("a load from the store");
        assert_eq!(rows(&loaded), rows(&block.block));
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(ssd.stats().misses, 2, "{damage:?}: the entry is gone");
        assert_eq!(writer.write_queued(), 1);
        assert_eq!(
            std::fs::read(&path).expect("the block's file"),
            block.stored,
            "{damage:?}: the load writes the block again"
        );
    }
}

/// Checks that a tier opened over the directory of an earlier tier finds
/// the earlier tier's blocks and reads them as disk hits under another
/// scope, removes the files in its subdirectories whose names aren't a
/// block's, and leaves the directory's other files alone.
#[tokio::test]
async fn a_new_tier_over_the_same_directory_finds_the_blocks() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let directory = tempdir().expect("tempdir");
    let temporary = {
        let (ssd, writer) = open_tier(directory.path(), MIB, MIB);
        let cache = Tiered::new(&ssd, &store, 0, MIB);
        cache.insert(&blocks[0]);
        cache.insert(&blocks[1]);
        assert_eq!(writer.write_queued(), 2);
        PathBuf::from(format!(
            "{}.tmp",
            file_of(&ssd, &store, &blocks[2]).display()
        ))
    };
    let stray = directory.path().join("00").join("not-a-block");
    let outside = directory.path().join("notes.txt");
    for path in [&temporary, &stray, &outside] {
        std::fs::write(path, b"stray").expect("write a stray file");
    }

    let (ssd, _writer) = open_tier(directory.path(), MIB, MIB);
    assert_eq!(ssd.stats().inserts, 2, "the new tier finds both blocks");
    assert!(!temporary.exists() && !stray.exists());
    assert!(outside.exists(), "a file outside the subdirectories stays");
    let cache = Tiered::new(&ssd, &store, 7, MIB);
    for block in &blocks[..2] {
        let hit = cache
            .blocks
            .get_block(&block.key, &block.handle)
            .await
            .expect("a disk hit after the restart");
        assert_eq!(rows(&hit), rows(&block.block));
    }
    assert_eq!(ssd.stats().hits, 2);
}

/// Checks that two stores with a block under the same object key keep two
/// files, and that each store's cache reads its own block from disk.
#[tokio::test]
async fn two_stores_with_the_same_object_key_keep_separate_entries() {
    let blocks = test_blocks();
    let (first_store, second_store) = (store("/a"), store("/b"));
    let directory = tempdir().expect("tempdir");
    let (ssd, writer) = open_tier(directory.path(), MIB, MIB);
    // Memory keeps nothing, so each get reads the disk.
    let first = Tiered::new(&ssd, &first_store, 0, 0);
    let second = Tiered::new(&ssd, &second_store, 1, 0);
    let key = &blocks[0].key;
    for (cache, block) in [(&first, &blocks[0]), (&second, &blocks[1])] {
        cache
            .blocks
            .insert_block(key.clone(), block.block.clone(), &block.stored);
    }
    assert_eq!(writer.write_queued(), 2);
    assert_eq!(
        ssd.stats().inserts,
        2,
        "each store's block has its own entry"
    );
    for (cache, block) in [(&first, &blocks[0]), (&second, &blocks[1])] {
        let hit = cache
            .blocks
            .get_block(key, &block.handle)
            .await
            .expect("a disk hit");
        assert_eq!(rows(&hit), rows(&block.block));
    }
}

/// Checks that an insert that finds the write queue full writes nothing to
/// disk and records a rejection, that the block stays in memory, and that
/// the queue takes writes again once the writer has caught up.
#[tokio::test]
async fn a_full_write_queue_rejects_the_write_and_the_block_stays_in_memory() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let (first, second) = (&blocks[0], &blocks[1]);
    let directory = tempdir().expect("tempdir");
    // The queue fits either block, but not both.
    let queue = SsdSegmentBlocks::queue_charge(first.stored.len().max(second.stored.len()));
    let (ssd, writer) = open_tier(directory.path(), MIB, queue);
    let cache = Tiered::new(&ssd, &store, 0, MIB);
    cache.insert(first);
    cache.insert(second);
    assert_eq!(ssd.stats().rejections, 1);
    assert!(cache.in_memory(second).await, "the block is in memory");
    assert_eq!(writer.write_queued(), 1);
    assert!(file_of(&ssd, &store, first).exists());
    assert!(!file_of(&ssd, &store, second).exists());

    cache.insert(second);
    assert_eq!(writer.write_queued(), 1, "the writer emptied the queue");
    assert!(file_of(&ssd, &store, second).exists());
}

/// Checks that concurrent gets or loads of one key share one load, which
/// reads the disk once, loads from the store once, and writes the block
/// once.
#[tokio::test]
async fn concurrent_get_or_load_callers_of_one_key_share_one_load() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let block = &blocks[0];
    let directory = tempdir().expect("tempdir");
    let (ssd, writer) = open_tier(directory.path(), MIB, MIB);
    let cache = Tiered::new(&ssd, &store, 0, MIB);
    let loads = AtomicUsize::new(0);
    let (release, released) = tokio::sync::watch::channel(false);
    let get_or_load = || {
        cache.blocks.get_or_load_block(
            &block.key,
            &block.handle,
            Box::pin(async {
                released
                    .clone()
                    .wait_for(|released| *released)
                    .await
                    .expect("the test keeps the sender");
                block.load(&loads)
            }),
        )
    };
    let mut first = pin!(get_or_load());
    assert!(poll!(first.as_mut()).is_pending());
    let mut second = pin!(get_or_load());
    assert!(poll!(second.as_mut()).is_pending());
    release.send(true).expect("the load waits for the release");
    let (first, second) = futures::join!(first, second);
    for loaded in [first, second] {
        assert_eq!(rows(&loaded.expect("the shared load")), rows(&block.block));
    }
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(ssd.stats().misses, 1, "the shared load reads the disk once");
    assert_eq!(writer.write_queued(), 1, "the shared load writes once");
}

/// Checks that closing a tier waits for the writer to write what it
/// queued, and that an insert after the close writes nothing and records a
/// rejection.
#[tokio::test]
async fn closing_the_tier_writes_its_queue_and_stops_taking_writes() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let directory = tempdir().expect("tempdir");
    let budget = NonZeroUsize::new(MIB).expect("nonzero");
    let ssd = Arc::new(
        SsdSegmentBlocks::open(directory.path(), budget)
            .await
            .expect("open the tier"),
    );
    let cache = Tiered::new(&ssd, &store, 0, MIB);
    for block in &blocks[..4] {
        cache.insert(block);
    }
    ssd.close().await;
    for block in &blocks[..4] {
        assert_eq!(
            std::fs::read(file_of(&ssd, &store, block)).expect("the block's file"),
            block.stored
        );
    }
    cache.insert(&blocks[4]);
    let stats = ssd.stats();
    assert_eq!((stats.inserts, stats.rejections), (4, 1));
}

/// Checks that the tier charges a block smaller than 4 KiB as 4 KiB, so a
/// budget of 64 times 4 KiB filled with blocks of one byte keeps at most 64
/// of them, the number that the sizing formula's estimate of the index
/// assumes.
#[tokio::test]
async fn a_budget_full_of_tiny_blocks_keeps_at_most_one_block_for_each_4_kib() {
    const SLOTS: usize = 64;
    let store = store("/a");
    let directory = tempdir().expect("tempdir");
    let (ssd, writer) = open_tier(directory.path(), SLOTS * ASSUMED_STORED_BLOCK_BYTES, MIB);
    let blocks = 4 * SLOTS;
    for offset in 0..blocks {
        let key = MetadataSegmentCacheKey {
            identity: "namespaces/demo/segments/seg_tiny".to_owned(),
            block_kind: SegmentBlockKind::Data,
            block_offset: offset as u64,
        };
        ssd.insert_stored(&store, &key, b"x");
    }
    assert_eq!(writer.write_queued(), blocks);
    let stats = ssd.stats();
    let kept = stats.inserts - stats.evictions;
    assert!(
        (1..=SLOTS).contains(&kept),
        "a budget of {SLOTS} blocks of 4 KiB keeps {kept} blocks of one byte"
    );
    let files: usize = (0..256)
        .map(|first_byte| {
            std::fs::read_dir(directory.path().join(format!("{first_byte:02x}")))
                .expect("a subdirectory")
                .count()
        })
        .sum();
    assert_eq!(files, kept, "the evicted blocks' files are gone");
}

/// Checks that a file that grew past its block's stored length is deleted
/// and the get is a miss, without the tier reading the file: the decode
/// never receives a byte of it.
#[tokio::test]
async fn a_file_longer_than_its_block_is_deleted_without_being_read() {
    let (blocks, store) = (test_blocks(), store("/a"));
    let block = &blocks[0];
    let directory = tempdir().expect("tempdir");
    let (ssd, writer) = open_tier(directory.path(), MIB, MIB);
    let cache = Tiered::new(&ssd, &store, 0, 0);
    cache.insert(block);
    assert_eq!(writer.write_queued(), 1);
    let path = file_of(&ssd, &store, block);
    let mut grown = std::fs::read(&path).expect("the block's file");
    grown.resize(4 * MIB, 0);
    std::fs::write(&path, grown).expect("grow the file");

    let decoded_bytes = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&decoded_bytes);
    let read = ssd
        .read(&SsdBlockKey::new(&store, &block.key), move |stored| {
            seen.fetch_add(stored.len(), Ordering::SeqCst);
            Ok::<_, String>(())
        })
        .await;
    assert!(read.is_none(), "the get is a miss");
    assert_eq!(
        decoded_bytes.load(Ordering::SeqCst),
        0,
        "the tier read none of the file"
    );
    assert!(!path.exists(), "the file is deleted");
    let stats = ssd.stats();
    assert_eq!((stats.hits, stats.misses), (0, 1));
}
