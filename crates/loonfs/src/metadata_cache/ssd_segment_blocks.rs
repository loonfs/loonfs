//! The SSD tier of the segment block cache: one file for each metadata
//! segment block, in a directory on the server's local SSD, and an index of
//! those files in memory.
//!
//! The index is an instance of the cache engine, keyed by each block's disk
//! key and charged at the block's stored length or 4 KiB, whichever is
//! larger, so the tier evicts its least recently used block and reports the
//! same counters as every other cache. File reads, writes, and deletes run
//! on the runtime's blocking pool: inserts queue their writes for one writer
//! there, and each read of a block from disk is a blocking task of its own.

use loonfs_core::cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockKey, SegmentBlockKind,
    SegmentCacheKey,
};
use loonfs_core::limits::SSD_WRITE_QUEUE_BYTES;
use loonfs_objectstore::StoreIdentity;
use loonfs_types::format::hex::{hex_decode_bytes, hex_encode_bytes};
use sha2::{Digest, Sha256};
use std::io::{self, Read};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use tokio::task::{JoinError, JoinHandle};

/// The heap that the index allocates for each block on disk.
///
/// Each entry takes its key, its table slot, and two recency positions, as
/// in every instance of the cache engine, and the table and the recency
/// queue grow by doubling. The `cache_heap_accounting` test in this crate
/// measures an index of 16,385 entries, whose recency queue has just
/// doubled. That's 162 bytes for each entry, the most that the test found
/// across sizes, and the test checks that it stays within this value.
pub const SSD_INDEX_ENTRY_BYTES: usize = 168;

/// The least that the tier charges a block against its budget, which is
/// also the block size that the server's sizing formula assumes.
///
/// The index charges each block its stored length or this, whichever is
/// larger, so a tier keeps at most one block for each 4 KiB of its budget,
/// and the formula's estimate of the index's memory is a bound. Without the
/// floor, a budget full of blocks of a few hundred bytes would keep ten
/// times as many entries as the estimate. The cost is disk: a tier of
/// blocks smaller than 4 KiB keeps fewer bytes on disk than its budget.
///
/// Metadata rows compress well, so a block's stored size is much smaller
/// than its decoded size. In a namespace of 20,000 files whose row families
/// compaction had merged into one base run each, the mean stored block was
/// 4.1 KiB: the data blocks of inode and revision rows were about 1.7 KiB
/// each, and each segment's filter was about 13 KiB.
pub const ASSUMED_STORED_BLOCK_BYTES: usize = 4 * 1024;

/// The version of the disk key's encoding. A change to the encoding changes
/// this tag, so the blocks under the old keys are never found again and
/// leave the disk as the tier evicts them.
const DISK_KEY_VERSION: &[u8] = b"loonfs segment block 1";

/// The number of subdirectories, one for each value of a disk key's first
/// byte, so no one directory keeps every file.
const SUBDIRECTORIES: usize = 256;

/// The suffix of a file that the writer hasn't renamed into place yet.
const TEMPORARY_SUFFIX: &str = ".tmp";

/// The SSD tier of the segment block cache, which keeps the stored bytes of
/// metadata segment blocks in a directory on local disk.
///
/// The tier keeps every block that the block cache inserts, under one byte
/// budget, and evicts its least recently used block when it's over the
/// budget. The directory keeps its blocks while the server is stopped, and
/// the tier reads the directory when it opens, so the blocks stay readable
/// across a restart. The directory contains nothing that the server depends
/// on, so an operator can empty it at any time.
///
/// [`Self::close`] waits for the writer to write the blocks it queued. A
/// tier dropped without it stops taking writes, and its writer finishes the
/// queue on the blocking pool.
pub struct SsdSegmentBlocks {
    files: Arc<SsdBlockFiles>,
    /// The writes waiting for the writer, until [`Self::close`] takes the
    /// sender. Without a sender, the writer ends once it has written every
    /// queued block.
    queue: Mutex<Option<mpsc::Sender<QueuedWrite>>>,
    /// The writer's task, until [`Self::close`] waits for it.
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for SsdSegmentBlocks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SsdSegmentBlocks")
            .field("directory", &self.files.directory)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// Counters and occupancy of an [`SsdSegmentBlocks`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SsdSegmentBlocksStats {
    /// Gets that read their block from disk.
    pub hits: usize,
    /// Gets that didn't find their block on disk, or found a damaged or
    /// unreadable file, which the tier deleted.
    pub misses: usize,
    /// Blocks that the tier wrote to disk, or found in its directory when it
    /// opened.
    pub inserts: usize,
    /// Blocks that the tier deleted to stay within its budget.
    pub evictions: usize,
    /// Blocks that the tier didn't write, because the write queue was full,
    /// the write failed, the tier was closed, or the block was larger than
    /// the whole budget.
    pub rejections: usize,
    /// The bytes charged to the blocks on disk: each block's stored length
    /// (or [`ASSUMED_STORED_BLOCK_BYTES`] for a smaller block) and the
    /// index's own allocation for the block. A tier of small blocks keeps
    /// fewer bytes on disk than this.
    pub stored_bytes: usize,
}

/// The key of one block on disk: the first 16 bytes of the SHA-256 of the
/// block's store identity, segment object key, kind, and offset.
///
/// The store's identity, unlike the scope number that a process assigns to
/// each store, is the same in every process, so a restarted server finds its
/// blocks and two stores with the same object key keep separate blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SsdBlockKey([u8; 16]);

impl DecodedBlockKey for SsdBlockKey {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl SsdBlockKey {
    /// Returns the disk key of the block under `key` in `store`.
    pub(crate) fn new(store: &StoreIdentity, key: &SegmentCacheKey) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(DISK_KEY_VERSION);
        add_field(&mut hasher, store.provider.as_bytes());
        add_field(&mut hasher, store.location.as_bytes());
        match &store.key_prefix {
            Some(prefix) => {
                hasher.update([1]);
                add_field(&mut hasher, prefix.as_bytes());
            }
            None => hasher.update([0]),
        }
        add_field(&mut hasher, key.identity.as_bytes());
        hasher.update([match key.block_kind {
            SegmentBlockKind::Index => 0,
            SegmentBlockKind::Filter => 1,
            SegmentBlockKind::Data => 2,
            SegmentBlockKind::Manifest => 3,
        }]);
        hasher.update(key.block_offset.to_le_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest[..16]);
        Self(bytes)
    }
}

/// Adds `field` to `hasher` after its length, so no two sequences of fields
/// hash the same bytes.
fn add_field(hasher: &mut Sha256, field: &[u8]) {
    hasher.update((field.len() as u64).to_le_bytes());
    hasher.update(field);
}

/// The index's entry for one block on disk.
#[derive(Debug, Clone, Copy)]
struct SsdBlockFile {
    stored_bytes: u32,
}

impl DecodedBlock for SsdBlockFile {
    /// Returns the block's stored length, or [`ASSUMED_STORED_BLOCK_BYTES`]
    /// for a smaller block, so the budget bounds the number of entries.
    fn weight(&self) -> usize {
        (self.stored_bytes as usize).max(ASSUMED_STORED_BLOCK_BYTES)
    }
}

/// One block's stored bytes, waiting for the writer.
struct QueuedWrite {
    key: SsdBlockKey,
    stored: Box<[u8]>,
}

impl QueuedWrite {
    /// The bytes that the write takes from the queue's cap: its stored bytes
    /// and its place in the queue.
    fn charge(stored_bytes: usize) -> usize {
        stored_bytes + size_of::<Self>()
    }
}

/// The directory, the index of its files, and the queue's occupancy, which
/// the tier, its writer, and its reads share.
struct SsdBlockFiles {
    directory: PathBuf,
    index: DecodedBlockCache<SsdBlockKey, SsdBlockFile>,
    queued_bytes: AtomicUsize,
    max_queued_bytes: usize,
    /// Whether a failed read, write, or delete has been logged at `warn`.
    /// The rest go to `debug`, because a failing disk fails every file the
    /// same way.
    failure_logged: AtomicBool,
}

/// The writer of an [`SsdSegmentBlocks`], which writes each queued block to
/// a file and adds the file to the index.
///
/// A test gets the writer of a tier from [`SsdSegmentBlocks::open_paused`].
pub struct SsdBlockWriter {
    files: Arc<SsdBlockFiles>,
    queue: mpsc::Receiver<QueuedWrite>,
}

impl SsdBlockWriter {
    /// Writes queued blocks until the tier's sender is gone.
    fn run(self) {
        while let Ok(write) = self.queue.recv() {
            self.files.write(write);
        }
    }

    /// Writes the blocks queued so far, and returns how many it took from
    /// the queue.
    ///
    /// This is a hook for tests, whose tiers don't write until the test
    /// calls it.
    #[cfg(any(test, feature = "test-support"))]
    pub fn write_queued(&self) -> usize {
        let mut written = 0;
        while let Ok(write) = self.queue.try_recv() {
            self.files.write(write);
            written += 1;
        }
        written
    }
}

/// Returns the panic of a blocking task, or an error if the runtime shut
/// down before the task ran.
fn blocking_task_failure(error: JoinError) -> io::Error {
    if error.is_panic() {
        std::panic::resume_unwind(error.into_panic());
    }
    io::Error::other("the runtime shut down before the ssd tier's task ran")
}

impl SsdSegmentBlocks {
    /// Opens the SSD tier in `directory`, which keeps at most
    /// `max_segment_bytes` of blocks, and starts its writer on the runtime's
    /// blocking pool.
    ///
    /// Opening creates the directory if needed, reads the names of the files
    /// in it, and removes every file whose name isn't a block's. If the files
    /// add up to more than the budget, the tier deletes blocks until it's
    /// within the budget. Both the scan and the writer run on the blocking
    /// pool, so this must be called inside a Tokio runtime.
    pub async fn open(
        directory: impl Into<PathBuf>,
        max_segment_bytes: NonZeroUsize,
    ) -> io::Result<Self> {
        let directory = directory.into();
        let (files, queue, writer) = tokio::task::spawn_blocking(move || {
            Self::open_parts(directory, max_segment_bytes, SSD_WRITE_QUEUE_BYTES)
        })
        .await
        .map_err(blocking_task_failure)??;
        let writer = tokio::task::spawn_blocking(move || writer.run());
        Ok(Self {
            files,
            queue: Mutex::new(Some(queue)),
            writer: Mutex::new(Some(writer)),
        })
    }

    /// Opens the tier like [`Self::open`], on the calling thread and with a
    /// write queue of at most `max_queued_bytes`, and returns its writer
    /// instead of starting it.
    ///
    /// This is a hook for tests, which call [`SsdBlockWriter::write_queued`]
    /// to write what the tier queued.
    #[cfg(any(test, feature = "test-support"))]
    pub fn open_paused(
        directory: impl Into<PathBuf>,
        max_segment_bytes: NonZeroUsize,
        max_queued_bytes: usize,
    ) -> io::Result<(Self, SsdBlockWriter)> {
        let (files, queue, writer) =
            Self::open_parts(directory.into(), max_segment_bytes, max_queued_bytes)?;
        let tier = Self {
            files,
            queue: Mutex::new(Some(queue)),
            writer: Mutex::new(None),
        };
        Ok((tier, writer))
    }

    fn open_parts(
        directory: PathBuf,
        max_segment_bytes: NonZeroUsize,
        max_queued_bytes: usize,
    ) -> io::Result<(
        Arc<SsdBlockFiles>,
        mpsc::Sender<QueuedWrite>,
        SsdBlockWriter,
    )> {
        let files = Arc::new(SsdBlockFiles {
            directory,
            index: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_segment_bytes.get())),
            queued_bytes: AtomicUsize::new(0),
            max_queued_bytes,
            failure_logged: AtomicBool::new(false),
        });
        files.load()?;
        let (sender, receiver) = mpsc::channel();
        let writer = SsdBlockWriter {
            files: Arc::clone(&files),
            queue: receiver,
        };
        Ok((files, sender, writer))
    }

    /// Returns the most memory that a tier with a budget of
    /// `max_segment_bytes` keeps: its full write queue, and an index of one
    /// entry for each [`ASSUMED_STORED_BLOCK_BYTES`] of the budget, which is
    /// the most entries that the budget can hold.
    pub fn memory_bytes(max_segment_bytes: NonZeroUsize) -> usize {
        let entries = max_segment_bytes.get() / ASSUMED_STORED_BLOCK_BYTES;
        SSD_WRITE_QUEUE_BYTES.saturating_add(entries.saturating_mul(SSD_INDEX_ENTRY_BYTES))
    }

    /// Returns the tier's counters and occupancy.
    pub fn stats(&self) -> SsdSegmentBlocksStats {
        let index = self.files.index.stats();
        SsdSegmentBlocksStats {
            hits: index.hits,
            misses: index.misses,
            inserts: index.inserts,
            evictions: index.evictions,
            rejections: index.rejections,
            stored_bytes: index.cached_decoded_bytes,
        }
    }

    /// Stops taking writes, and waits for the writer to write the blocks
    /// that it queued.
    ///
    /// A host calls this when it shuts down, once no runtime reads through
    /// the tier, so the blocks that reads loaded last are on disk for the
    /// next start. After it, an insert writes nothing and records a
    /// rejection, and gets still read the blocks on disk.
    pub async fn close(&self) {
        drop(self.lock_queue().take());
        let writer = self
            .writer
            .lock()
            .expect("the ssd tier's writer lock should not be poisoned")
            .take();
        let Some(writer) = writer else {
            return;
        };
        match writer.await {
            Ok(()) => {}
            Err(error) if error.is_panic() => tracing::error!(
                directory = %self.files.directory.display(),
                "the ssd tier's writer panicked"
            ),
            Err(_) => {}
        }
    }

    fn lock_queue(&self) -> std::sync::MutexGuard<'_, Option<mpsc::Sender<QueuedWrite>>> {
        self.queue
            .lock()
            .expect("the ssd tier's queue lock should not be poisoned")
    }

    /// Queues a write of `stored` under `key`, or marks the block recently
    /// used if the disk already keeps it.
    ///
    /// If the queue doesn't have room for the block, or the tier is closed,
    /// the tier writes nothing and records a rejection.
    pub(crate) fn insert(&self, key: SsdBlockKey, stored: &[u8]) {
        if self.files.index.touch(&key) {
            return;
        }
        let charge = QueuedWrite::charge(stored.len());
        let max_queued_bytes = self.files.max_queued_bytes;
        let reserved =
            self.files
                .queued_bytes
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |queued| {
                    queued
                        .checked_add(charge)
                        .filter(|queued| *queued <= max_queued_bytes)
                });
        if reserved.is_err() {
            self.files.index.record_rejection(stored.len());
            return;
        }
        let write = QueuedWrite {
            key,
            stored: stored.into(),
        };
        // The send fails if the tier is closed, or if a panic stopped the
        // writer.
        let sent = self
            .lock_queue()
            .as_ref()
            .is_some_and(|queue| queue.send(write).is_ok());
        if !sent {
            self.files.queued_bytes.fetch_sub(charge, Ordering::SeqCst);
            self.files.index.record_rejection(stored.len());
        }
    }

    /// Marks the block under `key` recently used, if the disk keeps it.
    pub(crate) fn touch(&self, key: &SsdBlockKey) {
        self.files.index.touch(key);
    }

    /// Reads the block under `key` from disk on the blocking pool, and
    /// returns what `decode` returns for its bytes there, or `None` if the
    /// disk doesn't keep the block.
    ///
    /// If the file can't be read, its length isn't the block's stored
    /// length, or `decode` returns an error (for example, because the bytes
    /// don't match the caller's handle), the tier deletes the file, records a
    /// miss, and returns `None`. A file of the wrong length is deleted
    /// without being read.
    pub(crate) async fn read<T, E>(
        &self,
        key: &SsdBlockKey,
        decode: impl FnOnce(Vec<u8>) -> Result<T, E> + Send + 'static,
    ) -> Option<T>
    where
        T: Send + 'static,
        E: std::fmt::Display,
    {
        let Some(file) = self.files.index.peek(key) else {
            self.files.index.record_miss();
            return None;
        };
        let files = Arc::clone(&self.files);
        let path = self.files.path(key, file.stored_bytes, FileName::Block);
        let stored_bytes = file.stored_bytes;
        let read = tokio::task::spawn_blocking(move || {
            let decoded = match read_block_file(&path, stored_bytes) {
                Ok(stored) => decode(stored).map_err(|error| Some(error.to_string())),
                // The writer evicted the block after the lookup above, or an
                // operator emptied the directory.
                Err(error) if error.kind() == io::ErrorKind::NotFound => Err(None),
                Err(error) => Err(Some(error.to_string())),
            };
            if let Err(failure) = &decoded {
                if let Some(failure) = failure {
                    files.log_failure(&path, &format!("the block can't be used: {failure}"));
                }
                files.remove(&path);
            }
            decoded.ok()
        })
        .await;
        let decoded = match read {
            Ok(decoded) => decoded,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // The runtime is shutting down, and the read didn't run.
            Err(_) => return None,
        };
        match decoded {
            Some(block) => {
                self.files.index.touch(key);
                self.files.index.record_hit();
                Some(block)
            }
            None => {
                // The read deleted the file before this drops the entry, so
                // the writer, which writes only the blocks that the index
                // doesn't have, can't write a new copy that the read would
                // then delete.
                self.files.index.invalidate(key);
                self.files.index.record_miss();
                None
            }
        }
    }

    /// Returns the path of the file that keeps the block under `key`.
    #[cfg(test)]
    pub(crate) fn block_path(&self, key: &SsdBlockKey, stored_bytes: u32) -> PathBuf {
        self.files.path(key, stored_bytes, FileName::Block)
    }

    /// Returns the bytes that the index charges a block of `stored_bytes`.
    #[cfg(test)]
    pub(crate) fn charged_bytes(stored_bytes: u32) -> usize {
        let index = DecodedBlockCache::new(DecodedBlockCacheConfig::new(usize::MAX));
        index.insert(SsdBlockKey([0; 16]), SsdBlockFile { stored_bytes });
        index.stats().cached_decoded_bytes
    }

    /// Returns the bytes that a write of `stored_bytes` takes from the
    /// queue's cap.
    #[cfg(test)]
    pub(crate) fn queue_charge(stored_bytes: usize) -> usize {
        QueuedWrite::charge(stored_bytes)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl SsdSegmentBlocks {
    /// Queues a write of `stored` under the disk key of the block under
    /// `key` in `store`, as an insert into the block cache does.
    ///
    /// This is a hook for tests that fill the tier without a block cache.
    pub fn insert_stored(&self, store: &StoreIdentity, key: &SegmentCacheKey, stored: &[u8]) {
        self.insert(SsdBlockKey::new(store, key), stored);
    }

    /// Marks the block under `key` in `store` recently used, as a hit in the
    /// memory tier does.
    ///
    /// This is a hook for tests that fill the tier without a block cache.
    pub fn touch_stored(&self, store: &StoreIdentity, key: &SegmentCacheKey) {
        self.touch(&SsdBlockKey::new(store, key));
    }
}

/// Which of a block's two file names a path ends in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileName {
    /// The name of a block on disk.
    Block,
    /// The name of a block that the writer is still writing.
    Temporary,
}

impl SsdBlockFiles {
    /// Returns the path of the block under `key`, of `stored_bytes`.
    ///
    /// The file's name is the key in hexadecimal and the stored length, in
    /// the subdirectory for the key's first byte, as in
    /// `3f/3fa2...91-18342`, so the scan when the tier opens reads the
    /// stored length from the name instead of from the file.
    fn path(&self, key: &SsdBlockKey, stored_bytes: u32, name: FileName) -> PathBuf {
        let hex = hex_encode_bytes(&key.0);
        let suffix = match name {
            FileName::Block => "",
            FileName::Temporary => TEMPORARY_SUFFIX,
        };
        self.directory
            .join(&hex[..2])
            .join(format!("{hex}-{stored_bytes}{suffix}"))
    }

    /// Builds the index from the names of the files in the directory, and
    /// removes every file whose name isn't a block's, such as a file that a
    /// writer didn't rename into place before the server stopped.
    ///
    /// The scan's order becomes the blocks' recency, because a file name
    /// doesn't record when the block was last used.
    fn load(&self) -> io::Result<()> {
        let mut removed = 0usize;
        for first_byte in 0..SUBDIRECTORIES {
            let subdirectory = self.directory.join(format!("{first_byte:02x}"));
            std::fs::create_dir_all(&subdirectory)?;
            for entry in std::fs::read_dir(&subdirectory)? {
                let entry = entry?;
                let block = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| parse_block_name(first_byte, name))
                    .filter(|(key, _)| self.index.peek(key).is_none());
                let Some((key, stored_bytes)) = block else {
                    self.remove(&entry.path());
                    removed += 1;
                    continue;
                };
                self.add(key, stored_bytes);
            }
        }
        let index = self.index.stats();
        tracing::info!(
            directory = %self.directory.display(),
            blocks = index.inserts - index.evictions,
            removed,
            stored_bytes = index.cached_decoded_bytes,
            "the ssd tier loaded the blocks in its directory"
        );
        Ok(())
    }

    /// Writes one queued block to its file, and adds the file to the index.
    fn write(&self, write: QueuedWrite) {
        let QueuedWrite { key, stored } = write;
        // The index has the block already if it was queued twice.
        if self.index.peek(&key).is_none() {
            let stored_bytes = u32::try_from(stored.len())
                .expect("a block's stored length comes from its handle, which records it as u32");
            match self.write_file(&key, stored_bytes, &stored) {
                Ok(()) => self.add(key, stored_bytes),
                Err(error) => {
                    let path = self.path(&key, stored_bytes, FileName::Block);
                    self.log_failure(&path, &format!("the block can't be written: {error}"));
                    self.index.record_rejection(stored.len());
                }
            }
        }
        self.queued_bytes
            .fetch_sub(QueuedWrite::charge(stored.len()), Ordering::SeqCst);
    }

    /// Writes `stored` to a temporary file and renames it into place, so a
    /// block's file never has part of its bytes. (A crash can still leave
    /// a damaged file behind, which the check on a disk hit finds.)
    ///
    /// An operator can empty the directory while the server runs, so a
    /// write that finds its subdirectory gone creates it again.
    fn write_file(&self, key: &SsdBlockKey, stored_bytes: u32, stored: &[u8]) -> io::Result<()> {
        let temporary = self.path(key, stored_bytes, FileName::Temporary);
        let mut written = std::fs::write(&temporary, stored);
        if written
            .as_ref()
            .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
        {
            if let Some(subdirectory) = temporary.parent() {
                written = std::fs::create_dir_all(subdirectory)
                    .and_then(|()| std::fs::write(&temporary, stored));
            }
        }
        let renamed = written.and_then(|()| {
            std::fs::rename(&temporary, self.path(key, stored_bytes, FileName::Block))
        });
        if renamed.is_err() {
            self.remove(&temporary);
        }
        renamed
    }

    /// Adds the block under `key`, whose file exists, to the index, and
    /// deletes the files of the blocks that the index evicts. If the block
    /// is larger than the whole budget, the index doesn't keep it, and its
    /// file goes too.
    fn add(&self, key: SsdBlockKey, stored_bytes: u32) {
        let insertion = self
            .index
            .insert_evicting(key, SsdBlockFile { stored_bytes });
        if !insertion.kept {
            self.remove(&self.path(&key, stored_bytes, FileName::Block));
        }
        for (evicted, file) in insertion.evicted {
            self.remove(&self.path(&evicted, file.stored_bytes, FileName::Block));
        }
    }

    /// Deletes the file at `path`. A file that's already gone is fine.
    fn remove(&self, path: &Path) {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => self.log_failure(path, &format!("the file can't be deleted: {error}")),
        }
    }

    fn log_failure(&self, path: &Path, reason: &str) {
        if self.failure_logged.swap(true, Ordering::Relaxed) {
            tracing::debug!(path = %path.display(), reason, "ssd tier file failure");
        } else {
            tracing::warn!(
                path = %path.display(),
                reason,
                "ssd tier file failure, and later failures are logged at debug"
            );
        }
    }
}

/// Reads the file at `path`, which keeps a block of `stored_bytes`, without
/// reading more than that.
///
/// A file of any other length is damaged, so this returns an error before it
/// reads any of the file, and a damaged file can't make the read allocate
/// more than the block.
fn read_block_file(path: &Path, stored_bytes: u32) -> io::Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    if length != u64::from(stored_bytes) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the file has {length} bytes, and the block has {stored_bytes}"),
        ));
    }
    let mut stored = Vec::with_capacity(stored_bytes as usize);
    // The file can grow after the length check, so the read stops at the
    // block's length, and the decode checks the length again.
    file.take(u64::from(stored_bytes))
        .read_to_end(&mut stored)?;
    Ok(stored)
}

/// Returns the key and the stored length of the block whose file in the
/// subdirectory for `first_byte` is named `name`, or `None` if `name` isn't
/// a block's.
fn parse_block_name(first_byte: usize, name: &str) -> Option<(SsdBlockKey, u32)> {
    let (hex, stored_bytes) = name.split_once('-')?;
    let key: [u8; 16] = hex_decode_bytes(hex).ok()?.try_into().ok()?;
    let stored_bytes: u32 = stored_bytes.parse().ok()?;
    // Only the name the writer writes is a block's, so `+12` or `012` isn't.
    let canonical = format!("{hex}-{stored_bytes}") == name;
    (canonical && usize::from(key[0]) == first_byte).then_some((SsdBlockKey(key), stored_bytes))
}
