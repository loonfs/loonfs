//! Node-local cache of encoded metadata blocks, implemented with foyer.
//!
//! Higher layers use the [`StoredMetadataBlockCache`] interface rather than
//! foyer types. Cache reads and inserts are best effort: failures are recorded
//! and treated as misses or dropped inserts. Closing is the only fallible
//! operation exposed to the caller.
//!
//! An exclusive file lock gives one server process ownership of the cache
//! directory. A second process configured with the same directory fails at
//! startup.

use crate::config::{LocalCacheConfig, ServerConfigError};
use async_trait::async_trait;
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Compression, DeviceBuilder, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCachePolicy, PsyncIoEngineConfig, RecoverMode,
};
use loonfs::metrics::{CounterHandle, MetricsRecorder};
use loonfs::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
    StoredMetadataBlockKind,
};
use mixtrics::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec,
    CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// The directory beneath the configured root that the disk tier writes.
///
/// The name carries the layout version. Nothing under it is durable, so a
/// layout change takes a new name and the old directory is deleted rather
/// than migrated.
const CACHE_DIRECTORY: &str = "metadata-blocks-v1";

/// The lock file beneath the configured root, held open and locked for as
/// long as this process owns the directory.
const LOCK_FILE: &str = "owner.lock";

/// The geometry marker inside the versioned directory, which records the
/// disk geometry that directory was last built for. See [`DiskGeometry`].
///
/// The name cannot collide with a block file: foyer names those from a
/// fixed prefix and a number.
const GEOMETRY_MARKER: &str = "geometry.json";

/// The name foyer knows this cache by, which is also the label its own
/// counters carry.
const CACHE_NAME: &str = "loonfs-metadata-blocks";

// These disk-engine values remain fixed until production measurements support
// exposing them as deployment settings.

/// Size of one disk allocation and eviction block.
///
/// Foyer creates and holds one file per block at startup, and entries must fit
/// within a block after index overhead. This uses foyer's default size.
pub(crate) const DISK_BLOCK_BYTES: usize = 16 * 1024 * 1024;
/// Flusher tasks writing the disk tier.
const DISK_FLUSHERS: usize = 2;
/// Total flush buffer pool, split evenly among the flushers. Each flusher
/// holds two buffers of its share, so this is also the tier's steady write
/// memory.
const DISK_BUFFER_POOL_BYTES: usize = 64 * 1024 * 1024;
/// Bytes of queued inserts the disk tier accepts before it drops further
/// ones. A drop is counted, never surfaced.
const DISK_SUBMIT_QUEUE_THRESHOLD_BYTES: usize = 256 * 1024 * 1024;

/// Minimum supported disk capacity.
///
/// Capacity is allocated in whole blocks. Six blocks is the smallest layout
/// that satisfies foyer's stability requirement for flushers and clean-block
/// reserves.
pub(crate) const MIN_DISK_BYTES: u64 = 6 * DISK_BLOCK_BYTES as u64;

/// Per-entry overhead the memory weigher charges on top of the value's
/// length: the key's own bytes and foyer's bookkeeping for one entry. It is
/// a fixed, generous estimate rather than a measurement, because its only
/// job is to keep small entries from being treated as free.
const MEMORY_ENTRY_OVERHEAD_BYTES: usize = 256;

/// The block kinds this cache reports separately, in the order its per-kind
/// counters are indexed by.
const BLOCK_KINDS: [StoredMetadataBlockKind; 3] = [
    StoredMetadataBlockKind::Data,
    StoredMetadataBlockKind::Index,
    StoredMetadataBlockKind::Filter,
];

/// One counter per block kind, indexed by [`kind_index`].
type PerKind = [Arc<dyn CounterHandle>; BLOCK_KINDS.len()];

/// Foyer-backed cache with a byte-limited memory tier and local disk tier.
///
/// After [`close`](StoredMetadataBlockCache::close), lookups return `None`,
/// mutations do nothing, and repeated closes succeed.
pub struct FoyerStoredMetadataBlockCache {
    cache: HybridCache<StoredMetadataBlockKey, Bytes>,
    /// The exclusive lock on the configured root, held for the life of this
    /// cache. Closing the file is what releases it, so the field is kept
    /// even though nothing reads it.
    _directory_lock: File,
    /// The two foyer counters this process keeps, filled by foyer itself
    /// through the registry installed at construction.
    overflow: FoyerOverflowCounters,
    /// False while the cache is Open, true once it is Closed. The one
    /// transition is the swap in `close`.
    closed: AtomicBool,
    /// Whether a lookup failure has already been logged at error level. The
    /// first one is worth waking someone for; the rest are the same fact
    /// repeated, so they go to debug.
    get_failure_logged: AtomicBool,
    metrics: LocalCacheMetrics,
}

impl Debug for FoyerStoredMetadataBlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerStoredMetadataBlockCache")
            .field("name", &CACHE_NAME)
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl FoyerStoredMetadataBlockCache {
    /// Opens the cache in the configured directory.
    ///
    /// The root is created and locked exclusively. The versioned cache directory
    /// is reused only when its recorded geometry matches the requested geometry.
    /// If foyer cannot open a compatible directory, startup fails without deleting
    /// it automatically.
    pub(crate) async fn open(
        config: &LocalCacheConfig,
        recorder: &dyn MetricsRecorder,
    ) -> Result<Self, ServerConfigError> {
        let root = PathBuf::from(config.path.trim());
        create_directory(&root)?;
        let directory_lock = lock_directory(&root)?;

        let directory = root.join(CACHE_DIRECTORY);
        let capacity = usize::try_from(config.disk_bytes).map_err(|_| {
            invalid_local_cache(format!(
                "`disk_bytes` of {} does not fit this platform's address space",
                config.disk_bytes
            ))
        })?;
        prepare_directory(&directory, capacity)?;
        let device = FsDeviceBuilder::new(&directory)
            .with_capacity(capacity)
            .build()
            .map_err(|error| open_failure(&directory, capacity, &error.to_string()))?;

        let overflow = FoyerOverflowCounters::default();
        let memory_bytes = usize::try_from(config.memory_bytes).map_err(|_| {
            invalid_local_cache(format!(
                "`memory_bytes` of {} does not fit this platform's address space",
                config.memory_bytes
            ))
        })?;
        let cache = HybridCacheBuilder::<StoredMetadataBlockKey, Bytes>::new()
            .with_name(CACHE_NAME)
            // Persist entries on insertion so blocks still in memory survive a restart.
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_metrics_registry(Box::new(OverflowRegistry::new(overflow.clone())))
            .memory(memory_bytes)
            // The memory budget is bytes, so the weight of an entry is its
            // size. Counting entries instead would let one budget mean
            // wildly different amounts of memory.
            .with_weighter(|_key: &StoredMetadataBlockKey, value: &Bytes| {
                value.len() + MEMORY_ENTRY_OVERHEAD_BYTES
            })
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(device)
                    .with_block_size(DISK_BLOCK_BYTES)
                    .with_flushers(DISK_FLUSHERS)
                    .with_buffer_pool_size(DISK_BUFFER_POOL_BYTES)
                    .with_submit_queue_size_threshold(DISK_SUBMIT_QUEUE_THRESHOLD_BYTES),
            )
            // Stored segment sections are already compressed; do not recompress them.
            .with_compression(Compression::None)
            // Drop unreadable recovered entries. Object storage remains authoritative.
            .with_recover_mode(RecoverMode::Quiet)
            .build()
            .await
            .map_err(|error| open_failure(&directory, capacity, &error.to_string()))?;

        Ok(Self {
            cache,
            _directory_lock: directory_lock,
            overflow,
            closed: AtomicBool::new(false),
            get_failure_logged: AtomicBool::new(false),
            metrics: LocalCacheMetrics::register(recorder),
        })
    }

    /// Whether [`StoredMetadataBlockCache::close`] has run.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// What foyer reports about itself right now.
    ///
    /// These are levels and running totals foyer keeps, read when a scrape
    /// asks rather than accumulated as work happens.
    pub(crate) fn foyer_stats(&self) -> FoyerCacheStats {
        FoyerCacheStats {
            memory_bytes: self.cache.memory().usage(),
            memory_capacity_bytes: self.cache.memory().capacity(),
            disk_bytes: self.cache.storage().device().allocated(),
            disk_capacity_bytes: self.cache.storage().device().capacity(),
            queue_buffer_overflows: self.overflow.buffer.load(Ordering::Relaxed),
            queue_channel_overflows: self.overflow.channel.load(Ordering::Relaxed),
        }
    }

    /// Records one lookup failure: counted always, logged loudly once.
    fn record_get_failure(&self, key: &StoredMetadataBlockKey, error: &dyn std::fmt::Display) {
        self.metrics.get_failures[kind_index(key.kind)].increment(1);
        if self.get_failure_logged.swap(true, Ordering::Relaxed) {
            tracing::debug!(
                cache = CACHE_NAME,
                "local block cache lookup failed: {error}"
            );
        } else {
            tracing::error!(
                cache = CACHE_NAME,
                "local block cache lookup failed, and reads are falling back to object \
                 storage; further failures are logged at debug: {error}"
            );
        }
    }
}

#[async_trait]
impl StoredMetadataBlockCache for FoyerStoredMetadataBlockCache {
    async fn get(&self, key: &StoredMetadataBlockKey) -> Option<Bytes> {
        if self.is_closed() {
            return None;
        }
        match self.cache.get(key).await {
            Ok(Some(entry)) => {
                self.metrics.hits[kind_index(key.kind)].increment(1);
                Some(entry.value().clone())
            }
            Ok(None) => {
                self.metrics.misses[kind_index(key.kind)].increment(1);
                None
            }
            Err(error) => {
                self.record_get_failure(key, &error);
                None
            }
        }
    }

    fn insert(&self, key: StoredMetadataBlockKey, bytes: Bytes) {
        if self.is_closed() {
            return;
        }
        self.metrics.inserts[kind_index(key.kind)].increment(1);
        // foyer takes the entry and decides for itself whether it reaches
        // disk, so there is nothing here to fail. An insert the disk tier
        // could not keep shows up in the two overflow counters instead.
        let _entry = self.cache.insert(key, bytes);
    }

    fn invalidate(&self, key: &StoredMetadataBlockKey) {
        if self.is_closed() {
            return;
        }
        // Invalidation is called only for cached bytes that failed to decode, so
        // record it as a rejection rather than an eviction.
        self.metrics.rejections[kind_index(key.kind)].increment(1);
        self.cache.remove(key);
    }

    async fn close(&self) -> Result<(), StoredMetadataBlockCacheCloseError> {
        // The first caller performs shutdown. Later callers observe the closed state.
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        match self.cache.close().await {
            Ok(()) => {
                self.metrics.closes_clean.increment(1);
                Ok(())
            }
            Err(error) => {
                self.metrics.closes_failed.increment(1);
                Err(StoredMetadataBlockCacheCloseError::new(error.to_string()))
            }
        }
    }
}

/// What foyer reports about the local cache when a scrape asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FoyerCacheStats {
    /// Bytes the memory tier is holding.
    pub(crate) memory_bytes: usize,
    /// Bytes the memory tier may hold.
    pub(crate) memory_capacity_bytes: usize,
    /// Disk capacity claimed as foyer blocks. This is allocated capacity, not
    /// the size of live cache entries.
    pub(crate) disk_bytes: usize,
    /// Bytes of disk the device was opened with.
    pub(crate) disk_capacity_bytes: usize,
    /// Inserts the disk tier dropped because a flusher's buffer was full.
    pub(crate) queue_buffer_overflows: u64,
    /// Inserts the disk tier dropped because its submit queue was over
    /// threshold.
    pub(crate) queue_channel_overflows: u64,
}

/// The disk geometry the versioned directory was built to hold, as its
/// marker records it.
///
/// foyer names one file per disk block from an in-memory counter and never
/// looks at the directory it was pointed at. A start that claims fewer
/// blocks than the last one therefore reopens files `0..blocks` and leaves
/// every higher-numbered file exactly where it is: cached bytes on disk
/// that no metric counts, that nothing reclaims, and that put the tier over
/// the size its configuration asked for. The marker is the only record of
/// how many files could be down there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskGeometry {
    /// Bytes in one disk block, which is [`DISK_BLOCK_BYTES`] for a
    /// directory this build wrote.
    block_bytes: usize,
    /// Blocks the directory was built to hold, which is an upper bound on
    /// the block files it holds.
    blocks: usize,
}

/// Prepares the versioned cache directory for the configured capacity.
///
/// A directory with the same block size is reused when its capacity is no
/// larger than the requested capacity. Other layouts are discarded because
/// Foyer would leave unused block files behind. The cache contains no durable
/// data, so rebuilding it is safe.
///
/// The geometry marker is written before Foyer opens the device. The caller
/// already holds the directory lock.
fn prepare_directory(directory: &Path, capacity: usize) -> Result<(), ServerConfigError> {
    let wanted = DiskGeometry {
        block_bytes: DISK_BLOCK_BYTES,
        blocks: capacity / DISK_BLOCK_BYTES,
    };
    let found = read_geometry(directory)?;
    let keep = found.is_some_and(|found| {
        found.block_bytes == wanted.block_bytes && found.blocks <= wanted.blocks
    });

    if !keep && directory.exists() {
        let reason = match found {
            None => "the geometry marker is missing or invalid".to_owned(),
            Some(found) if found.block_bytes != wanted.block_bytes => format!(
                "the stored block size is {} bytes but the configured size is {}",
                found.block_bytes, wanted.block_bytes
            ),
            Some(found) => format!(
                "the stored block count is {} but the configured capacity allows {}",
                found.blocks, wanted.blocks
            ),
        };
        tracing::info!(
            cache = CACHE_NAME,
            "discarding the local cache directory `{}` because {reason}; keeping it would leave \
             unused block files",
            display_path(directory)
        );
        std::fs::remove_dir_all(directory).map_err(|error| {
            invalid_local_cache(format!(
                "failed to remove `{}`: {error}",
                display_path(directory)
            ))
        })?;
    }

    create_directory(directory)?;
    // Update the marker after replacing the directory or increasing capacity.
    if found != Some(wanted) {
        write_geometry(directory, wanted)?;
    }
    Ok(())
}

/// The geometry the versioned directory records, or `None` when it records
/// none this process can make sense of.
///
/// A missing marker and an unparsable one both mean the directory says
/// nothing about itself, and a directory that says nothing about itself is
/// discarded. There is no repair path, because the marker is smaller than
/// one write.
///
/// A marker that exists and cannot be read is neither of those. Permission
/// and I/O errors fail startup for the same reason the write side does: the
/// recovery is to delete a directory whose geometry is on the far side of
/// the error, which would report "it records no geometry of its own" about
/// a marker that may record exactly the right one.
fn read_geometry(directory: &Path) -> Result<Option<DiskGeometry>, ServerConfigError> {
    let path = directory.join(GEOMETRY_MARKER);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(invalid_local_cache(format!(
                "failed to read `{}`: {error}",
                display_path(&path)
            )))
        }
    };
    Ok(serde_json::from_slice(&bytes).ok())
}

/// Records the geometry this start claims.
///
/// A marker that cannot be written fails startup. The directory is about to
/// be filled with block files, so a process that cannot write a few bytes
/// beside them has a problem the next start would inherit as orphaned
/// blocks.
fn write_geometry(directory: &Path, geometry: DiskGeometry) -> Result<(), ServerConfigError> {
    let path = directory.join(GEOMETRY_MARKER);
    let bytes = serde_json::to_vec(&geometry).map_err(|error| {
        invalid_local_cache(format!("failed to encode `{geometry:?}`: {error}"))
    })?;
    std::fs::write(&path, bytes).map_err(|error| {
        invalid_local_cache(format!(
            "failed to write `{}`: {error}",
            display_path(&path)
        ))
    })
}

/// Takes the exclusive lock that says this process owns the configured root.
///
/// The lock is advisory and process-wide, which is exactly the claim being
/// made: one server owns one cache directory. The returned file holds the
/// lock, so it lives as long as the cache does.
fn lock_directory(root: &Path) -> Result<File, ServerConfigError> {
    let lock_path = root.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| {
            invalid_local_cache(format!(
                "failed to open the lock file `{}`: {error}",
                display_path(&lock_path)
            ))
        })?;
    fs4::fs_std::FileExt::try_lock_exclusive(&file)
        .map_err(|error| {
            invalid_local_cache(format!(
                "failed to lock `{}`: {error}",
                display_path(&lock_path)
            ))
        })?
        .then_some(file)
        .ok_or_else(|| {
            invalid_local_cache(format!(
                "`{}` is locked by another process; a local cache directory belongs to \
                 one server, so give this one a directory of its own",
                display_path(root)
            ))
        })
}

/// The error a directory foyer could not open produces.
///
/// It names the two things an operator can act on: that the directory holds
/// nothing durable and may be deleted, and how much of the filesystem — and
/// how many open files — the configured capacity asks for.
fn open_failure(directory: &Path, capacity: usize, reason: &str) -> ServerConfigError {
    let blocks = capacity / DISK_BLOCK_BYTES;
    invalid_local_cache(format!(
        "failed to open the local cache directory `{}`: {reason}. The directory holds no \
         durable data and may be deleted while this server is stopped; it is rebuilt on the \
         next start. `disk_bytes` of {capacity} claims {blocks} files of {DISK_BLOCK_BYTES} \
         bytes, each held open, so it also needs an open-file limit above that.",
        display_path(directory)
    ))
}

fn invalid_local_cache(reason: String) -> ServerConfigError {
    ServerConfigError::InvalidField {
        field: "local_cache.path",
        reason,
    }
}

fn create_directory(path: &Path) -> Result<(), ServerConfigError> {
    std::fs::create_dir_all(path).map_err(|error| {
        invalid_local_cache(format!(
            "failed to create `{}`: {error}",
            display_path(path)
        ))
    })
}

/// A path as a string, for an error message. A path that is not UTF-8 still
/// has to be reportable.
fn display_path(path: &Path) -> String {
    path.display().to_string()
}

/// The `kind` label one block kind carries.
fn kind_label(kind: StoredMetadataBlockKind) -> &'static str {
    match kind {
        StoredMetadataBlockKind::Data => "data",
        StoredMetadataBlockKind::Index => "index",
        StoredMetadataBlockKind::Filter => "filter",
    }
}

/// Where one block kind's counter sits in a [`PerKind`] array.
fn kind_index(kind: StoredMetadataBlockKind) -> usize {
    match kind {
        StoredMetadataBlockKind::Data => 0,
        StoredMetadataBlockKind::Index => 1,
        StoredMetadataBlockKind::Filter => 2,
    }
}

/// The counters this cache reports, every one registered at construction.
///
/// Registering the whole closed set up front is what keeps the label space
/// closed: the only labels are the three block kinds and the two close
/// outcomes, all of them `&'static str`, and no call path can add another.
struct LocalCacheMetrics {
    hits: PerKind,
    misses: PerKind,
    get_failures: PerKind,
    inserts: PerKind,
    rejections: PerKind,
    closes_clean: Arc<dyn CounterHandle>,
    closes_failed: Arc<dyn CounterHandle>,
}

impl LocalCacheMetrics {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let per_kind = |name: &'static str, description: &'static str, result: &'static str| {
            BLOCK_KINDS.map(|kind| {
                recorder.register_counter(
                    name,
                    description,
                    &[("kind", kind_label(kind)), ("result", result)],
                )
            })
        };
        let closes = |result: &'static str| {
            recorder.register_counter(
                "loonfs.local_cache.closes",
                "Local block cache shutdowns, by outcome",
                &[("result", result)],
            )
        };
        Self {
            hits: per_kind(
                "loonfs.local_cache.gets",
                "Local block cache lookups, by block kind and outcome",
                "hit",
            ),
            misses: per_kind(
                "loonfs.local_cache.gets",
                "Local block cache lookups, by block kind and outcome",
                "miss",
            ),
            get_failures: per_kind(
                "loonfs.local_cache.gets",
                "Local block cache lookups, by block kind and outcome",
                "failed",
            ),
            inserts: BLOCK_KINDS.map(|kind| {
                recorder.register_counter(
                    "loonfs.local_cache.inserts",
                    "Blocks offered to the local block cache, by block kind",
                    &[("kind", kind_label(kind))],
                )
            }),
            rejections: BLOCK_KINDS.map(|kind| {
                recorder.register_counter(
                    "loonfs.local_cache.rejections",
                    "Local block cache entries dropped because their bytes did not decode, \
                     by block kind",
                    &[("kind", kind_label(kind))],
                )
            }),
            closes_clean: closes("clean"),
            closes_failed: closes("failed"),
        }
    }
}

/// The two foyer counters this process keeps.
///
/// foyer increments them when the disk tier refuses an insert, which is the
/// only signal that the tier is behind. It reports them through a metrics
/// registry and offers no other way to read them, so the registry below
/// exists to hold these two numbers.
#[derive(Debug, Clone, Default)]
struct FoyerOverflowCounters {
    buffer: Arc<AtomicU64>,
    channel: Arc<AtomicU64>,
}

/// The counter vector foyer registers its inner-operation counts under.
const FOYER_INNER_OP_COUNTER_VEC: &str = "foyer_storage_inner_op_total";
/// The label value on the counter foyer bumps when a flusher's buffer is
/// full.
const FOYER_BUFFER_OVERFLOW_LABEL: &str = "buffer_overflow";
/// The label value on the counter foyer bumps when the submit queue is over
/// threshold.
const FOYER_CHANNEL_OVERFLOW_LABEL: &str = "channel_overflow";

/// Metrics registry that retains foyer's two overflow counters and discards
/// all other instruments.
///
/// Foyer registers handles once and updates them later, so this registry keeps
/// only the handles the server exposes.
#[derive(Debug)]
struct OverflowRegistry {
    counters: FoyerOverflowCounters,
}

impl OverflowRegistry {
    fn new(counters: FoyerOverflowCounters) -> Self {
        Self { counters }
    }
}

impl RegistryOps for OverflowRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        if name == FOYER_INNER_OP_COUNTER_VEC {
            Box::new(OverflowCounterVec {
                counters: self.counters.clone(),
            })
        } else {
            Box::new(DiscardVec)
        }
    }

    fn register_gauge_vec(
        &self,
        _name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        Box::new(DiscardVec)
    }

    fn register_histogram_vec(
        &self,
        _name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        Box::new(DiscardVec)
    }

    fn register_histogram_vec_with_buckets(
        &self,
        _name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
        _buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
        Box::new(DiscardVec)
    }
}

/// foyer's inner-operation counters, of which two are kept.
///
/// The vector is labelled by cache name and operation, and the operation is
/// the last label. An operation this process does not keep gets a discarding
/// counter, so a foyer release that adds one costs nothing here.
#[derive(Debug)]
struct OverflowCounterVec {
    counters: FoyerOverflowCounters,
}

impl CounterVecOps for OverflowCounterVec {
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        match labels.last().map(Cow::as_ref) {
            Some(FOYER_BUFFER_OVERFLOW_LABEL) => {
                Box::new(SharedCounter(Arc::clone(&self.counters.buffer)))
            }
            Some(FOYER_CHANNEL_OVERFLOW_LABEL) => {
                Box::new(SharedCounter(Arc::clone(&self.counters.channel)))
            }
            _ => Box::new(Discard),
        }
    }
}

/// A counter foyer increments and this process reads.
#[derive(Debug)]
struct SharedCounter(Arc<AtomicU64>);

impl CounterOps for SharedCounter {
    fn increase(&self, val: u64) {
        self.0.fetch_add(val, Ordering::Relaxed);
    }
}

/// An instrument vector nothing reads, and so nothing keeps.
#[derive(Debug)]
struct DiscardVec;

impl CounterVecOps for DiscardVec {
    fn counter(&self, _labels: &[Cow<'static, str>]) -> BoxedCounter {
        Box::new(Discard)
    }
}

impl GaugeVecOps for DiscardVec {
    fn gauge(&self, _labels: &[Cow<'static, str>]) -> BoxedGauge {
        Box::new(Discard)
    }
}

impl HistogramVecOps for DiscardVec {
    fn histogram(&self, _labels: &[Cow<'static, str>]) -> BoxedHistogram {
        Box::new(Discard)
    }
}

/// An instrument that drops what it is given.
#[derive(Debug)]
struct Discard;

impl CounterOps for Discard {
    fn increase(&self, _val: u64) {}
}

impl GaugeOps for Discard {
    fn increase(&self, _val: u64) {}
    fn decrease(&self, _val: u64) {}
    fn absolute(&self, _val: u64) {}
}

impl HistogramOps for Discard {
    fn record(&self, _val: f64) {}
}

#[cfg(test)]
mod tests;
