//! The builder for a [`LoonFs`] runtime in either mode.

use super::{LoonFs, ReadOnly, Writable};
use crate::config::ReadConfig;
use crate::fs::{RuntimeCore, WriterBits, WriterIdentity};
use crate::metrics::{
    fan_out_object_store_recorder, MetricsRecorder, ObjectStoreMetricsRecorder, RuntimeInstruments,
};
use crate::publisher::{NamespaceAdvanceHint, NamespaceAdvanceObserver, PublisherRegistry};
use crate::{
    InlineContentOptions, MaintenanceHint, MaintenanceHintObserver, PublicationLimits, Result,
    RuntimeCacheConfig, RuntimeError, SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
};
use loonfs_core::cache::StoredMetadataBlockCache;
use loonfs_core::MetadataLsmPolicy;
use loonfs_objectstore::metrics::InstrumentedObjectStore;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Builder for a [`LoonFs`] runtime in mode `M`.
///
/// Settings that every runtime reads are available in both modes. Settings
/// for the writer identity, publication, and maintenance exist only on the
/// writable builder.
#[must_use]
pub struct LoonFsBuilder<M> {
    core: CoreSettings,
    writer: WriterSettings,
    mode: PhantomData<M>,
}

/// Where a runtime's object-store client comes from.
enum StoreSource {
    /// Built from configuration inside the runtime's ownership domain.
    Config(StoreConfig),
    /// Supplied by the caller, who owns the sharing decision.
    Shared(SharedObjectStore),
}

/// What the runtime core opens from, in every mode.
struct CoreSettings {
    source: StoreSource,
    max_read_content_bytes: Option<u64>,
    runtime_cache: RuntimeCacheConfig,
    /// Carries the merge input budget; the block memo budget comes from
    /// `runtime_cache` when the core opens.
    metadata_lsm_policy: MetadataLsmPolicy,
    timer: Arc<dyn loonfs_api::MonotonicTimer>,
    wall_clock: Arc<dyn crate::WallClock>,
    stored_metadata_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
    trace_mode: TraceMode,
    trace_store_kind: Option<TraceStoreKind>,
    object_store_metrics_recorder: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

/// What only a writable runtime reads. A read-only build ignores it.
struct WriterSettings {
    writer_id: Option<String>,
    min_publish_interval_ms: u64,
    publication_limits: PublicationLimits,
    inline_content: InlineContentOptions,
    max_concurrent_folds: NonZeroUsize,
    namespace_advance_observer: Option<NamespaceAdvanceObserver>,
    maintenance_hint_observer: Option<MaintenanceHintObserver>,
}

impl<M> LoonFsBuilder<M> {
    pub(super) fn from_config(store_config: StoreConfig) -> Self {
        Self::new(StoreSource::Config(store_config))
    }

    pub(super) fn from_store(store: SharedObjectStore) -> Self {
        Self::new(StoreSource::Shared(store))
    }

    fn new(source: StoreSource) -> Self {
        Self {
            core: CoreSettings {
                source,
                max_read_content_bytes: None,
                runtime_cache: RuntimeCacheConfig::default(),
                metadata_lsm_policy: MetadataLsmPolicy::default(),
                timer: Arc::new(loonfs_api::StdMonotonicTimer::default()),
                wall_clock: Arc::new(loonfs_core::time::SystemWallClock),
                stored_metadata_block_cache: None,
                trace_mode: TraceMode::Embedded,
                trace_store_kind: None,
                object_store_metrics_recorder: None,
                metrics_recorder: None,
            },
            writer: WriterSettings {
                writer_id: None,
                min_publish_interval_ms: crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS,
                publication_limits: PublicationLimits::default(),
                inline_content: InlineContentOptions::default(),
                max_concurrent_folds: NonZeroUsize::new(
                    crate::config::DEFAULT_MAX_CONCURRENT_FOLDS,
                )
                .expect("default maximum concurrent folds should be nonzero"),
                namespace_advance_observer: None,
                maintenance_hint_observer: None,
            },
            mode: PhantomData,
        }
    }

    /// Supplies monotonic time for runtime scheduling and deterministic tests.
    pub fn monotonic_timer(mut self, timer: Arc<dyn loonfs_api::MonotonicTimer>) -> Self {
        self.core.timer = timer;
        self
    }

    /// Supplies wall time for durable timestamps and expiration decisions.
    /// The read-only view and the maintenance of this runtime read the same
    /// clock.
    pub fn wall_clock(mut self, clock: Arc<dyn crate::WallClock>) -> Self {
        self.core.wall_clock = clock;
        self
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.core.runtime_cache = runtime_cache;
        self
    }

    /// Caps the file content size the buffered read APIs will materialize
    /// for one call, checked against resolved metadata before any content
    /// fetch; over-limit reads fail with `content_too_large`. Unset by
    /// default: embedded callers read files of any size. Servers set this
    /// so one proxied read cannot buffer arbitrarily large content.
    pub fn max_read_content_bytes(mut self, max_read_content_bytes: u64) -> Self {
        self.core.max_read_content_bytes = Some(max_read_content_bytes);
        self
    }

    /// Installs a node-local encoded-block cache beneath the decoded cache.
    ///
    /// The read-only view of this runtime shares the decoded cache, and so
    /// this local cache too. The host owns and closes it; object storage
    /// remains authoritative.
    pub fn stored_metadata_block_cache(
        mut self,
        stored_metadata_block_cache: Arc<dyn StoredMetadataBlockCache>,
    ) -> Self {
        self.core.stored_metadata_block_cache = Some(stored_metadata_block_cache);
        self
    }

    /// Sets the tracing mode label.
    pub fn trace_mode(mut self, trace_mode: TraceMode) -> Self {
        self.core.trace_mode = trace_mode;
        self
    }

    /// Sets the object-store kind label used by tracing and metrics.
    ///
    /// Config-built stores derive this automatically; setting it overrides
    /// the derived label.
    pub fn trace_store_kind(mut self, trace_store_kind: TraceStoreKind) -> Self {
        self.core.trace_store_kind = Some(trace_store_kind);
        self
    }

    /// Installs raw object-store sample collection for this runtime.
    ///
    /// The runtime wraps its object store before opening; callers do not
    /// need to construct an instrumented store manually. Combines with
    /// [`Self::metrics_recorder`]: one wrapper feeds both.
    pub fn object_store_metrics_recorder(
        mut self,
        recorder: Arc<dyn ObjectStoreMetricsRecorder>,
    ) -> Self {
        self.core.object_store_metrics_recorder = Some(recorder);
        self
    }

    /// Installs the metrics recorder this runtime reports its instruments to
    /// (see [`crate::metrics`]).
    ///
    /// The runtime registers its instrument set once, here, and reports into
    /// it from then on. Every mode reports object-store calls and caches. A
    /// writable runtime and its maintenance also report publications,
    /// compactions, and collection passes. A runtime built without one
    /// registers nothing.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }
}

impl LoonFsBuilder<ReadOnly> {
    /// Opens the read-only runtime inside the Tokio runtime that will drive
    /// its reads.
    ///
    /// Async for uniformity with the writable build; a read-only runtime owns
    /// no background work, so it needs no ambient Tokio runtime of its own.
    pub async fn build(self) -> Result<LoonFs<ReadOnly>> {
        Ok(LoonFs {
            core: self.core.open()?,
            mode: ReadOnly,
        })
    }
}

impl LoonFsBuilder<Writable> {
    /// Sets the writer id used by namespace mutations. Required.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer.writer_id = Some(writer_id.into());
        self
    }

    /// Sets the minimum interval between publication starts per namespace,
    /// in milliseconds (see [`crate::publisher`]).
    ///
    /// A request to an idle namespace publishes immediately; the interval
    /// only paces requests that queued behind a publish, so concurrent
    /// publishes amortize into fewer, larger WAL objects — with each caller still awaiting its own
    /// durable, visible result. Defaults to 15 ms; zero keeps only the
    /// batching that in-flight publications force.
    pub fn min_publish_interval_ms(mut self, min_publish_interval_ms: u64) -> Self {
        self.writer.min_publish_interval_ms = min_publish_interval_ms;
        self
    }

    /// Sets the maximum number of WAL tails this runtime folds concurrently.
    /// The default is [`crate::DEFAULT_MAX_CONCURRENT_FOLDS`].
    pub fn max_concurrent_folds(mut self, limit: NonZeroUsize) -> Self {
        self.writer.max_concurrent_folds = limit;
        self
    }

    /// Sets the decoded metadata bytes one maintenance step may merge. A step
    /// merges inline only the runs that fit; a larger window runs as a
    /// streaming compaction that holds at most this much at once. Applies to
    /// this runtime's [`LoonFs::maintenance`]. Defaults to 64 MiB.
    pub fn max_merge_input_bytes(mut self, max_merge_input_bytes: NonZeroUsize) -> Self {
        self.core
            .metadata_lsm_policy
            .max_decoded_input_bytes_per_step = max_merge_input_bytes;
        self
    }

    /// Sets the shared admission and concurrency limits for publications.
    pub fn publication_limits(mut self, limits: PublicationLimits) -> Self {
        self.writer.publication_limits = limits;
        self
    }

    /// Sets inline preparation, segment, fold, and tail limits.
    pub fn inline_content(mut self, options: InlineContentOptions) -> Self {
        self.writer.inline_content = options;
        self
    }

    /// Registers an observer called with a [`NamespaceAdvanceHint`] after
    /// each publication batch that durably commits at least one mutation.
    ///
    /// The call happens after durable visibility. One batch may cover
    /// several commits, so the hint carries a high-water mark rather than
    /// one commit, and delivery is best-effort. The observer runs
    /// synchronously on the publication task, so it must do nothing but a
    /// non-blocking handoff such as a bounded-channel `try_send`: no
    /// network, filesystem, object-store, lock-contended, or waiting work.
    /// Downstream correctness comes from a durable change-feed cursor,
    /// never from the hints. Runtimes that register no observer publish
    /// exactly as before.
    pub fn namespace_advance_observer(
        mut self,
        observer: impl Fn(NamespaceAdvanceHint) + Send + Sync + 'static,
    ) -> Self {
        self.writer.namespace_advance_observer = Some(Arc::new(observer));
        self
    }

    /// Registers a non-blocking observer for best-effort maintenance hints.
    /// It runs synchronously on publication and upload tasks and must only
    /// perform a non-blocking handoff.
    pub fn maintenance_hint_observer(
        mut self,
        observer: impl Fn(MaintenanceHint) + Send + Sync + 'static,
    ) -> Self {
        self.writer.maintenance_hint_observer = Some(Arc::new(observer));
        self
    }

    /// Opens the writable runtime inside the Tokio runtime that owns
    /// publication tasks.
    ///
    /// Construction runs one way only, so nothing here is cyclic: the read
    /// core opens first, the writer's bits are built on top of it, and the
    /// publication service is created last, holding the core strongly and
    /// the bits weakly.
    pub async fn build(self) -> Result<LoonFs<Writable>> {
        let writer = self.writer;
        writer.inline_content.validate()?;
        let writer_id = writer
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        if writer.publication_limits.max_concurrent_publications.get() > Semaphore::MAX_PERMITS {
            return Err(RuntimeError::Config(format!(
                "max_concurrent_publications must not exceed {}",
                Semaphore::MAX_PERMITS
            )));
        }
        let identity = WriterIdentity::new(writer_id)?;
        let runtime = owning_runtime()?;
        let core = self.core.open()?;
        let bits = Arc::new(WriterBits {
            inline_content: writer.inline_content,
            identity,
            wal_fold_permits: Semaphore::new(writer.max_concurrent_folds.get()),
            wal_folds_waiting: AtomicUsize::new(0),
            namespace_advance_observer: writer.namespace_advance_observer,
            maintenance_hint_observer: writer.maintenance_hint_observer,
        });
        let publisher = PublisherRegistry::new(
            core.clone(),
            Arc::downgrade(&bits),
            runtime,
            std::time::Duration::from_millis(writer.min_publish_interval_ms),
            writer.publication_limits,
        );
        Ok(LoonFs {
            core,
            mode: Writable { bits, publisher },
        })
    }
}

impl CoreSettings {
    /// Resolves the object-store client, wraps it for metrics when the
    /// builder was given a recorder, and opens the runtime core.
    ///
    /// A builder given no recorder of either kind wraps nothing: the store
    /// the core holds is the store it was handed, and the instrument set it
    /// carries reports nowhere.
    fn open(self) -> Result<RuntimeCore> {
        let (store, derived_kind) = match self.source {
            StoreSource::Config(config) => {
                let kind = TraceStoreKind::from(config.kind());
                let store = config
                    .configured_object_store()
                    .map_err(|error| RuntimeError::Config(error.public_message().into_owned()))?;
                (store.into_shared(), kind)
            }
            StoreSource::Shared(store) => (store, TraceStoreKind::Unknown),
        };
        let trace_store_kind = self.trace_store_kind.unwrap_or(derived_kind);
        let instruments = RuntimeInstruments::new(self.metrics_recorder);
        let recorder = fan_out_object_store_recorder(
            self.object_store_metrics_recorder,
            instruments.object_store_recorder(),
        );
        let store = match recorder {
            Some(recorder) => Arc::new(
                InstrumentedObjectStore::new(store, recorder).store_kind(trace_store_kind.as_str()),
            ) as SharedObjectStore,
            None => store,
        };
        Ok(RuntimeCore::open(
            store,
            ReadConfig {
                max_read_content_bytes: self.max_read_content_bytes,
                metadata_lsm_policy: MetadataLsmPolicy {
                    max_block_memo_bytes: self
                        .runtime_cache
                        .metadata_segment_cache
                        .max_block_memo_bytes,
                    ..self.metadata_lsm_policy
                },
                runtime_cache: self.runtime_cache,
                trace_mode: self.trace_mode,
                trace_store_kind,
            },
            None,
            self.stored_metadata_block_cache,
            instruments,
            self.timer,
            self.wall_clock,
        ))
    }
}

/// Resolves the Tokio runtime that will own a writable runtime's background
/// tasks.
///
/// A runtime is built inside the Tokio runtime that owns it, so the current
/// one is the owner. Building outside a Tokio runtime is a configuration
/// error, not a panic.
fn owning_runtime() -> Result<tokio::runtime::Handle> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        RuntimeError::Config(
            "a writable runtime must be built inside the Tokio runtime that will own it".to_owned(),
        )
    })
}
