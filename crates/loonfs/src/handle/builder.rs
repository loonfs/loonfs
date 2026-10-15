//! The builder for a [`LoonFs`] runtime in either mode.

use super::{LoonFs, ReadOnly, Writable};
use crate::config::ReadConfig;
use crate::fs::{RuntimeCore, WriterBits, WriterIdentity};
use crate::metrics::{
    fan_out_object_store_recorder, MetricsRecorder, ObjectStoreMetricsRecorder, RuntimeInstruments,
};
use crate::publisher::PublisherRegistry;
use crate::{
    Error, ExecutionBudget, InlineContentPolicy, MetadataCache, PublicationPolicy, Result,
    SharedObjectStore, StoreConfig, TraceMode,
};
use loonfs_core::cache::StoredMetadataBlockCache;
use loonfs_core::limits::READ_REVALIDATION_BOUND_MS;
use loonfs_core::MetadataRunPolicy;
use loonfs_objectstore::metrics::InstrumentedObjectStore;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::WriterId;
use std::marker::PhantomData;
use std::sync::Arc;

/// Builder for a [`LoonFs`] runtime in mode `M`.
///
/// Settings that every runtime reads are available in both modes. Settings
/// for the writer identity, publication, and maintenance exist only on the
/// writable builder. [`Self::read_only`] turns a writable builder into a
/// read-only one.
#[must_use]
pub struct LoonFsBuilder<M> {
    core: CoreConfig,
    writer: WriterConfig,
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
struct CoreConfig {
    source: StoreSource,
    max_read_content_bytes: u64,
    /// `None` builds a private cache when the core opens.
    metadata_cache: Option<MetadataCache>,
    manifest_revalidation_interval_ms: u64,
    bounded_read_interval_ms: u64,
    execution_budget: Option<ExecutionBudget>,
    timer: Arc<dyn loonfs_types::MonotonicTimer>,
    wall_clock: Arc<dyn crate::WallClock>,
    stored_metadata_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
    trace_mode: TraceMode,
    store_kind: Option<ConfiguredObjectStoreKind>,
    object_store_metrics_recorder: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

/// What only a writable runtime reads. A read-only build ignores it.
struct WriterConfig {
    writer_id: Option<WriterId>,
    min_publish_interval_ms: u64,
    publication_policy: PublicationPolicy,
    inline_content: InlineContentPolicy,
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
            core: CoreConfig {
                source,
                max_read_content_bytes: crate::config::DEFAULT_MAX_READ_CONTENT_BYTES,
                metadata_cache: None,
                manifest_revalidation_interval_ms:
                    crate::config::DEFAULT_MANIFEST_REVALIDATION_INTERVAL_MS,
                bounded_read_interval_ms: crate::config::DEFAULT_BOUNDED_READ_INTERVAL_MS,
                execution_budget: None,
                timer: Arc::new(loonfs_types::StdMonotonicTimer::default()),
                wall_clock: Arc::new(loonfs_core::time::SystemWallClock),
                stored_metadata_block_cache: None,
                trace_mode: TraceMode::Embedded,
                store_kind: None,
                object_store_metrics_recorder: None,
                metrics_recorder: None,
            },
            writer: WriterConfig {
                writer_id: None,
                min_publish_interval_ms: crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS,
                publication_policy: PublicationPolicy::default(),
                inline_content: InlineContentPolicy::default(),
            },
            mode: PhantomData,
        }
    }

    /// Supplies monotonic time for runtime scheduling and deterministic tests.
    pub fn monotonic_timer(mut self, timer: Arc<dyn loonfs_types::MonotonicTimer>) -> Self {
        self.core.timer = timer;
        self
    }

    /// Supplies wall time for durable timestamps and expiration decisions.
    ///
    /// The read-only view and the maintenance of this runtime read the same
    /// clock.
    pub fn wall_clock(mut self, clock: Arc<dyn crate::WallClock>) -> Self {
        self.core.wall_clock = clock;
        self
    }

    /// Reads through `metadata_cache`, which other runtimes may share.
    ///
    /// Runtimes built over clones of one [`SharedObjectStore`] share the
    /// decoded segment blocks and manifests in the cache, and runtimes over
    /// different object stores never read each other's blocks. This runtime
    /// keeps its own head anchors and WAL-tail projections in the cache, and
    /// its clones, its read-only view, and its maintenance share them. The
    /// cache reports its metrics to its own recorder. Without this setting,
    /// the runtime creates a private cache with the default limits that
    /// reports to [`Self::metrics_recorder`].
    pub fn metadata_cache(mut self, metadata_cache: MetadataCache) -> Self {
        self.core.metadata_cache = Some(metadata_cache);
        self
    }

    /// Sets the minimum interval, in milliseconds, between checks for a
    /// successor to a cached manifest.
    ///
    /// Defaults to 1000, and zero checks on every read. A value above
    /// [`READ_REVALIDATION_BOUND_MS`](crate::engine::READ_REVALIDATION_BOUND_MS)
    /// fails the build with [`Error::Config`].
    pub fn manifest_revalidation_interval_ms(
        mut self,
        manifest_revalidation_interval_ms: u64,
    ) -> Self {
        self.core.manifest_revalidation_interval_ms = manifest_revalidation_interval_ms;
        self
    }

    /// Sets the interval, in milliseconds, of a bounded read that doesn't set
    /// its own (see [`Freshness::Bounded`](crate::Freshness::Bounded)).
    ///
    /// In a runtime without the namespace's writer session, a bounded read
    /// reads at the head that the runtime confirmed last, without a request,
    /// while that confirmation is younger than the interval. Defaults to
    /// 1000, and zero confirms the head on every read. A value above
    /// [`READ_REVALIDATION_BOUND_MS`](crate::engine::READ_REVALIDATION_BOUND_MS)
    /// fails the build with [`Error::Config`].
    pub fn bounded_read_interval_ms(mut self, bounded_read_interval_ms: u64) -> Self {
        self.core.bounded_read_interval_ms = bounded_read_interval_ms;
        self
    }

    /// Runs this runtime's reads, publications, folds, and merges under
    /// `execution_budget`, which other runtimes may share.
    ///
    /// Read memos share its read working memory pool.
    /// Every publication request this runtime admits counts against the
    /// budget's admitted totals until its work settles. Public reads wait for
    /// a read permit before loading anything. Publication batches,
    /// session folds and merges, [`LoonFs::maintenance`] work, namespace
    /// deletions, and the creation of checkpoints, snapshots, and forks of
    /// the current head take their permits from it, and every merge holds at
    /// most its merge input size. The per-namespace admission limits stay
    /// with this runtime. The budget
    /// reports its metrics to its own recorder. Without this setting, the
    /// runtime creates a private budget with the default limits that reports
    /// to [`Self::metrics_recorder`].
    pub fn execution_budget(mut self, execution_budget: ExecutionBudget) -> Self {
        self.core.execution_budget = Some(execution_budget);
        self
    }

    /// Caps the file content size the buffered read APIs will materialize for
    /// one call, checked against resolved metadata before any content fetch;
    /// over-limit reads fail with `content_too_large`.
    ///
    /// Defaults to
    /// [`DEFAULT_MAX_READ_CONTENT_BYTES`](crate::DEFAULT_MAX_READ_CONTENT_BYTES),
    /// 256 MiB. A caller that reads larger files through these APIs raises it,
    /// and a server sets it to its download limit.
    pub fn max_read_content_bytes(mut self, max_read_content_bytes: u64) -> Self {
        self.core.max_read_content_bytes = max_read_content_bytes;
        self
    }

    /// Installs a node-local encoded-block cache beneath the metadata cache.
    ///
    /// Its keys carry no runtime scope, so one tier serves the runtimes over
    /// one store, one after another across restarts. It must not be given to
    /// runtimes over different stores. The read-only view of this runtime
    /// uses it too. The host owns and closes it; object storage remains
    /// authoritative.
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
    pub fn store_kind(mut self, store_kind: ConfiguredObjectStoreKind) -> Self {
        self.core.store_kind = Some(store_kind);
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
    /// it from then on. Every mode reports object-store calls and metadata
    /// view reads. A writable runtime and its maintenance also report
    /// publications, compactions, and collection passes. A runtime built
    /// without one registers nothing. The private metadata cache a runtime
    /// creates without [`Self::metadata_cache`] reports here too; a cache
    /// given to that setting reports only to its own recorder. The same holds
    /// for the execution budget (see
    /// [`LoonFsBuilder::execution_budget`]).
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
    /// Sets the writer id used by namespace mutations.
    ///
    /// Required.
    pub fn writer_id(mut self, writer_id: impl Into<WriterId>) -> Self {
        self.writer.writer_id = Some(writer_id.into());
        self
    }

    /// Sets the minimum interval between publication starts per namespace,
    /// in milliseconds.
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

    /// Sets the admission limits this runtime applies to each namespace's
    /// publication requests.
    ///
    /// The totals across namespaces, and the publications running at once, are
    /// limits of the execution budget (see [`Self::execution_budget`]).
    pub fn publication_policy(mut self, policy: PublicationPolicy) -> Self {
        self.writer.publication_policy = policy;
        self
    }

    /// Sets inline preparation, segment, fold, and tail limits.
    pub fn inline_content(mut self, policy: InlineContentPolicy) -> Self {
        self.writer.inline_content = policy;
        self
    }

    /// Turns this builder into one for a read-only runtime.
    ///
    /// It keeps the settings both modes share, such as the store, the
    /// metadata cache, execution budget, and manifest revalidation interval.
    /// It drops writer-only settings such as the writer id and publication limits.
    pub fn read_only(self) -> LoonFsBuilder<ReadOnly> {
        LoonFsBuilder {
            core: self.core,
            writer: self.writer,
            mode: PhantomData,
        }
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
            .ok_or_else(|| Error::Config("`writer_id` is required".to_owned()))?;
        let identity = WriterIdentity { writer_id };
        let runtime = owning_runtime()?;
        let core = self.core.open()?;
        let execution_budget = core.inner.config.execution_budget.clone();
        let bits = Arc::new(WriterBits {
            inline_content: writer.inline_content,
            identity,
            execution_budget: execution_budget.clone(),
            compactor_epochs: tokio::sync::Mutex::default(),
        });
        let publisher = PublisherRegistry::new(
            core.clone(),
            Arc::downgrade(&bits),
            runtime,
            std::time::Duration::from_millis(writer.min_publish_interval_ms),
            writer.publication_policy,
            execution_budget,
        );
        core.set_head_positions(publisher.head_positions());
        Ok(LoonFs {
            core,
            mode: Writable { bits, publisher },
        })
    }
}

impl CoreConfig {
    /// Resolves the object-store client, wraps it for metrics when the
    /// builder was given a recorder, and opens the runtime core.
    ///
    /// A builder given no recorder of either kind wraps nothing: the store
    /// the core holds is the store it was handed, and the instrument set it
    /// carries reports nowhere.
    fn open(self) -> Result<RuntimeCore> {
        if !loonfs_core::time::system_clock_reads_after_epoch() {
            return Err(Error::Config(
                "the system clock reads before the unix epoch; LoonFS stamps unix milliseconds \
                 into durable state"
                    .to_owned(),
            ));
        }
        // A reader reloads a view whose manifest check is as old as the
        // revalidation bound, whatever the interval, so a longer interval
        // would promise something that the runtime doesn't do.
        for (setting, interval_ms) in [
            (
                "manifest_revalidation_interval_ms",
                self.manifest_revalidation_interval_ms,
            ),
            ("bounded_read_interval_ms", self.bounded_read_interval_ms),
        ] {
            if interval_ms > READ_REVALIDATION_BOUND_MS {
                return Err(Error::Config(format!(
                    "`{setting}` is {interval_ms} ms, which exceeds the revalidation bound of \
                     {READ_REVALIDATION_BOUND_MS} ms"
                )));
            }
        }
        let (store, derived_kind) = match self.source {
            StoreSource::Config(config) => {
                let kind = config.kind();
                let store = config
                    .configured_object_store()
                    .map_err(|error| Error::Config(error.public_message().into_owned()))?;
                (store.into_shared(), Some(kind))
            }
            StoreSource::Shared(store) => (store, None),
        };
        let store_kind = self.store_kind.or(derived_kind);
        let metadata_cache = self.metadata_cache.unwrap_or_else(|| {
            let builder = MetadataCache::builder();
            match &self.metrics_recorder {
                Some(recorder) => builder.metrics_recorder(Arc::clone(recorder)),
                None => builder,
            }
            .build()
        });
        let execution_budget = self.execution_budget.unwrap_or_else(|| {
            let builder = ExecutionBudget::builder();
            match &self.metrics_recorder {
                Some(recorder) => builder.metrics_recorder(Arc::clone(recorder)),
                None => builder,
            }
            .build()
        });
        let metadata_run_policy = MetadataRunPolicy {
            max_decoded_input_bytes_per_step: execution_budget.max_merge_input_bytes(),
            ..MetadataRunPolicy::default()
        };
        // Bind the cache to the store the caller supplied, not to the metrics
        // wrapper below. Each runtime builds its own wrapper, so runtimes over
        // clones of one store would otherwise read under separate scopes.
        let metadata_cache = metadata_cache.bind(
            &store,
            execution_budget.read_working_memory(),
            self.stored_metadata_block_cache,
        );
        let instruments = RuntimeInstruments::new(self.metrics_recorder);
        let recorder = fan_out_object_store_recorder(
            self.object_store_metrics_recorder,
            instruments.object_store_recorder(),
        );
        let store = match recorder {
            Some(recorder) => Arc::new(
                InstrumentedObjectStore::new(store, recorder)
                    .store_kind(crate::trace::store_kind_label(store_kind)),
            ) as SharedObjectStore,
            None => store,
        };
        Ok(RuntimeCore::open(
            store,
            ReadConfig {
                max_read_content_bytes: self.max_read_content_bytes,
                manifest_revalidation_interval_ms: self.manifest_revalidation_interval_ms,
                bounded_read_interval_ms: self.bounded_read_interval_ms,
                metadata_run_policy,
                execution_budget,
                trace_mode: self.trace_mode,
                store_kind,
            },
            metadata_cache,
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
        Error::Config(
            "a writable runtime must be built inside the Tokio runtime that will own it".to_owned(),
        )
    })
}
