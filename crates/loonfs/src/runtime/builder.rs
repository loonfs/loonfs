//! The builder for a [`LoonFs`] runtime in either mode.

use super::{LoonFs, ReadOnly, Writable, WriterState};
use crate::metrics::{
    fan_out_object_store_recorder, MetricsRecorder, ObjectStoreMetricsRecorder, RuntimeInstruments,
};
use crate::publisher::PublisherRegistry;
use crate::runtime_core::RuntimeCore;
use crate::{
    Error, ExecutionBudget, MetadataCache, Result, RuntimeConfig, SharedObjectStore, StoreConfig,
};
use loonfs_core::MetadataRunPolicy;
use loonfs_objectstore::metrics::InstrumentedObjectStore;
use loonfs_objectstore::ConfiguredObjectStoreKind;
use loonfs_types::WriterId;
use std::marker::PhantomData;
use std::sync::Arc;

/// Builder for a [`LoonFs`] runtime in mode `M`.
///
/// [`Self::config`] sets the runtime's settings, and the other methods set
/// its components: the store's label, the caches, the clocks, the execution
/// budget, and the metrics recorders. The writer ID exists only on the
/// writable builder. [`Self::read_only`] turns a writable builder into a
/// read-only one.
#[must_use]
pub struct LoonFsBuilder<M> {
    core: CoreConfig,
    writer_id: Option<WriterId>,
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
    config: RuntimeConfig,
    store_kind: Option<ConfiguredObjectStoreKind>,
    /// `None` builds a private cache when the core opens.
    metadata_cache: Option<MetadataCache>,
    execution_budget: Option<ExecutionBudget>,
    timer: Arc<dyn loonfs_types::MonotonicTimer>,
    wall_clock: Arc<dyn crate::WallClock>,
    object_store_metrics_recorder: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
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
                config: RuntimeConfig::default(),
                store_kind: None,
                metadata_cache: None,
                execution_budget: None,
                timer: Arc::new(loonfs_types::StdMonotonicTimer::default()),
                wall_clock: Arc::new(loonfs_core::time::SystemWallClock),
                object_store_metrics_recorder: None,
                metrics_recorder: None,
            },
            writer_id: None,
            mode: PhantomData,
        }
    }

    /// Sets the runtime's settings.
    ///
    /// Defaults to [`RuntimeConfig::default`]. The build fails with
    /// [`Error::Config`] when [`RuntimeConfig::validate`] does.
    pub fn config(mut self, config: RuntimeConfig) -> Self {
        self.core.config = config;
        self
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
    /// keeps its own namespace heads and WAL-tail projections in the cache, and
    /// its clones, its read-only view, and its maintenance share them.
    /// Without this setting, the runtime creates a private cache with the
    /// default limits. Either cache keeps its counters in
    /// [`MetadataCache::stats`], and no metrics recorder receives them.
    pub fn metadata_cache(mut self, metadata_cache: MetadataCache) -> Self {
        self.core.metadata_cache = Some(metadata_cache);
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
    /// the current head take their permits from it, and every merge keeps at
    /// most its merge input size. The per-namespace admission limits stay
    /// with this runtime. The budget
    /// reports its metrics to its own recorder. Without this setting, the
    /// runtime creates a private budget with the default limits that reports
    /// to [`Self::metrics_recorder`].
    pub fn execution_budget(mut self, execution_budget: ExecutionBudget) -> Self {
        self.core.execution_budget = Some(execution_budget);
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
    /// it from then on. Every mode reports object-store calls and where each
    /// read took the head it started from. A writable runtime and its
    /// maintenance also report publications, compactions, and collection
    /// passes. A runtime built without one registers nothing. The metadata
    /// cache doesn't report here, and its counters are read with
    /// [`MetadataCache::stats`]. The private execution budget that a runtime
    /// creates without [`Self::execution_budget`] also reports here, and a
    /// budget given to that setting reports to its own recorder instead.
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
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Turns this builder into one for a read-only runtime.
    ///
    /// It keeps the store, the settings, and the components, such as the
    /// metadata cache and the execution budget, and drops the writer ID.
    pub fn read_only(self) -> LoonFsBuilder<ReadOnly> {
        LoonFsBuilder {
            core: self.core,
            writer_id: None,
            mode: PhantomData,
        }
    }

    /// Opens the writable runtime inside the Tokio runtime that owns
    /// publication tasks.
    ///
    /// Construction runs one way only, so nothing here is cyclic: the read
    /// core opens first, the writer's state is built on top of it, and the
    /// publication service is created last, keeping the core strongly and
    /// the writer's state weakly.
    pub async fn build(self) -> Result<LoonFs<Writable>> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| Error::Config("`writer_id` is required".to_owned()))?;
        let runtime = owning_runtime()?;
        let core = self.core.open()?;
        let writer = Arc::new(WriterState {
            writer_id,
            compactor_epochs: tokio::sync::Mutex::default(),
        });
        let publisher = PublisherRegistry::new(core.clone(), Arc::downgrade(&writer), runtime);
        core.set_head_positions(publisher.head_positions());
        Ok(LoonFs {
            core,
            mode: Writable { writer, publisher },
        })
    }
}

impl CoreConfig {
    /// Resolves the object-store client, wraps it for metrics when the
    /// builder was given a recorder, and opens the runtime core.
    ///
    /// A builder given no recorder of either kind wraps nothing: the store
    /// the core keeps is the store it was handed, and the instrument set it
    /// has reports nowhere.
    fn open(self) -> Result<RuntimeCore> {
        if !loonfs_core::time::system_clock_reads_after_epoch() {
            return Err(Error::Config(
                "the system clock reads before the unix epoch; LoonFS stamps unix milliseconds \
                 into durable state"
                    .to_owned(),
            ));
        }
        self.config.validate()?;
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
        let metadata_cache = self.metadata_cache.unwrap_or_default();
        let execution_budget = self.execution_budget.unwrap_or_else(|| {
            let builder = ExecutionBudget::builder();
            match &self.metrics_recorder {
                Some(recorder) => builder.metrics_recorder(Arc::clone(recorder)),
                None => builder,
            }
            .build()
        });
        let run_policy = MetadataRunPolicy {
            max_decoded_input_bytes_per_step: execution_budget.max_merge_input_bytes(),
            ..MetadataRunPolicy::default()
        };
        // Bind the cache to the store the caller supplied, not to the metrics
        // wrapper below. Each runtime builds its own wrapper, so runtimes over
        // clones of one store would otherwise read under separate scopes.
        let metadata_cache = metadata_cache.bind(&store, execution_budget.read_working_memory());
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
            self.config,
            execution_budget,
            run_policy,
            store_kind,
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
