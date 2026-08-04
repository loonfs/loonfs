//! The write-capable runtime handle.

use super::{owning_runtime, FsReader, HandleBuilderCore};
use crate::fs::{ReadCore, WriterBits, WriterIdentity};
use crate::maintenance_runner::{register_core_jobs, MaintenanceRunner};
use crate::metrics::{MetricsRecorder, ObjectStoreMetricsRecorder};
use crate::publisher::{PublishObserver, PublisherRegistry};
use crate::{
    CapabilityDocument, ChangeSeq, FsBackgroundWork, MaintenanceHandle, MaintenanceJob,
    MaintenanceJobId, NamespaceId, Result, RuntimeCacheConfig, RuntimeCacheStats, RuntimeError,
    SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
};
use loonfs_core::cache::MetadataTableCache;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Write-capable handle for normal application and server use.
///
/// `FsWriter` owns a writer identity and the full mutation surface:
/// file and directory mutations, commit publication, uploads, and namespace
/// lifecycle. With [`FsBackgroundWork::Enabled`] it also owns a maintenance
/// runner, spawned on its owning runtime: metadata steps after writes that
/// cross the WAL-tail threshold, and collection passes for the upload
/// sessions it opened once their leases pass. Advancing the retention floor
/// stays explicit [`FsAdmin`](crate::FsAdmin) work.
///
/// The handle is runtime-bound: open it with `build().await` inside the
/// long-lived Tokio runtime that will drive it, and do not share one writer
/// across unrelated runtimes — open another from [`StoreConfig`] instead.
/// `FsWriter` is cheap to clone; clones share the identity, caches, and
/// background-work state — including its end, which is why
/// [`Self::shutdown`] is one call rather than a sequence a host respells.
#[derive(Clone)]
pub struct FsWriter {
    pub(crate) core: ReadCore,
    /// The writer half of the runtime: the writer identity, the maintenance
    /// runner, and the publish observer. Publisher workers hold this weakly,
    /// so dropping every clone of this handle stops new publication work.
    pub(crate) bits: Arc<WriterBits>,
    pub(crate) publisher: PublisherRegistry,
}

impl FsWriter {
    /// Starts a writer builder that constructs its object-store client from
    /// configuration inside this handle's runtime ownership domain.
    pub fn builder(store_config: StoreConfig) -> FsWriterBuilder {
        FsWriterBuilder::new(HandleBuilderCore::from_config(store_config))
    }

    /// Starts a writer builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this handle's runtime
    /// ownership domain. Do not use it to share one provider client across
    /// unrelated runtimes; open another handle from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> FsWriterBuilder {
        FsWriterBuilder::new(HandleBuilderCore::from_store(store))
    }

    /// Derives a read-only handle over this writer's store and caches.
    ///
    /// The reader lives in the writer's runtime ownership domain and sees
    /// its cache updates immediately; use it for read paths in the same
    /// process, such as a server's read endpoints. For a reader driven by a
    /// different runtime, open one with [`FsReader::builder`].
    pub fn reader(&self) -> FsReader {
        FsReader::from_read_core(self.core.clone())
    }

    /// This writer's shared decoded-block cache handle, for builders that
    /// open another core sharing it.
    pub(crate) fn metadata_table_cache(&self) -> Arc<MetadataTableCache> {
        self.core.metadata_table_cache()
    }

    /// This writer's object-store client, instrumented exactly as the
    /// handle's own traffic is.
    ///
    /// Server integrations that read LoonFS-owned objects outside the handle
    /// surface — the grep root and the grep worker's keyspace — use this so
    /// their requests are measured like every other request instead of
    /// escaping instrumentation on a second, raw client.
    pub fn object_store(&self) -> SharedObjectStore {
        self.core.shared_store()
    }

    /// This writer's publication service (see [`crate::publisher`]).
    ///
    /// The direct mutation methods on this handle submit through the same
    /// service; hosts that classify their own mutation candidates — the
    /// reference server, for example — submit here directly. Clones share
    /// the writer's per-namespace publishers.
    ///
    /// Lifecycle is not the caller's to drive here: [`Self::shutdown`] owns
    /// closing this service, and [`Self::is_shutting_down`] answers the one
    /// question a host asks about it from outside.
    pub fn publisher(&self) -> PublisherRegistry {
        self.publisher.clone()
    }

    /// Whether [`Self::shutdown`] has begun on this writer or any clone of
    /// it.
    ///
    /// Mutations submitted from here on fail with `shutting_down`, so a
    /// readiness probe answers "draining" from this and a load balancer can
    /// take the instance out before its in-flight work settles.
    pub fn is_shutting_down(&self) -> bool {
        self.publisher.is_admission_closed()
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    pub fn capabilities(&self) -> CapabilityDocument {
        self.core.capabilities()
    }

    /// Snapshots the runtime cache counters.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
    }

    // Namespace lifecycle lives in `fs/namespaces.rs`; mutation, commit,
    // and upload operations in `fs/writes.rs` and `fs/uploads.rs`.

    /// A nudge-only view of this writer's maintenance runner.
    ///
    /// Hosts that compose an extension — a derived index, say — hand this to
    /// whatever observes change, so their triggers reach the same admission
    /// the runtime's own jobs go through. Nudges never block and are ignored
    /// under [`FsBackgroundWork::ManualOnly`] and after shutdown.
    pub fn maintenance(&self) -> MaintenanceHandle {
        self.bits.maintenance.handle()
    }

    /// Registers an extension's maintenance executor alongside the runtime's
    /// own, under the job's own id.
    ///
    /// The registered job shares this writer's permit pool, backoff, and
    /// shutdown; [`Self::maintenance`] is how its work gets admitted.
    /// Registering the same id twice is a configuration error.
    pub fn register_maintenance_job(
        &self,
        job: Arc<dyn MaintenanceJob>,
    ) -> Result<MaintenanceJobId> {
        self.bits.maintenance.register(job)
    }

    /// The executor registered under `job`, runtime-owned or extension-owned
    /// alike, or `None` when nothing claims that id.
    ///
    /// For a host that drives bounded steps itself rather than through
    /// admission: `loonfs admin run --drain` runs each key to a settled
    /// conclusion under the operator's budget, and a caller with its own
    /// budget needs the executor, not a nudge. What it gets is the same
    /// bounded, compare-and-swap-published unit the runner admits — running
    /// one here duplicates work at worst, because delivery is at-least-once
    /// either way.
    pub fn maintenance_job(&self, job: MaintenanceJobId) -> Option<Arc<dyn MaintenanceJob>> {
        self.bits.maintenance.job(job)
    }

    /// Waits until every writer-scheduled maintenance task has finished,
    /// without closing anything. Panicked tasks surface as a runtime-task
    /// error.
    ///
    /// Non-terminal: the writer keeps admitting work, so what this waits
    /// for is quiet, not the end. A one-shot host calls it before exiting
    /// so a step is never torn down mid-flight; a long-lived host calls it
    /// to read durable state a step was about to leave. Callers await their
    /// own publications, so a quiet runner is a quiet writer.
    pub async fn flush_background(&self) -> Result<()> {
        self.bits.maintenance.drain().await
    }

    /// Terminal shutdown: closes maintenance admission, closes publication
    /// admission, settles admitted publications, then settles in-flight
    /// maintenance steps. Panicked tasks surface as a runtime-task error.
    ///
    /// This is the only valid order, and it lives here so no host has to
    /// respell it. Afterward, mutations fail with `shutting_down`, nudges
    /// are dropped, and work that claimed a maintenance slot without
    /// starting is refused rather than left running unobserved. Reads still
    /// work: nothing here touches the read path.
    ///
    /// Maintenance admission has to close first, and before this future's
    /// first await — the publication drain below is that await. While it is
    /// pending the runtime keeps polling the runner: its timer promotes
    /// keys whose deadlines have arrived, each landing publication nudges
    /// the jobs subscribed to publications, and a finishing step hands its
    /// permit straight to the next queued key. Everything admitted in that
    /// window is work this shutdown already decided to drop, and none of it
    /// is free — a metadata step advances the metadata root, a collection
    /// pass deletes provider objects, an index step writes segments, all
    /// after the process was asked to stop, and all of it the drain then
    /// has to sit through. Closing first leaves the window empty.
    ///
    /// Nor can the order wedge, for a reason worth stating because it is
    /// not the obvious one: no maintenance step submits to the publication
    /// service at all. Every job compare-and-swaps the namespace head
    /// through [`FsAdmin`](crate::FsAdmin), so the publication drain waits
    /// only on client work and its pending set can only shrink. A step
    /// already running finishes normally, and its chain then ends rather
    /// than passing its permit on, because a closed admission book releases
    /// the permit instead of handing it to the next key.
    ///
    /// Takes `&self` because `FsWriter` is [`Clone`] and exclusivity is
    /// therefore unenforceable: clones observe the shutdown rather than
    /// being consumed by it, and [`Self::is_shutting_down`] is what they
    /// see. Calling it again — from this handle or any clone — is safe: the
    /// closes are idempotent and the drains are waits, so a later call
    /// settles whatever a concurrent one has not and returns. Dropping
    /// every clone without calling this is best-effort cleanup, not the
    /// documented graceful shutdown path.
    pub async fn shutdown(&self) -> Result<()> {
        // Both closes belong above the first await, for the reason the doc
        // gives. Nothing may move below `drain`.
        self.bits.maintenance.close_admission();
        self.publisher.close_admission();
        self.publisher.drain().await?;
        self.bits.maintenance.drain().await
    }
}

/// Builder for [`FsWriter`].
pub struct FsWriterBuilder {
    core: HandleBuilderCore,
    writer_id: Option<String>,
    background_work: FsBackgroundWork,
    max_concurrent_maintenance: usize,
    min_publish_interval_ms: u64,
    publish_observer: Option<PublishObserver>,
    maintenance_clock: Option<Arc<dyn crate::maintenance_runner::MaintenanceClock>>,
}

impl FsWriterBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            writer_id: None,
            background_work: FsBackgroundWork::ManualOnly,
            max_concurrent_maintenance: crate::config::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
            min_publish_interval_ms: crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS,
            publish_observer: None,
            maintenance_clock: None,
        }
    }

    /// Substitutes the clock the maintenance runner schedules against, so a
    /// test can assert on the times a write path plants without waiting for
    /// them. Scheduling only: no durable stamp comes from here.
    #[cfg(test)]
    pub(crate) fn maintenance_clock(
        mut self,
        clock: Arc<dyn crate::maintenance_runner::MaintenanceClock>,
    ) -> Self {
        self.maintenance_clock = Some(clock);
        self
    }

    /// Sets the writer id used by namespace mutations. Required.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Sets the writer's background maintenance policy.
    ///
    /// Defaults to [`FsBackgroundWork::ManualOnly`]: right for CLIs, tests,
    /// scripts, and short-lived embedders. Long-lived servers should opt
    /// into [`FsBackgroundWork::Enabled`] explicitly.
    pub fn background_work(mut self, background_work: FsBackgroundWork) -> Self {
        self.background_work = background_work;
        self
    }

    /// Caps how many writer-scheduled maintenance steps may run at once
    /// across every job and namespace this writer serves. Each job already
    /// runs at most one step per namespace at a time; this bounds the
    /// fan-out when many namespaces need work together. Requests beyond the
    /// cap coalesce per job and namespace and run as permits free. Defaults to
    /// [`crate::DEFAULT_MAX_CONCURRENT_MAINTENANCE`]. The limit must be
    /// greater than zero; [`FsBackgroundWork::ManualOnly`] is the only way
    /// to disable scheduling.
    pub fn max_concurrent_maintenance(mut self, max_concurrent_maintenance: usize) -> Self {
        self.max_concurrent_maintenance = max_concurrent_maintenance;
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

    /// Sets the minimum interval between publication starts per namespace,
    /// in milliseconds (see [`crate::publisher`]).
    ///
    /// A cold namespace publishes immediately; the interval only paces
    /// follow-up batches, so concurrent publishes amortize into fewer,
    /// larger WAL segments — with each caller still awaiting its own
    /// durable, visible result. Defaults to 15 ms; zero keeps only the
    /// batching that in-flight publications force.
    pub fn min_publish_interval_ms(mut self, min_publish_interval_ms: u64) -> Self {
        self.min_publish_interval_ms = min_publish_interval_ms;
        self
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.core.runtime_cache = runtime_cache;
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

    /// Installs raw object-store sample collection for this handle.
    ///
    /// The handle wraps its object store before opening; callers do not
    /// need to construct an instrumented store manually. Combines with
    /// [`Self::metrics_recorder`]: one wrapper feeds both.
    pub fn object_store_metrics_recorder(
        mut self,
        recorder: Arc<dyn ObjectStoreMetricsRecorder>,
    ) -> Self {
        self.core.object_store_metrics_recorder = Some(recorder);
        self
    }

    /// Installs the metrics recorder this handle reports its instruments to
    /// (see [`crate::metrics`]).
    ///
    /// The handle registers its instrument set once, here, and reports into
    /// it from then on: object-store calls, maintenance steps, publications,
    /// and collection passes. A handle built without one registers nothing.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Registers a synchronous observer called after each successful durable
    /// publication with the namespace and the batch's highest committed
    /// sequence.
    ///
    /// The observer runs on the publication task and must not block. Use a
    /// non-blocking channel send to hand work to another task. Writers that
    /// register no observer retain the default publication behavior.
    pub fn publish_observer(
        mut self,
        observer: impl Fn(&NamespaceId, ChangeSeq) + Send + Sync + 'static,
    ) -> Self {
        self.publish_observer = Some(Arc::new(observer));
        self
    }

    /// Opens the writer inside the Tokio runtime that will own it. Any
    /// background work the writer schedules is spawned on that runtime.
    ///
    /// Construction runs one way only, so nothing here is cyclic: the read
    /// core opens first, the writer's bits are built on top of it, and the
    /// publication service is created last, holding the core strongly and
    /// the bits weakly.
    pub async fn build(self) -> Result<FsWriter> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        let identity = WriterIdentity::new(writer_id)?;
        let max_concurrent_maintenance = NonZeroUsize::new(self.max_concurrent_maintenance)
            .ok_or_else(|| {
                RuntimeError::Config(
                    "`max_concurrent_maintenance` must be greater than zero; \
                     `FsBackgroundWork::ManualOnly` disables scheduling"
                        .to_owned(),
                )
            })?;
        let runtime = Some(owning_runtime()?);
        let core = self.core.open_read_core()?;
        let instruments = Arc::clone(core.instruments());
        let maintenance = match self.maintenance_clock {
            Some(clock) => MaintenanceRunner::with_clock(
                self.background_work,
                runtime,
                max_concurrent_maintenance,
                clock,
                instruments,
            ),
            None => MaintenanceRunner::new(
                self.background_work,
                runtime,
                max_concurrent_maintenance,
                instruments,
            ),
        };
        let bits = Arc::new(WriterBits {
            identity,
            maintenance,
            publish_observer: self.publish_observer,
        });
        let publisher = PublisherRegistry::new(
            core.clone(),
            Arc::downgrade(&bits),
            std::time::Duration::from_millis(self.min_publish_interval_ms),
            core.trace_mode(),
            core.trace_store_kind(),
        );
        // The runtime's own executors register last: they run as an admin
        // over this writer's parts, so both have to exist first.
        register_core_jobs(&bits.maintenance, &core, &bits, &publisher)?;
        Ok(FsWriter {
            core,
            bits,
            publisher,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CreateNamespaceOptions, ErrorCode, FsBackgroundWork, FsWriter, MaintenanceJobId,
        NamespaceId, PutFileOptions,
    };
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::ids::namespace_id;
    use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
    use std::sync::Arc;
    use tempfile::tempdir;

    /// A writer whose store parks the first head compare-and-swap, so a
    /// publication can be held open across a shutdown's first poll.
    async fn parked_publication_writer(
        temp_dir: &std::path::Path,
        writer_id: &str,
        namespace_id: &NamespaceId,
    ) -> (FsWriter, Arc<BlockingStore<LocalFsStore>>) {
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir).expect("create local-fs store"),
            KeyPredicate::wal_head(namespace_id.as_str()),
            OperationClass::CompareAndSwap,
        ));
        let writer = FsWriter::builder_with_store(blocking.clone())
            .writer_id(writer_id)
            .background_work(FsBackgroundWork::Enabled)
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        (writer, blocking)
    }

    /// Both admissions must close on the shutdown's first poll, before it
    /// starts draining publications. The drain is a wait, and the runner
    /// stays live across it: a queue still open here lets a nudge landing in
    /// that window be admitted, and lets a finishing step hand its slot on
    /// and spawn work the shutdown already decided to drop. Asserting on the
    /// first poll pins the order on every machine; the integration coverage
    /// in `tests/it/handles.rs` only loses the race on some.
    #[tokio::test]
    async fn shutdown_closes_both_admissions_before_draining_publications() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("parked");
        let (writer, blocking) =
            parked_publication_writer(temp_dir.path(), "shutdown-order-writer", &namespace_id)
                .await;

        // Park a publication so the shutdown's publication drain is still
        // pending when the first poll returns.
        blocking.block_next();
        let put = tokio::spawn({
            let writer = writer.clone();
            let namespace_id = namespace_id.clone();
            async move {
                writer
                    .put_file_bytes(
                        &namespace_id,
                        "/parked.txt",
                        b"body",
                        PutFileOptions::default(),
                    )
                    .await
            }
        });
        blocking.wait_until_blocked().await;

        let mut shutdown = Box::pin(writer.shutdown());
        assert!(
            futures::poll!(shutdown.as_mut()).is_pending(),
            "the parked publication must keep the shutdown pending"
        );
        // Behavioral, not a flag read: a nudge lands after the first poll
        // and must leave nothing admitted behind it.
        writer
            .maintenance()
            .nudge(MaintenanceJobId::METADATA, &namespace_id);
        assert!(
            !writer
                .bits
                .maintenance
                .is_pending(MaintenanceJobId::METADATA, &namespace_id),
            "maintenance admission must already be closed while publications drain"
        );
        // The publication half of the same window: a mutation submitted
        // into the drain would be work the drain then has to wait for.
        let refused = writer
            .put_file_bytes(
                &namespace_id,
                "/late.txt",
                b"body",
                PutFileOptions::default(),
            )
            .await
            .expect_err("a mutation submitted during the drain must be refused");
        assert_eq!(
            refused.code(),
            ErrorCode::ShuttingDown,
            "a late mutation reports `shutting_down`: {refused:?}"
        );
        assert!(writer.is_shutting_down());

        blocking.release();
        put.await
            .expect("join the parked put")
            .expect("the released put succeeds");
        shutdown.await.expect("shut down the writer");
    }

    /// Shutdown is a state of the shared runtime, not a token one handle
    /// holds: a clone that shuts down is observable from every other clone,
    /// and a second shutdown settles rather than panicking or wedging.
    #[tokio::test]
    async fn a_clone_observes_a_shutdown_and_may_repeat_it() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = namespace_id("clones");
        let (writer, _blocking) =
            parked_publication_writer(temp_dir.path(), "shutdown-clone-writer", &namespace_id)
                .await;
        let clone = writer.clone();
        assert!(!clone.is_shutting_down());

        writer.shutdown().await.expect("shut down the writer");
        assert!(
            clone.is_shutting_down(),
            "a clone sees the runtime it shares shutting down"
        );
        clone
            .shutdown()
            .await
            .expect("a second shutdown settles rather than failing");
        clone
            .flush_background()
            .await
            .expect("a shut runner has nothing left to settle");
    }
}
