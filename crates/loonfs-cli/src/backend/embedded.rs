//! Embedded CLI backend implemented by an in-process `loonfs` runtime.
//!
//! It implements the same operations as the remote backend.

use crate::backend_error::{
    map_namespace_scoped_grep_error, map_namespace_scoped_runtime_error, map_runtime_error,
    BackendError,
};
use crate::render::write_stderr_warning;
use loonfs::{
    ByteStream, ChangesResponse, CheckpointPageCursor, CopyOptions, CreateCheckpointOptions,
    CreateDirectoryOptions, CreateNamespaceOptions, DeleteNamespaceOptions,
    DeleteNamespaceResponse, DeleteOptions, FileContentStream, FsAdmin, FsReader, FsWriter,
    ListChangesOptions, ListPathEntriesOptions, MaintenanceHandle, MaintenanceJob,
    MaintenanceJobId, MaintenancePlan, MaintenanceStepConclusion, MoveOptions, PutFileOptions,
    ReadFileStreamOptions, RestoreRevisionOptions, RuntimeError, SharedObjectStore,
    StatPathOptions, UndeleteOptions, UpdateAttributesOptions,
};
use loonfs_api::{
    v0::{
        DisableGrepIndexResponse, EnableGrepIndexResponse, GrepGcRequest, GrepGcResponse,
        GrepIndexLifecycle, GrepIndexStatusResponse, StoreProbeCheckOutcome, StoreProbeCheckResult,
        StoreProbeResponse,
    },
    AbsolutePath, AuthoritativePathEntry, ChangeSeq, CheckpointId, CommitResponse,
    CreateCheckpointRequest, CreateCheckpointResponse, EffectiveLimit, ErrorCode, GrepRequest,
    GrepResponse, InodeId, ListCheckpointsResponse, ListFileRevisionsResponse,
    ListPathEntriesResponse, ListTrashResponse, MaintenanceStepRequest, MaintenanceStepResponse,
    NamespaceId, NamespaceStatusResponse, NamespaceSummary, PaginationPolicy,
    ReleaseCheckpointResponse, RevisionNo,
};
use loonfs_client::NamespacePath;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBlockCache, GrepDisableOutcome, GrepEnableOutcome, GrepError,
    GrepGcJob, GrepMaintenanceJob, GrepService, GrepWorker, NamespaceReads, GREP_GC_JOB,
    GREP_INDEX_JOB,
};
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeOutcome, StoreProbeReport};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::sync::Arc;

use super::{GrepWaitProgress, MaintenanceDrainProgress, MaintenanceKeyProgress, StepBudget};

/// Purpose-specific handles over one shared store client: reads go through
/// the reader, mutations through the writer, and maintenance through the
/// admin handle. The embedded writer runs `FsBackgroundWork::Enabled` — the
/// same policy as the reference server — so a publish that crosses the WAL
/// threshold schedules its own maintenance step, and every mutation settles
/// scheduled work before the one-shot process exits. A publish gated on
/// `maintenance_required` waits for the step that same gated publish
/// scheduled, then resubmits, so embedded writes recover from WAL debt
/// instead of hard-stopping. `loonfs admin` commands remain the explicit path
/// for everything else (GC, retention, forced steps).
pub(crate) struct EmbeddedBackend {
    pub(crate) writer: FsWriter,
    pub(crate) reader: FsReader,
    pub(crate) admin: FsAdmin,
    /// Grep is composed here rather than by the runtime: this service owns
    /// the query side for the length of the command.
    pub(crate) grep: GrepService,
    /// The process-wide grep cache is retained separately so workers created
    /// lazily by commands share the service's decoded immutable blocks.
    pub(crate) grep_block_cache: Arc<GrepBlockCache>,
}

/// How many times a gated publish resubmits after settling the maintenance
/// step it scheduled. One recovery is the normal case; the second covers a
/// step that raced another writer's debt. Past that the error surfaces.
const MAX_MAINTENANCE_RECOVERIES: usize = 2;

/// One job `admin run` was asked to host, and the executor registered under
/// it.
type HostedJob = (MaintenanceJobId, Arc<dyn MaintenanceJob>);

impl EmbeddedBackend {
    /// Waits out writer-scheduled maintenance so a one-shot command never
    /// exits (tearing down the runtime) while a step is mid-flight. A settle
    /// failure after a committed mutation is reported as a warning on
    /// stderr, never as the mutation's outcome — the commit landed.
    async fn settle_background_work_after<T>(
        &self,
        result: Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        match (result, self.writer.flush_background().await) {
            (result, Ok(())) => result,
            (Ok(value), Err(error)) => {
                write_stderr_warning(format_args!(
                    "background maintenance did not settle cleanly: {error}"
                ));
                Ok(value)
            }
            (Err(error), Err(_)) => Err(error),
        }
    }

    /// Runs one mutation with `maintenance_required` recovery: a gated
    /// publish observes the oversized WAL tail and schedules its own
    /// recovery step (the writer policy is `Enabled`), so settle that step
    /// and resubmit. A gated attempt commits nothing, so the resubmission
    /// cannot double-apply.
    async fn publish_with_maintenance_recovery<T, F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        attempt: F,
    ) -> Result<T, BackendError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, RuntimeError>>,
    {
        let mut result = attempt().await;
        for _ in 0..MAX_MAINTENANCE_RECOVERIES {
            let gated = matches!(
                &result,
                Err(RuntimeError::Core(error))
                    if matches!(error.code(), ErrorCode::MaintenanceRequired)
            );
            if !gated {
                break;
            }
            self.writer
                .flush_background()
                .await
                .map_err(map_runtime_error)?;
            result = attempt().await;
        }
        let result =
            result.map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error));
        self.settle_background_work_after(result).await
    }

    /// A grep worker over this backend's own handles: grep's keyspace rides
    /// the writer's store client, its filesystem reads the reader, and its
    /// backfill checkpoints the admin handle.
    fn grep_worker(&self) -> GrepWorker<SharedObjectStore> {
        GrepWorker::with_block_cache(
            self.writer.object_store(),
            self.reader.clone(),
            self.admin.clone(),
            Arc::clone(&self.grep_block_cache),
        )
    }

    pub(super) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        let result = self
            .writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .map_err(map_runtime_error);
        self.settle_background_work_after(result).await
    }

    pub(super) async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        let options = DeleteNamespaceOptions { expected_head_seq };
        let result = self
            .writer
            .delete_namespace(namespace_id, options)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error));
        self.settle_background_work_after(result).await
    }

    pub(super) async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        let result = self
            .writer
            .fork_namespace(source_namespace_id, new_namespace_id)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(source_namespace_id, error));
        self.settle_background_work_after(result).await
    }

    pub(super) async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        self.admin
            .namespace_status(namespace_id)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    pub(super) async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<ListPathEntriesResponse, BackendError> {
        self.reader
            .list_path_entries_all(spec.namespace(), spec.absolute_path().as_str())
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))
    }

    pub(super) async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListPathEntriesResponse, BackendError> {
        let request = loonfs_api::PageRequest {
            limit: resolve_cli_page_limit(limit)?,
            cursor: cursor
                .map(loonfs_api::decode_cursor)
                .transpose()
                .map_err(|error| {
                    BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                })?,
        };
        self.reader
            .list_path_entries_page(
                spec.namespace(),
                spec.absolute_path().as_str(),
                request,
                ListPathEntriesOptions::default(),
            )
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))
    }

    pub(super) async fn stat_path(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<AuthoritativePathEntry, BackendError> {
        self.reader
            .stat_path(
                spec.namespace(),
                spec.absolute_path().as_str(),
                options.clone(),
            )
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))
    }

    pub(super) async fn get_file_bytes(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<u8>, BackendError> {
        let result = self
            .reader
            .get_file_bytes(spec.namespace(), spec.absolute_path().as_str())
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))?;
        Ok(result.bytes)
    }

    /// Opens a file's content as bounded chunks, at the runtime's own chunk
    /// size: what a download costs this process is that chunk, not the file.
    ///
    /// `start_offset` skips what the caller already holds; the stream still
    /// verifies the whole object, so those bytes reach it through
    /// [`FileContentStream::fold_resumed_prefix`] before it reads anything.
    pub(super) async fn read_file_stream(
        &self,
        spec: &NamespacePath,
        start_offset: u64,
    ) -> Result<FileContentStream<SharedObjectStore>, BackendError> {
        self.reader
            .read_file_stream(
                spec.namespace(),
                spec.absolute_path().as_str(),
                ReadFileStreamOptions {
                    start_offset,
                    ..ReadFileStreamOptions::default()
                },
            )
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))
    }

    pub(super) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError> {
        let store = self.writer.object_store();
        let reads = NamespaceReads::new(&self.reader, namespace_id);
        self.grep
            .query(request, &reads, &store)
            .await
            .map_err(|error| map_namespace_scoped_grep_error(namespace_id, error))
    }

    pub(super) async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse, BackendError> {
        let grep_error = |error| map_namespace_scoped_grep_error(namespace_id, error);
        let (already_enabled, lifecycle) = match self
            .grep_worker()
            .enable(namespace_id)
            .await
            .map_err(grep_error)?
        {
            GrepEnableOutcome::Enabled { state } => (false, state),
            GrepEnableOutcome::AlreadyEnabled { state } => (true, state),
            GrepEnableOutcome::Superseded => {
                return Err(grep_error(GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(namespace_id),
                }))
            }
        };
        // Enabling is one compare-and-swap and nothing else, here as on a
        // server. Driving the backfill afterwards is the command's job, not
        // this call's, so an embedded caller and a remote one get the same
        // answer to the same question.
        Ok(EnableGrepIndexResponse {
            namespace_id: namespace_id.clone(),
            already_enabled,
            state: GrepIndexLifecycle::from(&lifecycle),
        })
    }

    pub(super) async fn grep_index_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse, BackendError> {
        let root = self
            .grep_worker()
            .root_state(namespace_id)
            .await
            .map_err(|error| map_namespace_scoped_grep_error(namespace_id, error))?;
        let (state, next_run_ordinal, reorganize_pending) = match &root {
            Some(root) => (
                GrepIndexLifecycle::from(root.lifecycle()),
                root.index().next_run_ordinal,
                root.index().reorganize.is_some(),
            ),
            None => (GrepIndexLifecycle::Disabled, 0, false),
        };
        Ok(GrepIndexStatusResponse {
            namespace_id: namespace_id.clone(),
            state,
            next_run_ordinal,
            reorganize_pending,
        })
    }

    pub(super) async fn gc_grep_index(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepGcRequest,
    ) -> Result<GrepGcResponse, BackendError> {
        let report = self
            .grep_worker()
            .garbage_collect_namespace(
                namespace_id,
                current_unix_ms()?,
                &loonfs_grep::GrepGcRequest {
                    max_objects: request.max_objects,
                    cursor: request.cursor.clone(),
                },
            )
            .await
            .map_err(|error| map_namespace_scoped_grep_error(namespace_id, error))?;
        Ok(GrepGcResponse {
            namespace_id: namespace_id.clone(),
            deleted_segments: report.deleted_segments,
            deleted_other_objects: report.deleted_other_objects,
            namespace_reaped: report.namespace_reaped,
            retained_candidates: report.retained_candidates,
            namespace_degraded: report.namespace_degraded,
            next_cursor: report.next_cursor,
        })
    }

    /// Runs the grep-index job's bounded steps until the index has built
    /// through `target_seq`, or until the budget runs out.
    ///
    /// A one-shot command hosts no maintenance runner, so it runs the job
    /// itself — the same executor a server registers, minus admission and
    /// backoff, so the first failure surfaces instead of being retried. The
    /// target is fixed before the first step, so a namespace that keeps
    /// being written to cannot keep this loop running.
    pub(super) async fn drive_grep_index(
        &self,
        namespace_id: &NamespaceId,
        target_seq: ChangeSeq,
        budget: StepBudget,
    ) -> Result<GrepWaitProgress, BackendError> {
        let worker = self.grep_worker();
        let job = GrepMaintenanceJob::new(worker.clone(), GramIndexBuildPolicy::default());
        let timer = StdMonotonicTimer::default();
        let started_ms = timer.monotonic_now_ms();
        let mut steps = 0;
        let mut settled = false;
        loop {
            let state = GrepIndexLifecycle::from(
                &worker
                    .lifecycle(namespace_id)
                    .await
                    .map_err(|error| map_namespace_scoped_grep_error(namespace_id, error))?,
            );
            let reached = state.is_built_through(target_seq);
            let elapsed_ms = timer.monotonic_now_ms().saturating_sub(started_ms);
            if reached || settled || budget.spent(steps, elapsed_ms) {
                return Ok(GrepWaitProgress {
                    state,
                    steps,
                    reached,
                });
            }
            let conclusion = job
                .step(namespace_id, None)
                .await
                .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))?
                .conclusion;
            steps += 1;
            // A step that settled short of the target says the index has
            // nothing more to do — disabled underneath us, or blocked on a
            // budget of its own. Repeating it would only spin, so the next
            // turn of this loop reports where it stopped.
            settled = !matches!(
                conclusion,
                MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded
            );
        }
    }

    /// Registers the jobs this process composes itself, then resolves every
    /// selected job's executor from the writer that owns it.
    ///
    /// The runtime's own jobs are registered by the writer; grep's is
    /// registered here, over the same worker every other index command in
    /// this process uses. After this, one lookup answers for all three.
    fn hosted_jobs(&self, jobs: &[MaintenanceJobId]) -> Result<Vec<HostedJob>, BackendError> {
        if jobs.contains(&GREP_INDEX_JOB) {
            self.writer
                .register_maintenance_job(Arc::new(GrepMaintenanceJob::new(
                    self.grep_worker(),
                    GramIndexBuildPolicy::default(),
                )))
                .map_err(map_runtime_error)?;
        }
        if jobs.contains(&GREP_GC_JOB) {
            self.writer
                .register_maintenance_job(Arc::new(GrepGcJob::new(self.grep_worker())))
                .map_err(map_runtime_error)?;
        }
        jobs.iter()
            .map(|job| {
                let executor = self.writer.maintenance_job(*job).ok_or_else(|| {
                    BackendError::runtime_error(format!(
                        "no maintenance job is registered under `{job}`"
                    ))
                })?;
                Ok((*job, executor))
            })
            .collect()
    }

    /// Hosts `jobs` for `namespaces` until `shutdown` resolves.
    ///
    /// The runner does the work; this command's job is the assignment. It
    /// nudges every key once at start-up and again on `poll_interval_ms`
    /// (the default cadence when `None`), and the runner decides when each
    /// step runs, how many run at once, and what happens when one fails. The
    /// signal ends the assignment and [`FsWriter::shutdown`] ends the
    /// process's background work, in the one order that is correct.
    pub(super) async fn host_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        poll_interval_ms: Option<u64>,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), BackendError> {
        let hosted = self.hosted_jobs(jobs)?;
        let interval_ms = poll_interval_ms.unwrap_or(ASSIGNMENT_INTERVAL_MS);
        let maintenance = self.writer.maintenance();
        assign(&maintenance, &hosted, namespaces);
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                () = rest_between_assignments(interval_ms) => {
                    assign(&maintenance, &hosted, namespaces);
                }
            }
        }
        self.writer.shutdown().await.map_err(map_runtime_error)
    }

    /// Runs every `{job, namespace}` key to a settled conclusion, or until
    /// `budget` runs out.
    ///
    /// A drain hosts the steps itself rather than nudging the runner: it has
    /// a budget to spend and per-key progress to report, and admission
    /// offers neither. So it shuts the writer down first — a second
    /// scheduler over the same keys would race these steps and make the
    /// counts below a lie — and then walks the assignment, carrying each
    /// key's continuation from one step to the next exactly as the runner
    /// would have. Shutting the writer down does not disarm the steps: a
    /// job compare-and-swaps the namespace head through `FsAdmin`, never
    /// through the publication service the shutdown closed.
    pub(super) async fn drain_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        budget: StepBudget,
    ) -> Result<MaintenanceDrainProgress, BackendError> {
        let hosted = self.hosted_jobs(jobs)?;
        self.writer.shutdown().await.map_err(map_runtime_error)?;
        let timer = StdMonotonicTimer::default();
        let started_ms = timer.monotonic_now_ms();
        let mut steps = 0;
        let mut keys = Vec::with_capacity(hosted.len() * namespaces.len());
        for (job, executor) in &hosted {
            for namespace_id in namespaces {
                let mut key = MaintenanceKeyProgress {
                    job: *job,
                    namespace_id: namespace_id.clone(),
                    steps: 0,
                    conclusion: None,
                };
                let mut continuation = None;
                while !budget.spent(steps, timer.monotonic_now_ms().saturating_sub(started_ms)) {
                    let result = executor
                        .step(namespace_id, continuation.as_deref())
                        .await
                        .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))?;
                    steps += 1;
                    key.steps += 1;
                    key.conclusion = Some(result.conclusion);
                    continuation = result.continuation;
                    if key.settled() {
                        break;
                    }
                }
                keys.push(key);
            }
        }
        Ok(MaintenanceDrainProgress { keys, steps })
    }

    pub(super) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse, BackendError> {
        let grep_error = |error| map_namespace_scoped_grep_error(namespace_id, error);
        match self
            .grep_worker()
            .disable(namespace_id)
            .await
            .map_err(grep_error)?
        {
            GrepDisableOutcome::Disabled => Ok(DisableGrepIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: true,
            }),
            GrepDisableOutcome::NotEnabled => Ok(DisableGrepIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: false,
            }),
            GrepDisableOutcome::Superseded => Err(grep_error(GrepError::PublicationConflict {
                object_key: loonfs_grep::keyspace::root_key(namespace_id),
            })),
        }
    }

    pub(super) async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        let result = self
            .reader
            .get_file_revision_bytes(spec.namespace(), spec.absolute_path().as_str(), revision_no)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))?;
        Ok(result.bytes)
    }

    pub(super) async fn list_trash(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse, BackendError> {
        let request = loonfs_api::PageRequest {
            limit: resolve_cli_page_limit(limit)?,
            cursor: cursor
                .map(loonfs_api::decode_cursor)
                .transpose()
                .map_err(|error| {
                    BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                })?,
        };
        self.reader
            .list_trash_page(namespace_id, request)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    pub(super) async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        let request = loonfs_api::PageRequest {
            limit: resolve_cli_page_limit(limit)?,
            cursor: cursor
                .map(loonfs_api::decode_cursor)
                .transpose()
                .map_err(|error| {
                    BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                })?,
        };
        self.reader
            .list_file_revisions_page(spec.namespace(), spec.absolute_path().as_str(), request)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))
    }

    pub(super) async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.put_file_bytes(
                spec.namespace(),
                spec.absolute_path().as_str(),
                bytes,
                options.clone(),
            )
        })
        .await
    }

    /// Writes a file from a payload read once, straight into the runtime's
    /// streaming staging path.
    ///
    /// Unlike the buffered call this one makes a single attempt. The
    /// `maintenance_required` recovery above works by resubmitting, and a
    /// stream is consumed by the attempt that reads it: there is no second
    /// attempt to make. A gated publish commits nothing, so rerunning the
    /// command — with the same `--commit-id` if the caller wants the retry
    /// to be idempotent — is the honest recovery.
    pub(super) async fn put_file_stream(
        &self,
        spec: &NamespacePath,
        body: ByteStream,
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        let result = self
            .writer
            .put_file_stream(
                spec.namespace(),
                spec.absolute_path().as_str(),
                body,
                options.clone(),
            )
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error));
        self.settle_background_work_after(result).await
    }

    pub(super) async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.delete_path(
                spec.namespace(),
                spec.absolute_path().as_str(),
                options.clone(),
            )
        })
        .await
    }

    pub(super) async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.create_directory(
                spec.namespace(),
                spec.absolute_path().as_str(),
                options.clone(),
            )
        })
        .await
    }

    pub(super) async fn update_attributes(
        &self,
        spec: &NamespacePath,
        options: &UpdateAttributesOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.update_attributes(
                spec.namespace(),
                spec.absolute_path().as_str(),
                options.clone(),
            )
        })
        .await
    }

    pub(super) async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &MoveOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(from.namespace(), || {
            self.writer.move_path(
                from.namespace(),
                from.absolute_path().as_str(),
                to.absolute_path().as_str(),
                options.clone(),
            )
        })
        .await
    }

    pub(super) async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &CopyOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(from.namespace(), || {
            self.writer.copy_path(
                from.namespace(),
                from.absolute_path().as_str(),
                to.absolute_path().as_str(),
                options.clone(),
            )
        })
        .await
    }

    pub(super) async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &RestoreRevisionOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.restore_file_revision(
                spec.namespace(),
                spec.absolute_path().as_str(),
                source_revision_no,
                options.clone(),
            )
        })
        .await
    }

    pub(super) async fn undelete(
        &self,
        namespace: &NamespaceId,
        path: Option<&AbsolutePath>,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        options: &UndeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(namespace, || {
            self.writer.undelete(
                namespace,
                inode_id,
                deletion_seq,
                path.map(|path| path.as_str()),
                options.clone(),
            )
        })
        .await
    }

    // The admin methods mirror the server handlers' error scoping exactly:
    // every operation addressing an existing namespace names it when the
    // runtime reports namespace_not_found. Parity keeps embedded and remote
    // outputs identical.

    pub(super) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        self.admin
            .create_checkpoint(namespace_id, CreateCheckpointOptions::from_request(request))
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    pub(super) async fn list_checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListCheckpointsResponse, BackendError> {
        let request = loonfs_api::PageRequest {
            limit: resolve_cli_page_limit(limit)?,
            cursor: cursor
                .map(|cursor| {
                    loonfs_api::decode_namespace_cursor::<CheckpointPageCursor>(
                        cursor,
                        namespace_id,
                    )
                })
                .transpose()
                .map_err(|error| {
                    BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                })?,
        };
        self.admin
            .list_checkpoints_page(namespace_id, request)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    pub(super) async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse, BackendError> {
        self.admin
            .release_checkpoint(namespace_id, checkpoint_id)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    pub(super) async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse, BackendError> {
        let plan = MaintenancePlan::from_request(request)
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))?;
        self.admin
            .maintenance_step_namespace(namespace_id, plan)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    /// Proves this profile's object store honours the contract LoonFS
    /// depends on. Store-scoped: it names no namespace and reads none, so
    /// no namespace error scoping applies.
    pub(super) async fn probe_store(&self) -> StoreProbeResponse {
        let run_id = loonfs_api::generated_id("probe");
        let report = run_store_contract_probe(self.writer.object_store().as_ref(), &run_id).await;
        store_probe_response(report)
    }

    pub(super) async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        let limit = resolve_cli_page_limit(limit)?;
        self.reader
            .list_changes(
                namespace_id,
                after_seq,
                ListChangesOptions { limit: Some(limit) },
            )
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }
}

/// How long an assignment rests before it is asserted again, unless
/// `--poll-interval-ms` says otherwise.
///
/// The runner forgets a key whose probe found it idle, which is right for a
/// namespace this process merely touched and not enough for one an operator
/// assigned: an assigned namespace must stay covered while it is quiet. So
/// the host says so again on this interval, and the runner does the rest —
/// one bounded step per key, which reads durable state, finds nothing, and
/// concludes idle when there is nothing to do. It matches the runner's own
/// reconciliation cadence: a shorter one would only ask the same question
/// sooner, and a longer one would leave a cold namespace uncovered for
/// longer than the runner's own sweep would.
const ASSIGNMENT_INTERVAL_MS: u64 = 60_000;

/// Tells the runner every assigned key may have work.
///
/// Nudges are hints and never block: what this asserts is the assignment,
/// and every step that follows re-reads durable state to find out whether
/// there was anything to it.
fn assign(maintenance: &MaintenanceHandle, jobs: &[HostedJob], namespaces: &[NamespaceId]) {
    for (job, _) in jobs {
        for namespace_id in namespaces {
            maintenance.nudge(*job, namespace_id);
        }
    }
}

/// The one timer a maintenance host owns: how long an assignment rests
/// before it is asserted again. Nothing durable depends on it — it decides
/// when to look, never what is true, which is why an operator may set it.
#[allow(clippy::disallowed_methods)]
async fn rest_between_assignments(interval_ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
}

/// Grace windows are wall-clock policy, and this is where an embedded
/// command enters wall time — the same boundary the server's HTTP handler
/// is. Nothing durable replays through it.
#[allow(clippy::disallowed_methods)]
fn current_unix_ms() -> Result<u64, BackendError> {
    let server_error =
        |message: String| BackendError::new(ErrorCode::ServerError.as_str(), message);
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| server_error(format!("system time is before unix epoch: {error}")))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|error| server_error(format!("system time does not fit in milliseconds: {error}")))
}

fn resolve_cli_page_limit(limit: Option<u32>) -> Result<EffectiveLimit, BackendError> {
    // The server maps this same policy error to `invalid_request`; embedded
    // mode must report the identical registry code for the identical failure.
    PaginationPolicy::default()
        .resolve_limit(limit)
        .map_err(|error| BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string()))
}

/// Renders a probe report as the wire shape both backends answer with, so
/// an embedded run and a remote one print the same thing.
fn store_probe_response(report: StoreProbeReport) -> StoreProbeResponse {
    StoreProbeResponse {
        run_id: report.run_id,
        checks: report
            .checks
            .into_iter()
            .map(|check| {
                let (outcome, message) = match check.outcome {
                    StoreProbeOutcome::Passed => (StoreProbeCheckOutcome::Passed, None),
                    StoreProbeOutcome::Unsupported => (StoreProbeCheckOutcome::Unsupported, None),
                    StoreProbeOutcome::Failed { message } => {
                        (StoreProbeCheckOutcome::Failed, Some(message))
                    }
                };
                StoreProbeCheckResult {
                    name: check.name.to_owned(),
                    outcome,
                    message,
                }
            })
            .collect(),
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{
        map_runtime_error, resolve_cli_page_limit, GrepError, StepBudget, GREP_GC_JOB,
        GREP_INDEX_JOB,
    };
    use crate::backend_error::map_namespace_scoped_grep_error;
    use crate::config::StoreConfig;
    use crate::resolve::EmbeddedTarget;
    use loonfs::{
        BootstrapNamespaceError, CoreError, CreateNamespaceOptions, FsBackgroundWork, FsWriter,
        MaintenanceJobId, MaintenanceStepConclusion, MetadataMaintenanceOptions, PutFileOptions,
        RuntimeError, SharedObjectStore, StatPathOptions,
    };
    use loonfs_api::{
        ChangeSeq, CreateCheckpointRequest, DestinationBehavior, ErrorCode, InodeId, NamespaceId,
        RevisionNo,
    };
    use loonfs_client::NamespacePath;
    use tempfile::tempdir;

    fn namespace_id(value: &str) -> NamespaceId {
        NamespaceId::parse(value).expect("valid namespace id")
    }

    /// WAL-tail length at which the metadata job checkpoints — what a
    /// drained namespace must be back under.
    fn checkpoint_threshold() -> u64 {
        MetadataMaintenanceOptions::default()
            .max_wal_tail_segments
            .get()
    }

    /// The four jobs `admin run` hosts by default, in the order it drives
    /// them.
    fn every_job() -> [MaintenanceJobId; 4] {
        [
            MaintenanceJobId::METADATA,
            MaintenanceJobId::GC,
            GREP_INDEX_JOB,
            GREP_GC_JOB,
        ]
    }

    /// Leaves a namespace with a metadata backlog no scheduler will touch:
    /// a `ManualOnly` writer publishes past the WAL-tail checkpoint
    /// threshold, which is exactly the state a cold namespace is in when the
    /// process that wrote it went away.
    async fn seed_wal_backlog(store: &SharedObjectStore, namespace_id: &NamespaceId) {
        const PUBLISHES_PAST_THE_CHECKPOINT_THRESHOLD: usize = 34;
        let writer = FsWriter::builder_with_store(store.clone())
            .writer_id(format!("{namespace_id}-backlog"))
            .background_work(FsBackgroundWork::ManualOnly)
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build backlog writer");
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        for index in 0..PUBLISHES_PAST_THE_CHECKPOINT_THRESHOLD {
            writer
                .put_file_bytes(
                    namespace_id,
                    &format!("/notes/note-{index}.txt"),
                    b"assigned needle\n",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .unwrap_or_else(|error| panic!("seed put {index} failed: {error}"));
        }
    }

    fn local_store(temp_dir: &std::path::Path) -> (StoreConfig, SharedObjectStore) {
        let config = StoreConfig::LocalFs {
            root: temp_dir.display().to_string(),
            key_prefix: None,
        };
        let store = config
            .configured_object_store()
            .expect("configure store")
            .into_shared();
        (config, store)
    }

    /// A drain is the assigned host's catch-up: it walks every
    /// `{job, namespace}` key it was given to a settled conclusion and does
    /// the work it finds on the way. Two namespaces here, one with an index
    /// to build and one with none at all.
    #[tokio::test]
    async fn a_drain_settles_every_assigned_key_and_does_the_work_it_finds() {
        let temp_dir = tempdir().expect("create temp dir");
        let (store_config, store) = local_store(temp_dir.path());
        let indexed = namespace_id("alpha");
        let unindexed = namespace_id("beta");
        seed_wal_backlog(&store, &indexed).await;
        seed_wal_backlog(&store, &unindexed).await;

        let target = EmbeddedTarget::new(&store_config, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .enable_grep_index(&indexed)
            .await
            .expect("enable the index without driving it");
        let head_seq = target
            .backend
            .namespace_status(&indexed)
            .await
            .expect("status before the drain")
            .head_seq;

        let progress = target
            .backend
            .drain_maintenance(
                &[indexed.clone(), unindexed.clone()],
                &every_job(),
                StepBudget::default(),
            )
            .await
            .expect("drain the assignment");

        assert!(
            !progress.budget_exhausted(),
            "an unbudgeted drain settles every key: {:?}",
            progress.keys
        );
        assert_eq!(progress.keys.len(), 8, "four jobs over two namespaces");
        assert!(progress.steps >= 8, "every key took at least one step");
        // A namespace with no grep root has nothing for that job to
        // maintain, and saying so is a settled conclusion like any other.
        let unindexed_grep = progress
            .keys
            .iter()
            .find(|key| key.job == GREP_INDEX_JOB && key.namespace_id == unindexed)
            .expect("the unindexed namespace's grep key");
        assert_eq!(
            unindexed_grep.conclusion,
            Some(MaintenanceStepConclusion::NotEnabled)
        );

        // The work is durable, not a tally: both backlogs are flushed and
        // the one index there was is at the namespace head.
        for namespace_id in [&indexed, &unindexed] {
            let status = target
                .backend
                .namespace_status(namespace_id)
                .await
                .expect("status after the drain");
            assert!(
                status.wal_tail_segments < checkpoint_threshold(),
                "`{namespace_id}` kept a WAL tail of {} segments past the checkpoint threshold",
                status.wal_tail_segments
            );
            assert!(status.current_manifest_id.is_some(), "{namespace_id}");
        }
        let indexed_status = target
            .backend
            .grep_index_status(&indexed)
            .await
            .expect("index status after the drain");
        assert!(
            indexed_status.state.is_built_through(head_seq),
            "the assigned index must reach the head it was behind: {:?}",
            indexed_status.state
        );
    }

    /// A budget that runs out mid-assignment reports where every key got to
    /// — including the ones it never reached — instead of claiming the
    /// assignment is caught up.
    #[tokio::test]
    async fn a_spent_drain_budget_reports_the_keys_it_left_unsettled() {
        let temp_dir = tempdir().expect("create temp dir");
        let (store_config, store) = local_store(temp_dir.path());
        let namespace = namespace_id("alpha");
        seed_wal_backlog(&store, &namespace).await;
        let target = EmbeddedTarget::new(&store_config, None)
            .await
            .expect("build embedded target");

        let progress = target
            .backend
            .drain_maintenance(
                std::slice::from_ref(&namespace),
                &[MaintenanceJobId::METADATA, MaintenanceJobId::GC],
                StepBudget {
                    max_steps: Some(1),
                    deadline_ms: None,
                },
            )
            .await
            .expect("drain within a budget");

        assert!(progress.budget_exhausted());
        assert_eq!(progress.steps, 1);
        let metadata = &progress.keys[0];
        assert_eq!(metadata.job, MaintenanceJobId::METADATA);
        assert_eq!(metadata.steps, 1);
        assert_eq!(
            metadata.conclusion,
            Some(MaintenanceStepConclusion::Progressed),
            "one step of a real backlog moves durable state and leaves more behind"
        );
        assert!(!metadata.settled());
        let collection = &progress.keys[1];
        assert_eq!(collection.job, MaintenanceJobId::GC);
        assert_eq!(collection.steps, 0);
        assert_eq!(
            collection.conclusion, None,
            "a key the budget never reached reports no conclusion rather than a made-up one"
        );
        assert!(!collection.settled());
    }

    /// Hosting is the other half: the runner runs the steps, the assignment
    /// is what admits them, and the signal is what stops it. Nothing here
    /// discovered the namespace — the assignment did.
    #[tokio::test]
    async fn hosting_an_assignment_maintains_a_cold_namespace_until_the_signal() {
        let temp_dir = tempdir().expect("create temp dir");
        let (store_config, store) = local_store(temp_dir.path());
        let namespace = namespace_id("alpha");
        seed_wal_backlog(&store, &namespace).await;
        let target = EmbeddedTarget::new(&store_config, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .enable_grep_index(&namespace)
            .await
            .expect("enable the index without driving it");
        let head_seq = target
            .backend
            .namespace_status(&namespace)
            .await
            .expect("status before hosting")
            .head_seq;

        // The stop signal a test can drive: the host runs until the work it
        // was assigned is observable in durable state, which is all an
        // operator watching this process would have to go on either.
        let stop = async {
            wait_until(|| async {
                target
                    .backend
                    .grep_index_status(&namespace)
                    .await
                    .expect("index status while hosting")
                    .state
                    .is_built_through(head_seq)
            })
            .await;
        };
        target
            .backend
            .host_maintenance(std::slice::from_ref(&namespace), &every_job(), None, stop)
            .await
            .expect("the host shuts down cleanly on its signal");

        // The shutdown settled what it admitted, so this is the state the
        // host left rather than a race with it.
        let status = target
            .backend
            .namespace_status(&namespace)
            .await
            .expect("status after hosting");
        assert!(
            status.wal_tail_segments < checkpoint_threshold(),
            "the hosted runner left a WAL tail of {} segments",
            status.wal_tail_segments
        );
    }

    /// Bounded observation of work the runner publishes durably and reports
    /// nothing about in-process.
    #[allow(clippy::disallowed_methods)]
    async fn wait_until<F, Fut>(condition: F)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while !condition().await {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the hosted runner never reached the state it was assigned");
    }

    #[test]
    fn map_core_error_surfaces_registry_codes_verbatim() {
        let error = map_runtime_error(RuntimeError::Core(CoreError::RevisionNotFound {
            inode_id: InodeId(42),
            revision_no: RevisionNo(7),
        }));

        assert_eq!(error.code, ErrorCode::RevisionNotFound.as_str());

        let content_id = loonfs::ContentId::generate();
        let error = map_runtime_error(RuntimeError::Core(CoreError::ContentPreparation(
            loonfs::publish::ContentPreparationError::ContentNotPrepared {
                content_id: content_id.clone(),
            },
        )));
        assert_eq!(error.code, ErrorCode::ContentNotPrepared.as_str());
        assert!(error.message.contains(content_id.as_str()));
    }

    #[test]
    fn map_grep_error_preserves_embedded_remote_code_parity() {
        for (error, expected) in [
            (GrepError::NotEnabled, ErrorCode::NotSupported),
            (
                GrepError::CorruptIndex {
                    message: "bad pointer".to_owned(),
                },
                ErrorCode::IndexCorrupt,
            ),
            (
                GrepError::PublicationConflict {
                    object_key: "namespaces/demo/extensions/grep/root.json".to_owned(),
                },
                ErrorCode::StaleHead,
            ),
        ] {
            assert_eq!(
                map_namespace_scoped_grep_error(&namespace_id("demo"), error).code,
                expected.as_str()
            );
        }
    }

    #[test]
    fn page_limit_errors_report_the_registry_code_the_server_serves() {
        // `--limit 0` fails the same PaginationPolicy check in both modes;
        // embedded mode must answer `invalid_request` like the server, not a
        // CLI-local `invalid_input` rewrite.
        let error = resolve_cli_page_limit(Some(0)).expect_err("zero limit is invalid");
        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    }

    #[test]
    fn map_core_error_does_not_rewrite_invalid_id_codes() {
        // Embedded mode must report the same code the server serves for the
        // identical failure, not a CLI-local `invalid_input` rewrite.
        let invalid_id = NamespaceId::parse("bad/name").expect_err("invalid namespace id");
        let error = map_runtime_error(RuntimeError::Core(CoreError::InvalidNamespaceId(
            invalid_id,
        )));

        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    }

    #[test]
    fn map_bootstrap_error_surfaces_registry_codes_verbatim() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let error = map_runtime_error(RuntimeError::Bootstrap(
            BootstrapNamespaceError::NamespaceAlreadyExists { namespace_id },
        ));

        assert_eq!(error.code, ErrorCode::NamespaceExists.as_str());
        assert!(error.message.contains("already exists"));
    }

    #[tokio::test]
    async fn embedded_backend_put_returns_the_commit_id_it_committed_under() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .create_namespace(&namespace_id("demo"))
            .await
            .expect("create namespace");

        let response = target
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/file.txt").expect("namespace path"),
                b"hello",
                &PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");
        assert!(!response.commit_id.as_str().trim().is_empty());

        let changes = target
            .backend
            .list_changes(&namespace_id("demo"), ChangeSeq(0), None)
            .await
            .expect("list changes");
        assert_eq!(changes.changes.len(), 1);
        assert_eq!(changes.changes[0].commit_id, response.commit_id);
    }

    #[tokio::test]
    async fn embedded_admin_methods_surface_registry_codes_for_missing_namespaces() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None)
            .await
            .expect("build embedded target");

        let checkpoint = target
            .backend
            .create_checkpoint(
                &namespace_id("missing"),
                CreateCheckpointRequest {
                    name: "nightly".to_owned(),
                    ttl_ms: None,
                },
            )
            .await
            .expect_err("checkpoint on missing namespace");
        assert_eq!(checkpoint.code, ErrorCode::NamespaceNotFound.as_str());

        let changes = target
            .backend
            .list_changes(&namespace_id("missing"), ChangeSeq(0), None)
            .await
            .expect_err("changes on missing namespace");
        assert_eq!(changes.code, ErrorCode::NamespaceNotFound.as_str());
        assert_eq!(changes.message, "namespace `missing` does not exist");
    }

    #[tokio::test]
    async fn embedded_writes_never_stall_at_the_wal_backpressure_cap() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .create_namespace(&namespace_id("demo"))
            .await
            .expect("create namespace");

        // More publishes than the WAL backpressure cap: the Enabled policy
        // must keep stepping the tail down so no write ever stalls on
        // `maintenance_required` (each stall used to require a manual
        // `loonfs admin step`).
        for index in 0..140 {
            target
                .backend
                .put_file_bytes(
                    &NamespacePath::parse("demo", &format!("/files/f{index}.txt"))
                        .expect("namespace path"),
                    b"payload",
                    &PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("put {index} failed: {} {}", error.code, error.message)
                });
        }
    }

    #[tokio::test]
    async fn embedded_writes_recover_from_preexisting_wal_debt() {
        let temp_dir = tempdir().expect("create temp dir");
        let store_config = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };

        // Accumulate WAL debt the way pre-fix builds did: a ManualOnly
        // writer publishes until the backpressure gate refuses the next
        // publish outright.
        let store = store_config
            .configured_object_store()
            .expect("configure store")
            .into_shared();
        let writer = FsWriter::builder_with_store(store)
            .writer_id("debt-builder")
            .background_work(FsBackgroundWork::ManualOnly)
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build debt writer");
        let namespace = NamespaceId::parse("demo").expect("namespace id");
        writer
            .create_namespace(&namespace, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let mut stalled = false;
        for index in 0..200 {
            let result =
                writer
                    .put_file_bytes(
                        &namespace,
                        &format!("/files/f{index}.txt"),
                        b"payload",
                        PutFileOptions {
                            behavior: DestinationBehavior::NoReplace,
                            commit: loonfs_client::CommitOptions::new(
                                loonfs_test_support::test_actor(),
                            ),
                            expected_revision_no: None,
                        },
                    )
                    .await;
            match result {
                Ok(_) => {}
                Err(RuntimeError::Core(error))
                    if matches!(error.code(), ErrorCode::MaintenanceRequired) =>
                {
                    stalled = true;
                    break;
                }
                Err(error) => panic!("unexpected stall error: {error}"),
            }
        }
        assert!(stalled, "ManualOnly writer never hit the backpressure cap");

        // The embedded backend digs itself out: the gated publish schedules
        // its own step, the backend settles it and resubmits.
        let target = EmbeddedTarget::new(&store_config, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/recovered.txt").expect("namespace path"),
                b"payload",
                &PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("recovery put failed: {} {}", error.code, error.message)
            });
    }
    #[tokio::test]
    async fn a_fenced_put_fails_terminally_and_names_both_epochs() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        // Two backends over one store model two concurrent `loonfs` processes:
        // the writer id is shared (the CLI defaults it to the hostname), so
        // the epochs are what tell the two apart in the fence.
        let first = EmbeddedTarget::new(&store, Some("shared-host"))
            .await
            .expect("build first embedded target");
        first
            .backend
            .create_namespace(&namespace_id("demo"))
            .await
            .expect("create namespace");
        first
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/one.txt").expect("namespace path"),
                b"one",
                &PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("first put acquires the epoch");

        let rival = EmbeddedTarget::new(&store, Some("shared-host"))
            .await
            .expect("build rival embedded target");
        rival
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/two.txt").expect("namespace path"),
                b"two",
                &PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("rival put takes the epoch over");

        // Fenced sessions are terminal — no silent reacquisition, matching
        // remote mode and the core contract. Both writers share one label
        // here (`shared-host`, as two CLI runs on one machine would), so the
        // message leans on the epochs and the winner's acquisition stamp to
        // stay diagnosable. The failed put committed nothing.
        let error = first
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/three.txt").expect("namespace path"),
                b"three",
                &PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect_err("a fenced session is terminal");
        assert_eq!(error.code, ErrorCode::WriterFenced.as_str());
        assert!(
            error.message.contains("was fenced by epoch"),
            "{}",
            error.message
        );
        assert!(
            error
                .message
                .contains("(writer `shared-host`, acquired at "),
            "the winner is named with its acquisition stamp: {}",
            error.message
        );

        let missing = rival
            .backend
            .stat_path(
                &NamespacePath::parse("demo", "/three.txt").expect("namespace path"),
                &StatPathOptions::default(),
            )
            .await
            .expect_err("the fenced put committed nothing");
        assert_eq!(missing.code, ErrorCode::PathNotFound.as_str());
    }
}
