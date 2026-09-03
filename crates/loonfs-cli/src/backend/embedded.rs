//! Embedded CLI backend implemented by an in-process `loonfs` runtime.
//!
//! It implements the same operations as the remote backend.

use crate::backend_error::{map_namespace_scoped_grep_error, map_runtime_error, NamespaceScoped};
use crate::error::CliError;
use crate::render::write_stderr_warning;
use loonfs::{
    ByteStream, CheckpointPageCursor, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, CreateSnapshotOptions, DeleteNamespaceOptions, DeleteNamespaceResponse,
    DeleteOptions, FsMaintenance, FsReader, FsWriter,
    ListChangesOptions as RuntimeListChangesOptions, ListChangesResponse, ListPathEntriesOptions,
    MaintenanceAssignment, MaintenanceCancellation, MaintenanceConclusion, MaintenanceHandle,
    MaintenanceJob, MaintenanceJobId, MaintenanceRegistry, MaintenanceRunner, MoveOptions,
    PutFileOptions, RestoreRevisionOptions, RuntimeError, SharedObjectStore, StatPathOptions,
    UndeleteOptions, UpdateAttributesOptions,
};
use loonfs_api::{
    v0::{
        GrepGcRequest, GrepGcResponse, GrepIndex, GrepIndexLifecycle, ListSnapshotsResponse,
        SnapshotSummary, StoreProbeCheckOutcome, StoreProbeCheckResult, StoreProbeResponse,
    },
    AbsolutePath, ChangeSeq, Checkpoint, CheckpointId, CommitResponse, CreateCheckpointRequest,
    EffectiveLimit, ErrorCode, GrepRequest, GrepResponse, InodeId, ListCheckpointsResponse,
    ListFileRevisionsResponse, ListPathEntriesResponse, ListTrashResponse, MaintenanceRunRequest,
    MaintenanceRunResponse, Namespace, NamespaceId, PaginationPolicy, PathEntry, RevisionNo,
};
use loonfs_client::{NamespacePath, ReadFileOptions};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBlockCache, GrepDisableOutcome, GrepEnableOutcome, GrepError,
    GrepMaintenanceJob, GrepService, GrepWorker, NamespaceReads,
};
use loonfs_objectstore::probe::{run_store_contract_probe, StoreProbeOutcome, StoreProbeReport};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::sync::Arc;

use super::step_budget::{wait_for_grep_index, GrepWaitStep};
use super::{GrepWaitProgress, MaintenanceDrainProgress, MaintenanceKeyProgress, StepBudget};

/// Purpose-specific handles over one shared store client: reads go through
/// the reader, mutations through the writer, and maintenance through the
/// maintenance handle. A local runner receives bounded publication hints, and
/// every mutation settles admitted work before the one-shot process exits. A
/// publish gated on `maintenance_required` waits for metadata maintenance,
/// then resubmits, so embedded writes recover from WAL debt
/// instead of hard-stopping. `loonfs maintenance` commands remain the explicit path
/// for everything else (GC, retention, forced steps).
pub(crate) struct EmbeddedBackend {
    pub(crate) writer: FsWriter,
    pub(crate) reader: FsReader,
    pub(crate) maintenance: FsMaintenance,
    pub(crate) jobs: MaintenanceRegistry,
    pub(crate) runner: MaintenanceRunner,
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

const EMBEDDED_SNAPSHOT_MAX_TTL_MS: u64 = 86_400_000;
const EMBEDDED_SNAPSHOT_MAX_LIFETIME_MS: u64 = 604_800_000;
const EMBEDDED_SNAPSHOT_MAX_LIVE_PER_NAMESPACE: usize = 16;

/// A selected maintenance job and its executor.
type HostedJob = (MaintenanceJobId, Arc<dyn MaintenanceJob>);

impl EmbeddedBackend {
    /// Waits out locally scheduled maintenance so a one-shot command never
    /// exits (tearing down the runtime) while a step is mid-flight. A settle
    /// failure after a committed mutation is reported as a warning on
    /// stderr, never as the mutation's outcome — the commit landed.
    async fn settle_background_work_after<T>(
        &self,
        result: Result<T, CliError>,
    ) -> Result<T, CliError> {
        match (result, self.runner.drain().await) {
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
    /// publish observes the oversized WAL tail and emits a maintenance hint,
    /// so settle the local runner and resubmit. A gated attempt commits
    /// nothing, so the resubmission cannot double-apply.
    async fn publish_with_maintenance_recovery<T, F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        attempt: F,
    ) -> Result<T, CliError>
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
                .wait_for_fold(namespace_id)
                .await
                .map_err(map_runtime_error)?;
            self.runner.drain().await.map_err(map_runtime_error)?;
            result = attempt().await;
        }
        let result = result.scoped(namespace_id);
        self.settle_background_work_after(result).await
    }

    /// A grep worker over this backend's own handles: grep's keyspace rides
    /// the writer's store client, its filesystem reads the reader, and its
    /// backfill checkpoints the maintenance handle.
    pub(super) fn grep_worker(&self) -> GrepWorker<SharedObjectStore> {
        GrepWorker::with_block_cache(
            self.writer.object_store(),
            self.reader.clone(),
            self.maintenance.clone(),
            Arc::clone(&self.grep_block_cache),
        )
    }

    pub(super) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Namespace, CliError> {
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
    ) -> Result<DeleteNamespaceResponse, CliError> {
        let options = DeleteNamespaceOptions { expected_head_seq };
        let result = self
            .writer
            .delete_namespace(namespace_id, options)
            .await
            .scoped(namespace_id);
        self.settle_background_work_after(result).await
    }

    pub(super) async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<Namespace, CliError> {
        let result = self
            .writer
            .fork_namespace(source_namespace_id, new_namespace_id)
            .await
            .scoped(source_namespace_id);
        self.settle_background_work_after(result).await
    }

    pub(super) async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
        snapshot_id: Option<&CheckpointId>,
    ) -> Result<ListPathEntriesResponse, CliError> {
        let request = cli_page_request(limit, cursor)?;
        if let Some(snapshot_id) = snapshot_id {
            let snapshot = self
                .reader
                .pin_namespace_at_snapshot(spec.namespace(), snapshot_id)
                .await
                .scoped(spec.namespace())?;
            return snapshot
                .list_path_entries_page(
                    spec.absolute_path().as_str(),
                    request,
                    ListPathEntriesOptions::default(),
                )
                .await
                .scoped(spec.namespace());
        }
        self.reader
            .list_path_entries_page(
                spec.namespace(),
                spec.absolute_path().as_str(),
                request,
                ListPathEntriesOptions::default(),
            )
            .await
            .scoped(spec.namespace())
    }

    pub(super) async fn get_path_entry(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<PathEntry, CliError> {
        if let Some(snapshot_id) = &options.snapshot_id {
            let snapshot = self
                .reader
                .pin_namespace_at_snapshot(spec.namespace(), snapshot_id)
                .await
                .scoped(spec.namespace())?;
            let mut options = options.clone();
            options.snapshot_id = None;
            return snapshot
                .get_path_entry(spec.absolute_path().as_str(), options)
                .await
                .scoped(spec.namespace());
        }
        self.reader
            .get_path_entry(
                spec.namespace(),
                spec.absolute_path().as_str(),
                options.clone(),
            )
            .await
            .scoped(spec.namespace())
    }

    pub(super) async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, CliError> {
        let result = self
            .reader
            .get_file_bytes(spec.namespace(), spec.absolute_path().as_str())
            .await
            .scoped(spec.namespace())?;
        Ok(result.bytes)
    }

    pub(super) async fn get_file_bytes_with_options(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>, CliError> {
        if options.revision_no.is_some() && options.snapshot_id.is_some() {
            return Err(CliError::invalid_request(
                "revision_no cannot be combined with snapshot_id",
            )
            .with_param("revision_no"));
        }
        if let Some(snapshot_id) = &options.snapshot_id {
            let snapshot = self
                .reader
                .pin_namespace_at_snapshot(spec.namespace(), snapshot_id)
                .await
                .scoped(spec.namespace())?;
            return snapshot
                .get_file_bytes(spec.absolute_path().as_str())
                .await
                .map(|file| file.bytes)
                .scoped(spec.namespace());
        }
        match options.revision_no {
            Some(revision_no) => self.get_file_revision_bytes(spec, revision_no).await,
            None => self.get_file_bytes(spec).await,
        }
    }

    pub(super) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
        limit: Option<u32>,
    ) -> Result<GrepResponse, CliError> {
        let store = self.writer.object_store();
        let reads = NamespaceReads::new(&self.reader, namespace_id);
        self.grep
            .query(request, resolve_cli_page_limit(limit)?, &reads, &store)
            .await
            .scoped(namespace_id)
    }

    pub(super) async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, CliError> {
        match self
            .grep_worker()
            .enable(namespace_id)
            .await
            .scoped(namespace_id)?
        {
            GrepEnableOutcome::Enabled { .. } | GrepEnableOutcome::AlreadyEnabled { .. } => {}
            GrepEnableOutcome::Superseded => {
                return Err(map_namespace_scoped_grep_error(
                    namespace_id,
                    GrepError::PublicationConflict {
                        object_key: loonfs_grep::keyspace::root_key(namespace_id),
                    },
                ));
            }
        }
        // Enabling is one compare-and-swap and nothing else, here as on a
        // server. Driving the backfill afterwards is the command's job, not
        // this call's, so an embedded caller and a remote one get the same
        // answer to the same question.
        self.grep_worker()
            .get_grep_index(namespace_id)
            .await
            .scoped(namespace_id)
    }

    pub(super) async fn gc_grep_index(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepGcRequest,
    ) -> Result<GrepGcResponse, CliError> {
        let report = self
            .grep_worker()
            .garbage_collect_namespace(
                namespace_id,
                current_unix_ms()?,
                &loonfs_grep::GrepGcOptions {
                    max_objects: request.max_objects,
                    cursor: request.cursor.clone(),
                },
            )
            .await
            .scoped(namespace_id)?;
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
    ) -> Result<GrepWaitProgress, CliError> {
        let worker = self.grep_worker();
        let job = GrepMaintenanceJob::new(worker.clone(), GramIndexBuildPolicy::default());
        wait_for_grep_index(
            target_seq,
            budget,
            || async {
                let status = worker.lifecycle(namespace_id).await.scoped(namespace_id)?;
                Ok(GrepIndexLifecycle::from(&status))
            },
            || async {
                let conclusion = job
                    .run(namespace_id, None, &MaintenanceCancellation::new())
                    .await
                    .scoped(namespace_id)?
                    .conclusion;
                Ok(match conclusion {
                    MaintenanceConclusion::Progressed | MaintenanceConclusion::Superseded => {
                        GrepWaitStep::Continue
                    }
                    MaintenanceConclusion::Idle
                    | MaintenanceConclusion::Blocked
                    | MaintenanceConclusion::NotEnabled => GrepWaitStep::Settled,
                })
            },
        )
        .await
    }

    /// Resolves every selected job from the registry.
    fn hosted_jobs(&self, jobs: &[MaintenanceJobId]) -> Result<Vec<HostedJob>, CliError> {
        jobs.iter()
            .map(|job| {
                let executor = self.jobs.get(*job).ok_or_else(|| {
                    CliError::runtime_error(format!(
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
    /// signal ends the assignment, then the writer and runner shut down.
    pub(super) async fn host_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        poll_interval_ms: Option<u64>,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), CliError> {
        let hosted = self.hosted_jobs(jobs)?;
        let interval_ms = poll_interval_ms.unwrap_or(ASSIGNMENT_INTERVAL_MS);
        let maintenance = self.runner.handle();
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
        let writer = self.writer.shutdown().await;
        let runner = self.runner.shutdown().await;
        writer.and(runner).map_err(map_runtime_error)
    }

    /// Runs every `{job, namespace}` key to a settled conclusion, or until
    /// `budget` runs out.
    ///
    /// A drain hosts the steps itself rather than nudging the runner: it has
    /// a budget to spend and per-key progress to report, and admission
    /// offers neither. It shuts the runner down first so a second scheduler
    /// cannot race these steps, then closes the writer and walks the
    /// assignment. Each key's continuation passes from one run to the next.
    pub(super) async fn drain_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        budget: StepBudget,
    ) -> Result<MaintenanceDrainProgress, CliError> {
        let hosted = self.hosted_jobs(jobs)?;
        self.runner.shutdown().await.map_err(map_runtime_error)?;
        self.writer.shutdown().await.map_err(map_runtime_error)?;
        let timer = StdMonotonicTimer::default();
        let started_ms = timer.monotonic_now_ms();
        let mut steps = 0;
        let mut keys = Vec::with_capacity(hosted.len() * namespaces.len());
        for (job, _executor) in &hosted {
            for namespace_id in namespaces {
                let mut key = MaintenanceKeyProgress {
                    job: *job,
                    namespace_id: namespace_id.clone(),
                    steps: 0,
                    conclusion: None,
                };
                let mut continuation = None;
                while !budget.spent(steps, timer.monotonic_now_ms().saturating_sub(started_ms)) {
                    let result = self
                        .jobs
                        .execute(MaintenanceAssignment {
                            namespace_id: namespace_id.clone(),
                            job: *job,
                            continuation,
                        })
                        .await
                        .scoped(namespace_id)?;
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
    ) -> Result<GrepIndex, CliError> {
        match self
            .grep_worker()
            .disable(namespace_id)
            .await
            .scoped(namespace_id)?
        {
            GrepDisableOutcome::Disabled | GrepDisableOutcome::NotEnabled => self
                .grep_worker()
                .get_grep_index(namespace_id)
                .await
                .scoped(namespace_id),
            GrepDisableOutcome::Superseded => Err(map_namespace_scoped_grep_error(
                namespace_id,
                GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(namespace_id),
                },
            )),
        }
    }

    pub(super) async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, CliError> {
        let result = self
            .reader
            .get_file_revision_bytes(spec.namespace(), spec.absolute_path().as_str(), revision_no)
            .await
            .scoped(spec.namespace())?;
        Ok(result.bytes)
    }

    pub(super) async fn list_trash(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse, CliError> {
        let request = cli_page_request(limit, cursor)?;
        self.reader
            .list_trash_page(namespace_id, request)
            .await
            .scoped(namespace_id)
    }

    pub(super) async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, CliError> {
        let request = cli_page_request(limit, cursor)?;
        self.reader
            .list_file_revisions_page(spec.namespace(), spec.absolute_path().as_str(), request)
            .await
            .scoped(spec.namespace())
    }

    pub(super) async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
        let result = self
            .writer
            .put_file_stream(
                spec.namespace(),
                spec.absolute_path().as_str(),
                body,
                options.clone(),
            )
            .await
            .scoped(spec.namespace());
        self.settle_background_work_after(result).await
    }

    pub(super) async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        path: Option<&AbsolutePath>,
        options: &UndeleteOptions,
    ) -> Result<CommitResponse, CliError> {
        self.publish_with_maintenance_recovery(namespace_id, || {
            self.writer.undelete(
                namespace_id,
                inode_id,
                deletion_seq,
                path.map(|path| path.as_str()),
                options.clone(),
            )
        })
        .await
    }

    pub(super) async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary, CliError> {
        let now_ms = validate_embedded_snapshot_ttl(namespace_id, ttl_ms)?;
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        let checkpoint = self
            .writer
            .create_snapshot_with_quota(
                namespace_id,
                CreateSnapshotOptions {
                    name: name.to_owned(),
                    expires_at_ms,
                },
                now_ms,
                EMBEDDED_SNAPSHOT_MAX_LIVE_PER_NAMESPACE,
            )
            .await
            .scoped(namespace_id)
            .map_err(|error| error.with_invalid_request_param("/name"))?;
        SnapshotSummary::from_checkpoint(checkpoint).ok_or_else(|| {
            CliError::runtime_error("snapshot creation returned a non-snapshot checkpoint")
        })
    }

    pub(super) async fn list_snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListSnapshotsResponse, CliError> {
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
                    CliError::invalid_request(error.to_string()).with_param("cursor")
                })?,
        };
        self.reader
            .list_snapshots_page(namespace_id, request)
            .await
            .scoped(namespace_id)
    }

    pub(super) async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary, CliError> {
        let now_ms = validate_embedded_snapshot_ttl(namespace_id, ttl_ms)?;
        self.writer
            .extend_snapshot(
                namespace_id,
                snapshot_id,
                now_ms.saturating_add(ttl_ms),
                EMBEDDED_SNAPSHOT_MAX_LIFETIME_MS,
            )
            .await
            .scoped(namespace_id)
    }

    // The maintenance methods mirror the server handlers' error scoping exactly:
    // every operation addressing an existing namespace names it when the
    // runtime reports namespace_not_found. Parity keeps embedded and remote
    // outputs identical.

    pub(super) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<Checkpoint, CliError> {
        self.maintenance
            .create_checkpoint(namespace_id, CreateCheckpointOptions::from_request(request))
            .await
            .scoped(namespace_id)
            .map_err(|error| error.with_invalid_request_param("/name"))
    }

    pub(super) async fn list_checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListCheckpointsResponse, CliError> {
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
                    CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                })?,
        };
        self.maintenance
            .list_checkpoints_page(namespace_id, request)
            .await
            .scoped(namespace_id)
    }

    pub(super) async fn run_maintenance(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceRunRequest,
    ) -> Result<MaintenanceRunResponse, CliError> {
        let result = self
            .maintenance
            .run_maintenance(namespace_id, request)
            .await;
        let invalid_threshold = matches!(&result, Err(RuntimeError::Config(_)));
        let result = result.scoped(namespace_id);
        if invalid_threshold {
            result.map_err(|error| error.with_invalid_request_param("/max_wal_tail_segments"))
        } else {
            result
        }
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
        snapshot_id: Option<&CheckpointId>,
    ) -> Result<ListChangesResponse, CliError> {
        let limit = resolve_cli_page_limit(limit)?;
        let captured_seq = match snapshot_id {
            Some(snapshot_id) => Some(
                self.reader
                    .pin_namespace_at_snapshot(namespace_id, snapshot_id)
                    .await
                    .scoped(namespace_id)?
                    .head_seq(),
            ),
            None => None,
        };
        if let Some(captured_seq) = captured_seq {
            if after_seq > captured_seq {
                return Err(CliError::invalid_request(format!(
                    "after_seq `{after_seq}` is above snapshot sequence `{captured_seq}`"
                ))
                .with_param("after_seq"));
            }
            if after_seq == captured_seq {
                return Ok(ListChangesResponse {
                    namespace_id: namespace_id.clone(),
                    after_seq,
                    through_seq: captured_seq,
                    next_after_seq: None,
                    changes: Vec::new(),
                });
            }
        }
        let mut page = self
            .reader
            .list_changes(
                namespace_id,
                after_seq,
                RuntimeListChangesOptions { limit: Some(limit) },
            )
            .await
            .scoped(namespace_id)?;
        if let Some(captured_seq) = captured_seq {
            page.changes
                .retain(|change| change.committed_seq <= captured_seq);
            page.through_seq = captured_seq;
            page.next_after_seq = page
                .changes
                .last()
                .map(|change| change.committed_seq)
                .filter(|last_seq| *last_seq < captured_seq);
        }
        Ok(page)
    }
}

fn validate_embedded_snapshot_ttl(
    namespace_id: &NamespaceId,
    ttl_ms: u64,
) -> Result<u64, CliError> {
    if ttl_ms == 0 || ttl_ms > EMBEDDED_SNAPSHOT_MAX_TTL_MS {
        return Err(CliError::invalid_request(format!(
            "ttl_ms must be greater than zero and may not exceed the `snapshot.max_ttl_ms` limit \
             of {EMBEDDED_SNAPSHOT_MAX_TTL_MS} milliseconds"
        ))
        .with_param("/ttl_ms"));
    }
    if ttl_ms > EMBEDDED_SNAPSHOT_MAX_LIFETIME_MS {
        return Err(CliError::invalid_request(format!(
            "ttl_ms may not exceed the `snapshot.max_lifetime_ms` limit of \
             {EMBEDDED_SNAPSHOT_MAX_LIFETIME_MS} milliseconds"
        ))
        .with_param("/ttl_ms"));
    }
    loonfs::current_time_ms()
        .map_err(RuntimeError::Core)
        .scoped(namespace_id)
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
fn current_unix_ms() -> Result<u64, CliError> {
    let server_error = |message: String| CliError::new(ErrorCode::ServerError.as_str(), message);
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| server_error(format!("system time is before unix epoch: {error}")))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|error| server_error(format!("system time does not fit in milliseconds: {error}")))
}

fn resolve_cli_page_limit(limit: Option<u32>) -> Result<EffectiveLimit, CliError> {
    // Embedded and remote modes use the same error code for an invalid limit.
    PaginationPolicy::default()
        .resolve_limit(limit)
        .map_err(|error| {
            CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string()).with_param("limit")
        })
}

fn cli_page_request<C: loonfs_api::PageCursor>(
    limit: Option<u32>,
    cursor: Option<&str>,
) -> Result<loonfs_api::PageRequest<C>, CliError> {
    Ok(loonfs_api::PageRequest {
        limit: resolve_cli_page_limit(limit)?,
        cursor: cursor
            .map(loonfs_api::decode_cursor)
            .transpose()
            .map_err(|error| {
                CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                    .with_param("cursor")
            })?,
    })
}

/// Converts a store probe report to the response returned by both backends.
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
        cli_page_request, map_runtime_error, resolve_cli_page_limit, GrepError, StepBudget,
    };
    use crate::backend_error::map_namespace_scoped_grep_error;
    use crate::config::StoreConfig;
    use crate::resolve::EmbeddedTarget;
    use loonfs::{
        BootstrapNamespaceError, CoreError, CreateNamespaceOptions, FsWriter,
        MaintenanceConclusion, MaintenanceJobId, MetadataMaintenanceOptions, PutFileOptions,
        RuntimeError, SharedObjectStore, StatPathOptions,
    };
    use loonfs_api::{
        ChangeSeq, CreateCheckpointRequest, ErrorCode, InodeId, NamespaceId, RevisionNo,
    };
    use loonfs_client::NamespacePath;
    use loonfs_core::test_support::append_wal_segments;
    use loonfs_core::MutationContext;
    use loonfs_grep::{GREP_GC_JOB, GREP_INDEX_JOB};
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

    /// Jobs selected when `maintenance run` omits `--job`.
    fn every_job() -> [MaintenanceJobId; 5] {
        [
            MaintenanceJobId::METADATA,
            MaintenanceJobId::METADATA_COMPACTION,
            MaintenanceJobId::GC,
            GREP_INDEX_JOB,
            GREP_GC_JOB,
        ]
    }

    async fn seed_wal_backlog(store: &SharedObjectStore, namespace_id: &NamespaceId) {
        const PUBLISHES_PAST_THE_CHECKPOINT_THRESHOLD: u64 = 34;
        let writer = FsWriter::builder_with_store(store.clone())
            .writer_id(format!("{namespace_id}-backlog"))
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build backlog writer");
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        append_wal_segments(
            store.as_ref(),
            namespace_id,
            PUBLISHES_PAST_THE_CHECKPOINT_THRESHOLD,
            &MutationContext {
                writer_id: loonfs_api::WriterId::parse(format!("{namespace_id}-tail"))
                    .expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed WAL backlog");
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
            .reader
            .get_namespace(&indexed)
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
        assert_eq!(progress.keys.len(), 10, "five jobs over two namespaces");
        assert!(progress.steps >= 10, "every key took at least one step");
        // A namespace with no grep root has nothing for that job to
        // maintain, and saying so is a settled conclusion like any other.
        let unindexed_grep = progress
            .keys
            .iter()
            .find(|key| key.job == GREP_INDEX_JOB && key.namespace_id == unindexed)
            .expect("the unindexed namespace's grep key");
        assert_eq!(
            unindexed_grep.conclusion,
            Some(MaintenanceConclusion::NotEnabled)
        );

        // The work is durable, not a tally: both backlogs are flushed and
        // the one index there was is at the namespace head.
        for namespace_id in [&indexed, &unindexed] {
            let status = target
                .backend
                .maintenance
                .get_namespace_diagnostics(namespace_id)
                .await
                .expect("diagnostics after the drain");
            assert!(
                status.wal_tail_segments < checkpoint_threshold(),
                "`{namespace_id}` kept a WAL tail of {} segments past the checkpoint threshold",
                status.wal_tail_segments
            );
            assert!(status.current_manifest_no.is_some(), "{namespace_id}");
        }
        let indexed_status = target
            .backend
            .grep_worker()
            .get_grep_index(&indexed)
            .await
            .expect("index status after the drain");
        assert!(
            indexed_status.lifecycle.is_built_through(head_seq),
            "the assigned index must reach the head it was behind: {:?}",
            indexed_status.lifecycle
        );
    }

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
            Some(MaintenanceConclusion::Progressed),
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
            .reader
            .get_namespace(&namespace)
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
                    .grep_worker()
                    .get_grep_index(&namespace)
                    .await
                    .expect("index status while hosting")
                    .lifecycle
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
            .maintenance
            .get_namespace_diagnostics(&namespace)
            .await
            .expect("diagnostics after hosting");
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
        let error = resolve_cli_page_limit(Some(0)).expect_err("zero limit is invalid");
        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
        assert_eq!(error.param.as_deref(), Some("limit"));

        let error = cli_page_request::<loonfs_api::DirectoryPageCursor>(None, Some("not-a-cursor"))
            .expect_err("malformed cursor is invalid");
        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
        assert_eq!(error.param.as_deref(), Some("cursor"));
    }

    #[test]
    fn map_core_error_preserves_invalid_request_codes() {
        let error = map_runtime_error(RuntimeError::Core(CoreError::InvalidPath(
            "bad/path".to_owned(),
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
            .list_changes(&namespace_id("demo"), ChangeSeq(0), None, None)
            .await
            .expect("list changes");
        assert_eq!(changes.changes.len(), 1);
        assert_eq!(changes.changes[0].commit_id, response.commit_id);
    }

    #[tokio::test]
    async fn embedded_maintenance_methods_surface_registry_codes_for_missing_namespaces() {
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
            .list_changes(&namespace_id("missing"), ChangeSeq(0), None, None)
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
        // `loonfs maintenance step`).
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

        let store = store_config
            .configured_object_store()
            .expect("configure store")
            .into_shared();
        let writer = FsWriter::builder_with_store(store.clone())
            .writer_id("debt-builder")
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build debt writer");
        let namespace = NamespaceId::parse("demo").expect("namespace id");
        writer
            .create_namespace(&namespace, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        append_wal_segments(
            store.as_ref(),
            &namespace,
            loonfs_core::limits::MAX_UNFLUSHED_WAL_SEGMENTS,
            &MutationContext {
                writer_id: loonfs_api::WriterId::parse("debt-builder").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed WAL debt at the write-stop bound");

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
            .get_path_entry(
                &NamespacePath::parse("demo", "/three.txt").expect("namespace path"),
                &StatPathOptions::default(),
            )
            .await
            .expect_err("the fenced put committed nothing");
        assert_eq!(missing.code, ErrorCode::PathNotFound.as_str());
    }
}
