//! Dispatches each resolved operation to its embedded or remote implementation.

use super::progress::rest_between_status_checks;
use super::{FileDownload, GrepWaitProgress, MaintenanceDrainProgress, StepBudget};
use crate::backend_error::{map_namespace_scoped_runtime_error, BackendError};
use crate::payload::LocalPayload;
use crate::progress::ProgressReporter;
use crate::resolve::ResolvedTarget;
use crate::uploads::UploadJournal;
use loonfs::MaintenanceJobId;
use loonfs_api::{
    v0::{
        GrepGcRequest, GrepGcResponse, GrepIndex, ListChangesResponse, StoreProbeRequest,
        StoreProbeResponse, UploadSession,
    },
    AbsolutePath, CapabilityDocument, ChangeSeq, Checkpoint, CheckpointId, CommitResponse,
    ContentRef, CreateCheckpointRequest, DeleteNamespaceResponse, GrepRequest, GrepResponse,
    InodeId, ListCheckpointsResponse, ListFileRevisionsResponse, ListPathEntriesResponse,
    ListTrashResponse, MaintenanceStepRequest, MaintenanceStepResponse, Namespace, NamespaceId,
    PathEntry, ReleaseCheckpointResponse, RevisionNo, UploadId,
};
use loonfs_client::{
    ClientError, CopyOptions, CreateDirectoryOptions, DeleteOptions, ListPathEntriesOptions,
    MoveOptions, NamespacePath, PutFileOptions, RestoreRevisionOptions, StatPathOptions,
    UndeleteOptions, UpdateAttributesOptions,
};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::sync::Arc;

/// Returns errors for commands that require a different profile type.
///
/// Remote profiles cannot host local maintenance because the server already
/// schedules it. Embedded profiles do not expose upload sessions because
/// they stage content directly in-process.
fn upload_sessions_need_a_remote_profile() -> BackendError {
    BackendError::new(
        loonfs_api::ErrorCode::NotSupported.as_str(),
        "upload sessions belong to a server; an embedded profile stages content itself",
    )
}

fn maintenance_host_needs_an_embedded_profile() -> BackendError {
    BackendError::new(
        loonfs_api::ErrorCode::NotSupported.as_str(),
        "`admin maintenance run` requires an embedded profile because remote servers run their \
         own maintenance; use `loonfs admin maintenance step` for one pass or `loonfs admin \
         index status` to inspect the index",
    )
}

/// Directory pager for embedded and remote profiles.
pub(crate) enum PathEntriesPager {
    Embedded {
        pager: loonfs::PathEntriesPager,
        namespace_id: NamespaceId,
    },
    Remote(loonfs_client::PathEntriesPager),
}

impl PathEntriesPager {
    /// Returns the next page with its metadata.
    pub(crate) async fn next(&mut self) -> Option<Result<ListPathEntriesResponse, BackendError>> {
        match self {
            Self::Embedded {
                pager,
                namespace_id,
            } => pager.next().await.map(|result| {
                result.map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
            }),
            Self::Remote(pager) => pager.next().await.map(|result| result.map_err(Into::into)),
        }
    }
}

/// Implements CLI operations for embedded and remote targets.
///
/// Each method exhaustively matches both variants, so adding an operation or
/// target requires handling both transports. Both paths normalize failures to
/// the same error-code registry, which keeps command output consistent.
impl ResolvedTarget {
    /// Returns the capabilities reported by the selected deployment.
    pub(crate) async fn capabilities(&self) -> Result<CapabilityDocument, BackendError> {
        match self {
            Self::Embedded(target) => Ok(target.backend.reader.capabilities()),
            Self::Remote(target) => Ok(target.client.capabilities().await?),
        }
    }

    /// Checks whether the client can reach the remote health endpoint.
    ///
    /// Any API response means the connection succeeded. A separate check
    /// determines whether the health endpoint returned a successful status.
    pub(crate) async fn remote_connectivity(&self) -> Result<(), BackendError> {
        match self {
            Self::Embedded(_) => Ok(()),
            Self::Remote(target) => match target.client.health().await {
                Ok(()) | Err(ClientError::Api { .. }) => Ok(()),
                Err(error) => Err(error.into()),
            },
        }
    }

    /// Checks the remote server's public health endpoint.
    pub(crate) async fn remote_health(&self) -> Result<(), BackendError> {
        match self {
            Self::Embedded(_) => Ok(()),
            Self::Remote(target) => Ok(target.client.health().await?),
        }
    }

    /// Creates a new empty namespace.
    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Namespace, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.create_namespace(namespace_id).await,
            Self::Remote(target) => Ok(target.client.create_namespace(namespace_id).await?),
        }
    }

    /// Marks a namespace deleted; `expected_head_seq` guards against deleting
    /// a namespace that moved since the caller last observed it.
    pub(crate) async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .delete_namespace(namespace_id, expected_head_seq)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .delete_namespace(namespace_id, expected_head_seq)
                .await?),
        }
    }

    /// Creates a new namespace as a fork of the source's durable view.
    pub(crate) async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<Namespace, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .fork_namespace(source_namespace_id, new_namespace_id)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .fork_namespace(source_namespace_id, new_namespace_id)
                .await?),
        }
    }

    /// Summarizes a namespace's current head state.
    pub(crate) async fn get_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Namespace, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.get_namespace(namespace_id).await,
            Self::Remote(target) => Ok(target.client.get_namespace(namespace_id).await?),
        }
    }

    /// Creates a directory pager that fetches pages as needed.
    pub(crate) fn list_path_entries_pager(
        &self,
        spec: &NamespacePath,
        page_size: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<PathEntriesPager, BackendError> {
        match self {
            Self::Embedded(target) => Ok(PathEntriesPager::Embedded {
                pager: target
                    .backend
                    .list_path_entries_pager(spec, page_size, cursor)?,
                namespace_id: spec.namespace().clone(),
            }),
            Self::Remote(target) => Ok(PathEntriesPager::Remote(
                target.client.list_path_entries_pager(
                    spec,
                    page_size,
                    cursor.map(ToOwned::to_owned),
                    &ListPathEntriesOptions::default(),
                ),
            )),
        }
    }

    /// Lists one page of a directory, for callers that bound their output.
    pub(crate) async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListPathEntriesResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_path_entries_page(spec, limit, cursor)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_path_entries_page(spec, limit, cursor, &ListPathEntriesOptions::default())
                .await?),
        }
    }

    /// Describes a single path entry, attributes included.
    pub(crate) async fn stat_path(&self, spec: &NamespacePath) -> Result<PathEntry, BackendError> {
        self.stat_path_projected(spec, &StatPathOptions::default())
            .await
    }

    /// Describes one visible inode, including its attributes.
    pub(crate) async fn stat_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<PathEntry, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .stat_inode(namespace_id, inode_id, &StatPathOptions::default())
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .stat_inode(namespace_id, inode_id, &StatPathOptions::default())
                .await?),
        }
    }

    /// Describes a path without loading its attributes.
    pub(crate) async fn stat_path_without_attributes(
        &self,
        spec: &NamespacePath,
    ) -> Result<PathEntry, BackendError> {
        self.stat_path_projected(
            spec,
            &StatPathOptions {
                include_attributes: false,
            },
        )
        .await
    }

    async fn stat_path_projected(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<PathEntry, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.stat_path(spec, options).await,
            Self::Remote(target) => Ok(target.client.stat_path(spec, options).await?),
        }
    }

    /// Reads a file's current content.
    pub(crate) async fn get_file_bytes(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<u8>, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.get_file_bytes(spec).await,
            Self::Remote(target) => Ok(target.client.get_file_bytes(spec).await?),
        }
    }

    /// Opens a file download using the selected profile's best available path.
    ///
    /// Embedded profiles stream from their object store. Remote profiles use
    /// direct object-store downloads above the proxy limit and buffer smaller
    /// proxied responses. Every path verifies the complete file.
    ///
    /// `start_offset` resumes streaming downloads. Buffered responses and
    /// retained revisions restart at zero; [`FileDownload::resumed_from`]
    /// reports the offset actually used.
    pub(crate) async fn open_file_download(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
        size_bytes: Option<u64>,
        start_offset: u64,
    ) -> Result<FileDownload, BackendError> {
        if let (Self::Remote(target), Some(size_bytes)) = (self, size_bytes) {
            if target.client.offers_direct_download(size_bytes).await? {
                let grant = target.client.begin_download(spec, revision_no).await?;
                return Ok(FileDownload::Direct {
                    stream: Box::new(
                        target
                            .client
                            .open_direct_download_at(&grant, start_offset)
                            .await?,
                    ),
                    resumed_from: start_offset,
                });
            }
        }
        if let Some(revision_no) = revision_no {
            return Ok(FileDownload::Whole(
                self.get_file_revision_bytes(spec, revision_no).await?,
            ));
        }
        match self {
            Self::Embedded(target) => Ok(FileDownload::Streamed {
                namespace_id: spec.namespace().clone(),
                stream: Box::new(target.backend.read_file_stream(spec, start_offset).await?),
                resumed_from: start_offset,
            }),
            Self::Remote(target) => Ok(FileDownload::Whole(
                target.client.get_file_bytes(spec).await?,
            )),
        }
    }

    /// Content search over a namespace's grep index.
    pub(crate) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.grep(namespace_id, request).await,
            Self::Remote(target) => Ok(target.client.grep(namespace_id, request).await?),
        }
    }

    /// Enables the grep index on a namespace (admin plane).
    pub(crate) async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.enable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.enable_grep_index(namespace_id).await?),
        }
    }

    /// Disables the grep index on a namespace (admin plane).
    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.disable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.disable_grep_index(namespace_id).await?),
        }
    }

    /// Reads the namespace's grep-index lifecycle (admin plane).
    pub(crate) async fn get_grep_index_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.get_grep_index_status(namespace_id).await,
            Self::Remote(target) => Ok(target.client.get_grep_index_status(namespace_id).await?),
        }
    }

    /// Runs one bounded grep-index garbage-collection pass (admin plane).
    pub(crate) async fn gc_grep_index(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepGcRequest,
    ) -> Result<GrepGcResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.gc_grep_index(namespace_id, request).await,
            Self::Remote(target) => Ok(target.client.gc_grep_index(namespace_id, request).await?),
        }
    }

    /// Waits until the grep index has built through `target_seq`, or until
    /// the budget runs out.
    ///
    /// Embedded profiles run bounded index steps locally. Remote profiles poll
    /// the server, which runs index maintenance. Both stop at the captured
    /// target instead of following later commits.
    pub(crate) async fn wait_for_grep_index(
        &self,
        namespace_id: &NamespaceId,
        target_seq: ChangeSeq,
        budget: StepBudget,
    ) -> Result<GrepWaitProgress, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .drive_grep_index(namespace_id, target_seq, budget)
                    .await
            }
            Self::Remote(target) => {
                let timer = StdMonotonicTimer::default();
                let started_ms = timer.monotonic_now_ms();
                let mut steps = 0;
                loop {
                    let lifecycle = target
                        .client
                        .get_grep_index_status(namespace_id)
                        .await?
                        .lifecycle;
                    let reached = lifecycle.is_built_through(target_seq);
                    let elapsed_ms = timer.monotonic_now_ms().saturating_sub(started_ms);
                    if reached || budget.spent(steps, elapsed_ms) {
                        return Ok(GrepWaitProgress { steps, reached });
                    }
                    rest_between_status_checks().await;
                    steps += 1;
                }
            }
        }
    }

    /// Reads a retained file revision's content.
    pub(crate) async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .get_file_revision_bytes(spec, revision_no)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .get_file_revision_bytes(spec, revision_no)
                .await?),
        }
    }

    /// Lists one page of the namespace's recoverable deletions.
    pub(crate) async fn list_trash(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.list_trash(namespace_id, limit, cursor).await,
            Self::Remote(target) => Ok(target
                .client
                .list_trash_page(namespace_id, limit, cursor)
                .await?),
        }
    }

    /// Lists one page of a file's retained revisions.
    pub(crate) async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_file_revisions_page(spec, limit, cursor)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_file_revisions_page(spec, limit, cursor)
                .await?),
        }
    }

    /// Writes a file; `behavior` selects create-only or replace semantics.
    /// An explicit `commit_id` makes the call retryable by resubmission;
    /// absent, one is generated and returned in the response.
    pub(crate) async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.put_file_bytes(spec, bytes, options).await,
            Self::Remote(target) => Ok(target.client.put_file_bytes(spec, bytes, options).await?),
        }
    }

    /// Writes a file from a source stream with bounded memory use.
    ///
    /// Each profile opens the source in the form its transport requires.
    /// `progress` counts bytes read from the source. Remote multipart uploads
    /// use `journal` to resume interrupted sessions; embedded and proxied
    /// uploads do not need it.
    pub(crate) async fn put_file_stream(
        &self,
        spec: &NamespacePath,
        payload: &LocalPayload,
        options: &PutFileOptions,
        progress: &Arc<ProgressReporter>,
        journal: Option<&UploadJournal>,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                let body = payload.open_byte_stream(progress).await?;
                target.backend.put_file_stream(spec, body, options).await
            }
            Self::Remote(target) => {
                let source = payload.open_source(progress).await?;
                let Some(journal) = journal else {
                    return Ok(target.client.put_file_stream(spec, source, options).await?);
                };
                let resume = journal.resume();
                Ok(target
                    .client
                    .put_file_stream_resumable(spec, source, options, journal, resume.as_ref())
                    .await?)
            }
        }
    }

    /// Reads what became of an upload session a previous run opened.
    pub(crate) async fn get_upload_status(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadSession, BackendError> {
        match self {
            Self::Embedded(_) => Err(upload_sessions_need_a_remote_profile()),
            Self::Remote(target) => Ok(target
                .client
                .get_upload_status(namespace_id, upload_id)
                .await?),
        }
    }

    /// Commits content an upload session already completed, moving no bytes.
    pub(crate) async fn commit_completed_upload(
        &self,
        spec: &NamespacePath,
        content_ref: ContentRef,
        content_token: Option<loonfs_api::v0::ContentToken>,
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(_) => Err(upload_sessions_need_a_remote_profile()),
            Self::Remote(target) => Ok(target
                .client
                .commit_completed_upload(spec, content_ref, content_token, options)
                .await?),
        }
    }

    /// Creates a directory; `parents` also creates missing ancestors.
    pub(crate) async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.create_directory(spec, options).await,
            Self::Remote(target) => Ok(target.client.create_directory(spec, options).await?),
        }
    }

    /// Deletes a file or empty directory. With `expected_inode_id`, the
    /// delete applies only while the path still resolves to that inode, so
    /// callers reporting a recovery handle never report a raced rebinding.
    pub(crate) async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.delete_path(spec, options).await,
            Self::Remote(target) => Ok(target.client.delete_path(spec, options).await?),
        }
    }

    /// Writes and removes attributes on the inode a path resolves to.
    pub(crate) async fn update_attributes(
        &self,
        spec: &NamespacePath,
        options: &UpdateAttributesOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.update_attributes(spec, options).await,
            Self::Remote(target) => Ok(target.client.update_attributes(spec, options).await?),
        }
    }

    /// Moves a path within a namespace.
    pub(crate) async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &MoveOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.move_path(from, to, options).await,
            Self::Remote(target) => Ok(target.client.move_path(from, to, options).await?),
        }
    }

    /// Copies a file within a namespace.
    pub(crate) async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &CopyOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.copy_path(from, to, options).await,
            Self::Remote(target) => Ok(target.client.copy_path(from, to, options).await?),
        }
    }

    /// Restores a file to one of its retained revisions.
    pub(crate) async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &RestoreRevisionOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .restore_file_revision(spec, source_revision_no, options)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .restore_file_revision(spec, source_revision_no, options)
                .await?),
        }
    }

    /// Restores the deletion identified by `inode_id` and `deletion_seq`.
    /// When `path` is absent, the original parent and name are used.
    pub(crate) async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        path: Option<&AbsolutePath>,
        options: &UndeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .undelete(namespace_id, inode_id, deletion_seq, path, options)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .undelete(namespace_id, inode_id, deletion_seq, path, options)
                .await?),
        }
    }

    // --- maintenance/admin plane (`admin/v0`) ---

    /// Creates or reuses a named, user-owned checkpoint pinning the
    /// namespace's current view.
    pub(crate) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<Checkpoint, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .create_checkpoint(namespace_id, request)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .create_checkpoint(namespace_id, &request)
                .await?),
        }
    }

    /// Lists one page of the namespace's active checkpoint pins.
    pub(crate) async fn list_checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListCheckpointsResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_checkpoints_page(namespace_id, limit, cursor)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_checkpoints_page(namespace_id, limit, cursor)
                .await?),
        }
    }

    /// Releases a user-owned checkpoint pin by id. Idempotent.
    pub(crate) async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .release_checkpoint(namespace_id, checkpoint_id)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .release_checkpoint(namespace_id, checkpoint_id)
                .await?),
        }
    }

    /// Runs one bounded maintenance step: WAL flush, metadata
    /// reorganization, retention advance, and — when `request.gc` opts in —
    /// one garbage-collection pass. `request.only` restricts it to a single
    /// sub-step.
    pub(crate) async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.maintenance_step(namespace_id, request).await,
            Self::Remote(target) => Ok(target
                .client
                .maintenance_step(namespace_id, &request)
                .await?),
        }
    }

    /// Checks whether the profile's object store meets LoonFS requirements.
    ///
    /// This operation is store-scoped and does not require a namespace.
    /// Embedded profiles probe their configured store directly. Remote
    /// profiles ask the server to probe its configured store.
    pub(crate) async fn probe_store(&self) -> Result<StoreProbeResponse, BackendError> {
        match self {
            Self::Embedded(target) => Ok(target.backend.probe_store().await),
            Self::Remote(target) => Ok(target.client.probe_store(&StoreProbeRequest {}).await?),
        }
    }

    /// Runs `jobs` for `namespaces` until `shutdown` completes.
    ///
    /// Explicit assignments cover namespaces that automatic maintenance may
    /// not observe through normal activity. Only embedded profiles can host
    /// maintenance because remote servers already run their own host.
    ///
    /// `poll_interval_ms` overrides the interval between assignment checks.
    pub(crate) async fn host_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        poll_interval_ms: Option<u64>,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .host_maintenance(namespaces, jobs, poll_interval_ms, shutdown)
                    .await
            }
            Self::Remote(_) => Err(maintenance_host_needs_an_embedded_profile()),
        }
    }

    /// Runs each job and namespace until it settles or the budget expires.
    pub(crate) async fn drain_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        budget: StepBudget,
    ) -> Result<MaintenanceDrainProgress, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .drain_maintenance(namespaces, jobs, budget)
                    .await
            }
            Self::Remote(_) => Err(maintenance_host_needs_an_embedded_profile()),
        }
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    pub(crate) async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ListChangesResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_changes(namespace_id, after_seq, limit)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_changes(namespace_id, after_seq, limit)
                .await?),
        }
    }
}
