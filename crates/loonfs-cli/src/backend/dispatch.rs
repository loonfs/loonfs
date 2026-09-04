//! Dispatches each resolved operation to its embedded or remote implementation.

use super::step_budget::{rest_between_status_checks, wait_for_grep_index, GrepWaitStep};
use super::{FileDownload, GrepWaitProgress, MaintenanceDrainProgress, StepBudget};
use crate::backend_error::NamespaceScoped;
use crate::error::CliError;
use crate::payload::LocalPayload;
use crate::progress::ProgressReporter;
use crate::resolve::ResolvedTarget;
use crate::uploads::UploadJournal;
use loonfs::{MaintenanceJobId, ReadFileStreamOptions};
use loonfs_api::{
    v0::{
        GrepGcRequest, GrepGcResponse, GrepIndex, ListChangesResponse, ListSnapshotsResponse,
        ReleaseSnapshotResponse, SnapshotSummary, StoreProbeRequest, StoreProbeResponse,
        UploadSession,
    },
    AbsolutePath, CapabilityDocument, ChangeSeq, Checkpoint, CheckpointId, CommitResponse,
    ContentRef, CreateCheckpointRequest, DeleteNamespaceResponse, GrepRequest, GrepResponse,
    InodeId, ListCheckpointsResponse, ListFileRevisionsResponse, ListPathEntriesResponse,
    ListTrashResponse, MaintenanceRunRequest, MaintenanceRunResponse, Namespace, NamespaceId,
    PathEntry, ReleaseCheckpointResponse, RevisionNo, UploadId,
};
use loonfs_client::{
    ClientError, CopyOptions, CreateDirectoryOptions, DeleteOptions, DownloadOptions,
    ListChangesOptions, ListPathEntriesOptions, MoveOptions, NamespacePath, PutFileOptions,
    ReadFileOptions, RestoreRevisionOptions, StatPathOptions, UndeleteOptions,
    UpdateAttributesOptions,
};
use std::sync::Arc;

/// Returns errors for commands that require a different profile type.
///
/// Remote profiles cannot host local maintenance because the server already
/// schedules it. Embedded profiles do not expose upload sessions because
/// they stage content directly in-process.
fn upload_sessions_need_a_remote_profile() -> CliError {
    CliError::new(
        loonfs_api::ErrorCode::NotSupported.as_str(),
        "upload sessions belong to a server; an embedded profile stages content itself",
    )
}

fn maintenance_host_needs_an_embedded_profile() -> CliError {
    CliError::new(
        loonfs_api::ErrorCode::NotSupported.as_str(),
        "`maintenance run` requires an embedded profile because remote servers run their \
         own maintenance; use `loonfs maintenance step` for one pass or `loonfs maintenance \
         index status` to inspect the index",
    )
}

/// Implements CLI operations for embedded and remote targets.
///
/// Each method exhaustively matches both variants, so adding an operation or
/// target requires handling both transports. Both paths normalize failures to
/// the same error-code registry, which keeps command output consistent.
impl ResolvedTarget {
    /// Returns the capabilities reported by the selected deployment.
    pub(crate) async fn get_capabilities(&self) -> Result<CapabilityDocument, CliError> {
        match self {
            Self::Embedded(target) => Ok(target.backend.reader.get_capabilities()),
            Self::Remote(target) => Ok(target.client.get_capabilities().await?),
        }
    }

    /// Checks whether the client can reach the remote health endpoint.
    ///
    /// Any API response means the connection succeeded. A separate check
    /// determines whether the health endpoint returned a successful status.
    pub(crate) async fn remote_connectivity(&self) -> Result<(), CliError> {
        match self {
            Self::Embedded(_) => Ok(()),
            Self::Remote(target) => match target.client.get_health().await {
                Ok(()) | Err(ClientError::Api { .. }) => Ok(()),
                Err(error) => Err(error.into()),
            },
        }
    }

    /// Checks the remote server's public health endpoint.
    pub(crate) async fn remote_health(&self) -> Result<(), CliError> {
        match self {
            Self::Embedded(_) => Ok(()),
            Self::Remote(target) => Ok(target.client.get_health().await?),
        }
    }

    /// Creates a new empty namespace.
    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Namespace, CliError> {
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
    ) -> Result<DeleteNamespaceResponse, CliError> {
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
    ) -> Result<Namespace, CliError> {
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
    ) -> Result<Namespace, CliError> {
        match self {
            Self::Embedded(target) => target
                .backend
                .reader
                .get_namespace(namespace_id)
                .await
                .scoped(namespace_id),
            Self::Remote(target) => Ok(target.client.get_namespace(namespace_id).await?),
        }
    }

    /// Lists one page of a directory, for callers that bound their output.
    pub(crate) async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
        snapshot_id: Option<&CheckpointId>,
    ) -> Result<ListPathEntriesResponse, CliError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_path_entries_page(spec, limit, cursor, snapshot_id)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_path_entries_page(
                    spec,
                    limit,
                    cursor,
                    &ListPathEntriesOptions {
                        snapshot_id: snapshot_id.cloned(),
                        ..ListPathEntriesOptions::default()
                    },
                )
                .await?),
        }
    }

    /// Describes a path from the current state or a snapshot.
    pub(crate) async fn get_path_entry_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&CheckpointId>,
    ) -> Result<PathEntry, CliError> {
        self.get_path_entry_projected(
            spec,
            &StatPathOptions {
                snapshot_id: snapshot_id.cloned(),
                ..StatPathOptions::default()
            },
        )
        .await
    }

    /// Describes one visible inode, including its attributes.
    pub(crate) async fn get_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<PathEntry, CliError> {
        match self {
            Self::Embedded(target) => target
                .backend
                .reader
                .get_inode(namespace_id, inode_id, StatPathOptions::default())
                .await
                .scoped(namespace_id),
            Self::Remote(target) => Ok(target
                .client
                .get_inode(namespace_id, inode_id, &StatPathOptions::default())
                .await?),
        }
    }

    /// Describes a path without loading its attributes.
    pub(crate) async fn get_path_entry_without_attributes(
        &self,
        spec: &NamespacePath,
    ) -> Result<PathEntry, CliError> {
        self.get_path_entry_without_attributes_at_snapshot(spec, None)
            .await
    }

    pub(crate) async fn get_path_entry_without_attributes_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&CheckpointId>,
    ) -> Result<PathEntry, CliError> {
        self.get_path_entry_projected(
            spec,
            &StatPathOptions {
                include_attributes: loonfs_api::AttributeInclusion::Omit,
                snapshot_id: snapshot_id.cloned(),
            },
        )
        .await
    }

    async fn get_path_entry_projected(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<PathEntry, CliError> {
        match self {
            Self::Embedded(target) => target.backend.get_path_entry(spec, options).await,
            Self::Remote(target) => Ok(target.client.get_path_entry(spec, options).await?),
        }
    }

    /// Reads file content from a revision or snapshot.
    pub(crate) async fn get_file_bytes_with_options(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>, CliError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .get_file_bytes_with_options(spec, options)
                    .await
            }
            Self::Remote(target) => Ok(target.client.get_file_bytes(spec, options).await?),
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
        snapshot_id: Option<&CheckpointId>,
        size_bytes: Option<u64>,
        start_offset: u64,
    ) -> Result<FileDownload, CliError> {
        if let (Self::Remote(target), Some(size_bytes)) = (self, size_bytes) {
            if target.client.offers_direct_download(size_bytes).await? {
                let grant = target
                    .client
                    .create_download(
                        spec,
                        &DownloadOptions {
                            revision_no,
                            snapshot_id: snapshot_id.cloned(),
                        },
                    )
                    .await?;
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
        if revision_no.is_some() || snapshot_id.is_some() {
            return Ok(FileDownload::Whole(
                self.get_file_bytes_with_options(
                    spec,
                    &ReadFileOptions {
                        revision_no,
                        snapshot_id: snapshot_id.cloned(),
                    },
                )
                .await?,
            ));
        }
        match self {
            Self::Embedded(target) => Ok(FileDownload::Streamed {
                namespace_id: spec.namespace().clone(),
                stream: Box::new(
                    target
                        .backend
                        .reader
                        .read_file_stream(
                            spec.namespace(),
                            spec.absolute_path().as_str(),
                            ReadFileStreamOptions {
                                start_offset,
                                ..ReadFileStreamOptions::default()
                            },
                        )
                        .await
                        .scoped(spec.namespace())?,
                ),
                resumed_from: start_offset,
            }),
            Self::Remote(target) => Ok(FileDownload::Whole(
                target
                    .client
                    .get_file_bytes(spec, &ReadFileOptions::default())
                    .await?,
            )),
        }
    }

    /// Content search over a namespace's grep index.
    pub(crate) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
        limit: Option<u32>,
    ) -> Result<GrepResponse, CliError> {
        match self {
            Self::Embedded(target) => target.backend.grep(namespace_id, request, limit).await,
            Self::Remote(target) => Ok(target.client.grep(namespace_id, request, limit).await?),
        }
    }

    /// Enables the grep index on a namespace (maintenance plane).
    pub(crate) async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, CliError> {
        match self {
            Self::Embedded(target) => target.backend.enable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.enable_grep_index(namespace_id).await?),
        }
    }

    /// Disables the grep index on a namespace (maintenance plane).
    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, CliError> {
        match self {
            Self::Embedded(target) => target.backend.disable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.disable_grep_index(namespace_id).await?),
        }
    }

    /// Reads the namespace's grep-index lifecycle (maintenance plane).
    pub(crate) async fn get_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, CliError> {
        match self {
            Self::Embedded(target) => target
                .backend
                .grep_worker()
                .get_grep_index(namespace_id)
                .await
                .scoped(namespace_id),
            Self::Remote(target) => Ok(target.client.get_grep_index(namespace_id).await?),
        }
    }

    /// Runs one bounded grep-index garbage-collection pass (maintenance plane).
    pub(crate) async fn gc_grep_index(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepGcRequest,
    ) -> Result<GrepGcResponse, CliError> {
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
    ) -> Result<GrepWaitProgress, CliError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .drive_grep_index(namespace_id, target_seq, budget)
                    .await
            }
            Self::Remote(target) => {
                wait_for_grep_index(
                    target_seq,
                    budget,
                    || async {
                        let index = target.client.get_grep_index(namespace_id).await?;
                        Ok(index.lifecycle)
                    },
                    || async {
                        rest_between_status_checks().await;
                        Ok(GrepWaitStep::Continue)
                    },
                )
                .await
            }
        }
    }

    /// Lists one page of the namespace's recoverable deletions.
    pub(crate) async fn list_trash(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse, CliError> {
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
    ) -> Result<ListFileRevisionsResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    pub(crate) async fn get_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadSession, CliError> {
        match self {
            Self::Embedded(_) => Err(upload_sessions_need_a_remote_profile()),
            Self::Remote(target) => Ok(target.client.get_upload(namespace_id, upload_id).await?),
        }
    }

    /// Commits content an upload session already completed, moving no bytes.
    pub(crate) async fn commit_completed_upload(
        &self,
        spec: &NamespacePath,
        content_ref: ContentRef,
        content_token: Option<loonfs_api::v0::ContentToken>,
        options: &PutFileOptions,
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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
    ) -> Result<CommitResponse, CliError> {
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

    /// Saves the namespace's current state for a limited time.
    pub(crate) async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary, CliError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .create_snapshot(namespace_id, name, ttl_ms)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .create_snapshot(namespace_id, name, ttl_ms)
                .await?),
        }
    }

    /// Lists one page of available snapshots.
    pub(crate) async fn list_snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListSnapshotsResponse, CliError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_snapshots_page(namespace_id, limit, cursor)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_snapshots_page(namespace_id, limit, cursor)
                .await?),
        }
    }

    /// Extends a snapshot's lifetime.
    pub(crate) async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary, CliError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .extend_snapshot(namespace_id, snapshot_id, ttl_ms)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .extend_snapshot(namespace_id, snapshot_id, ttl_ms)
                .await?),
        }
    }

    /// Releases one snapshot. Repeated releases succeed.
    pub(crate) async fn release_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
    ) -> Result<ReleaseSnapshotResponse, CliError> {
        match self {
            Self::Embedded(target) => target
                .backend
                .writer
                .release_snapshot(namespace_id, snapshot_id)
                .await
                .scoped(namespace_id),
            Self::Remote(target) => Ok(target
                .client
                .release_snapshot(namespace_id, snapshot_id)
                .await?),
        }
    }

    // --- maintenance plane (`maintenance/v0`) ---

    /// Creates or reuses a named, user-owned checkpoint pinning the
    /// namespace's current view.
    pub(crate) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<Checkpoint, CliError> {
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
    ) -> Result<ListCheckpointsResponse, CliError> {
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
    ) -> Result<ReleaseCheckpointResponse, CliError> {
        match self {
            Self::Embedded(target) => target
                .backend
                .maintenance
                .release_checkpoint(namespace_id, checkpoint_id)
                .await
                .scoped(namespace_id),
            Self::Remote(target) => Ok(target
                .client
                .release_checkpoint(namespace_id, checkpoint_id)
                .await?),
        }
    }

    /// Runs one maintenance job.
    pub(crate) async fn run_maintenance(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceRunRequest,
    ) -> Result<MaintenanceRunResponse, CliError> {
        match self {
            Self::Embedded(target) => target.backend.run_maintenance(namespace_id, request).await,
            Self::Remote(target) => Ok(target
                .client
                .run_maintenance(namespace_id, &request)
                .await?),
        }
    }

    /// Checks whether the profile's object store meets LoonFS requirements.
    ///
    /// This operation is store-scoped and does not require a namespace.
    /// Embedded profiles probe their configured store directly. Remote
    /// profiles ask the server to probe its configured store.
    pub(crate) async fn probe_store(&self) -> Result<StoreProbeResponse, CliError> {
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
    ) -> Result<(), CliError> {
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
    ) -> Result<MaintenanceDrainProgress, CliError> {
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
        snapshot_id: Option<&CheckpointId>,
    ) -> Result<ListChangesResponse, CliError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .list_changes(namespace_id, after_seq, limit, snapshot_id)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .list_changes(
                    namespace_id,
                    after_seq,
                    &ListChangesOptions {
                        limit,
                        snapshot_id: snapshot_id.cloned(),
                    },
                )
                .await?),
        }
    }
}
