//! The backend seam: one logical LoonFS API over two transports.
//!
//! [`ResolvedTarget`] is what every CLI command programs against. A resolved
//! profile decides which arm answers — [`EmbeddedBackend`] drives an
//! in-process `loonfs` runtime, the remote arm drives a `loonfs-client` over
//! HTTP — and the commands above this seam cannot tell which they got.
//!
//! The seam is private to this crate on purpose. It exists so `loonfs` can run
//! its features against either transport, not as an extension point: an
//! application embedding LoonFS programs against `loonfs` (runtime) or
//! `loonfs-client` (HTTP) directly, and neither needs a seam between.
//!
//! The methods are async so the CLI drives both transports from its own
//! runtime. [`loonfs_client::Client`] is itself async, so the remote arms are
//! direct calls that map the client's error type onto [`BackendError`].

mod embedded;

use crate::backend_error::{map_namespace_scoped_runtime_error, BackendError};
use crate::payload::LocalPayload;
use crate::progress::ProgressReporter;
use crate::resolve::ResolvedTarget;
use crate::uploads::UploadJournal;
use bytes::Bytes;
use loonfs_api::{
    v0::{
        ChangesResponse, DisableGrepIndexResponse, EnableGrepIndexResponse, GrepGcRequest,
        GrepGcResponse, GrepIndexLifecycle, GrepIndexStatusResponse, StoreProbeRequest,
        StoreProbeResponse, UploadStatusResponse,
    },
    AuthoritativePathEntry, ChangeSeq, CheckpointId, CommitResponse, ContentRef,
    CreateCheckpointRequest, CreateCheckpointResponse, DeleteNamespaceResponse, GrepRequest,
    GrepResponse, InodeId, ListCheckpointsResponse, ListFileRevisionsResponse,
    ListPathEntriesResponse, ListTrashResponse, MaintenanceStepRequest, MaintenanceStepResponse,
    NamespaceId, NamespaceStatusResponse, NamespaceSummary, ReleaseCheckpointResponse, RevisionNo,
    UploadId,
};
use loonfs_client::{
    CopyOptions, CreateDirectoryOptions, DeleteOptions, DirectDownloadStream, MoveOptions,
    NamespacePath, PutFileOptions, RestoreRevisionOptions, UndeleteOptions,
};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::sync::Arc;

pub(crate) use embedded::EmbeddedBackend;
use loonfs::{
    FileContentStream, MaintenanceJobId, MaintenanceStepConclusion, RuntimeError, SharedObjectStore,
};

/// How long a remote wait rests between status checks.
///
/// A hosted server drives its own index, so this is only how often the
/// command asks. Modest and fixed: the wait is bounded by the caller's own
/// budgets, not by how fast it polls.
const REMOTE_STATUS_POLL_INTERVAL_MS: u64 = 250;

/// What a caller will spend driving bounded steps toward something.
///
/// What one step is depends on what is being driven: one bounded index step
/// where an embedded profile waits for the index, one status check where a
/// remote one does, one bounded maintenance step where `admin run` drains an
/// assignment. The accounting is the same in every case, and both bounds are
/// optional — an unbudgeted caller runs until the work settles.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StepBudget {
    pub max_steps: Option<u64>,
    pub deadline_ms: Option<u64>,
}

impl StepBudget {
    fn spent(&self, steps: u64, elapsed_ms: u64) -> bool {
        self.max_steps.is_some_and(|max_steps| steps >= max_steps)
            || self
                .deadline_ms
                .is_some_and(|deadline_ms| elapsed_ms >= deadline_ms)
    }
}

/// One file's content, in the pieces the transport that opened it delivers.
///
/// The arms exist because the transports genuinely differ, not because the
/// commands above want to know which they got: all are consumed by the
/// same [`FileDownload::next_chunk`] loop, so a download is written to disk
/// or to standard output the same way whichever profile served it.
pub(crate) enum FileDownload {
    /// The embedded runtime's bounded stream. Boxed because a stream carries
    /// its object key, reference, and running digest, and the whole arm is a
    /// pointer beside it.
    Streamed {
        namespace_id: NamespaceId,
        stream: Box<FileContentStream<SharedObjectStore>>,
        resumed_from: u64,
    },
    /// A remote object-store response, streamed and verified by the client
    /// against the content reference in its download grant.
    Direct {
        stream: Box<DirectDownloadStream>,
        resumed_from: u64,
    },
    /// Bytes the transport already holds whole.
    Whole(Vec<u8>),
}

impl FileDownload {
    /// The next piece of the file, or `None` at a verified end.
    ///
    /// A streamed download verifies size and digest on the call that reports
    /// the end, so a caller that drives this to `None` has verified content
    /// and a caller that stops early does not. Held bytes were verified
    /// before they were handed over and answer in one piece.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>, BackendError> {
        match self {
            Self::Streamed {
                namespace_id,
                stream,
                ..
            } => stream.next_chunk().await.map_err(|error| {
                map_namespace_scoped_runtime_error(namespace_id, RuntimeError::Core(error))
            }),
            Self::Direct { stream, .. } => stream.next_chunk().await.map_err(BackendError::from),
            Self::Whole(bytes) if bytes.is_empty() => Ok(None),
            Self::Whole(bytes) => Ok(Some(Bytes::from(std::mem::take(bytes)))),
        }
    }

    /// Where this download actually starts, which is not always where it was
    /// asked to: a transport that answers whole has no partial answer to
    /// pick up from and begins at zero however much the caller holds.
    pub(crate) fn resumed_from(&self) -> u64 {
        match self {
            Self::Streamed { resumed_from, .. } | Self::Direct { resumed_from, .. } => {
                *resumed_from
            }
            Self::Whole(_) => 0,
        }
    }

    /// Hands a resumed download the bytes below its start, so its
    /// verification still covers the whole file.
    ///
    /// Both streaming arms check a digest over the complete object, so a
    /// download that skipped a prefix refuses to read until it has been told
    /// what the prefix was. A held answer never resumed, so it has nothing
    /// to be told.
    pub(crate) fn fold_resumed_prefix(&mut self, bytes: &[u8]) {
        match self {
            Self::Streamed { stream, .. } => stream.fold_resumed_prefix(bytes),
            Self::Direct { stream, .. } => stream.fold_resumed_prefix(bytes),
            Self::Whole(_) => {}
        }
    }
}

/// One assigned `{job, namespace}` key, as a drain left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceKeyProgress {
    pub job: MaintenanceJobId,
    pub namespace_id: NamespaceId,
    /// Steps this drain ran for this key.
    pub steps: u64,
    /// What its last step concluded, absent when the budget ran out before
    /// the key took one.
    pub conclusion: Option<MaintenanceStepConclusion>,
}

impl MaintenanceKeyProgress {
    /// Whether this key has nothing left for a drain to drive.
    ///
    /// Progress and a lost race both leave work behind, so a drain keeps
    /// going. Everything else is where it stops — including `Blocked`, which
    /// says there is work this step's policy cannot move: repeating it would
    /// spin rather than finish.
    pub(crate) fn settled(&self) -> bool {
        match self.conclusion {
            Some(
                MaintenanceStepConclusion::Idle
                | MaintenanceStepConclusion::Blocked
                | MaintenanceStepConclusion::NotEnabled,
            ) => true,
            Some(MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded)
            | None => false,
        }
    }
}

/// Where a drain stopped and what it cost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceDrainProgress {
    /// Every assigned key, in the order the drain drove them.
    pub keys: Vec<MaintenanceKeyProgress>,
    /// Steps this drain ran across every key.
    pub steps: u64,
}

impl MaintenanceDrainProgress {
    /// Whether the budget stopped the drain before every key settled. A key
    /// only goes unsettled that way: the loop that drives it ends at a
    /// settled conclusion or at a spent budget, and at nothing else.
    pub(crate) fn budget_exhausted(&self) -> bool {
        !self.keys.iter().all(MaintenanceKeyProgress::settled)
    }
}

/// Refuses a maintenance host to a profile that has no runtime to host it
/// in. Remote profiles are served by a server that runs its own runner;
/// stepping one from here would be a second scheduler over the same
/// namespaces, and there is no remote step to drive anyway.
/// Refuses upload-session bookkeeping to a profile that has no sessions.
/// An embedded profile stages content through its own runtime, so there is
/// no session to ask after and nothing an interrupted upload could rejoin.
fn upload_sessions_need_a_remote_profile() -> BackendError {
    BackendError::new(
        loonfs_api::ErrorCode::NotSupported.as_str(),
        "upload sessions belong to a server; an embedded profile stages content itself",
    )
}

fn maintenance_host_needs_an_embedded_profile() -> BackendError {
    BackendError::new(
        loonfs_api::ErrorCode::NotSupported.as_str(),
        "`admin run` hosts maintenance in this process and needs an embedded profile; \
         a remote profile's server runs its own maintenance",
    )
}

/// Where a wait stopped and what it cost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrepWaitProgress {
    /// The lifecycle last observed.
    pub state: GrepIndexLifecycle,
    /// Steps this wait spent.
    pub steps: u64,
    /// True when the index reached the target sequence.
    pub reached: bool,
}

/// The one timer this CLI owns: how long a remote wait rests between status
/// checks. Nothing durable depends on it — it only decides how often a
/// command that is already waiting asks again.
#[allow(clippy::disallowed_methods)]
async fn rest_between_status_checks() {
    tokio::time::sleep(std::time::Duration::from_millis(
        REMOTE_STATUS_POLL_INTERVAL_MS,
    ))
    .await;
}

/// One logical LoonFS API over two transports.
///
/// Every method below is an exhaustive `match self` with no catch-all arm,
/// and that exhaustiveness *is* the parity statement this seam used to make
/// with a trait and two named implementations: neither transport can quietly
/// go missing from a method, and both must report the same registry error
/// code for the same failure, so a command renders identical outcomes
/// regardless of which transport a profile selects (this crate's two-mode
/// parity tests hold that line).
impl ResolvedTarget {
    /// Creates a new empty namespace.
    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
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
    ) -> Result<NamespaceSummary, BackendError> {
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
    pub(crate) async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.namespace_status(namespace_id).await,
            Self::Remote(target) => Ok(target.client.namespace_status(namespace_id).await?),
        }
    }

    /// Lists the entries of a directory.
    pub(crate) async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.list_path_entries_all(spec).await,
            Self::Remote(target) => Ok(target.client.list_path_entries_all(spec).await?.entries),
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
                .list_path_entries_page(spec, limit, cursor)
                .await?),
        }
    }

    /// Describes a single path entry.
    pub(crate) async fn stat_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<AuthoritativePathEntry, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.stat_path(spec).await,
            Self::Remote(target) => Ok(target.client.stat_path(spec).await?),
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

    /// Opens a download of one file, in the largest pieces the transport
    /// hands out and no larger.
    ///
    /// An embedded profile reads its own store, so it streams: chunks arrive
    /// one at a time and the file's size never becomes the command's memory.
    /// A remote file past the deployment's proxy cap streams straight from
    /// object storage under a download grant; a smaller proxied response
    /// arrives whole. Every arm is verified before it reports its end.
    ///
    /// `start_offset` asks the two streaming arms to skip bytes the caller
    /// already holds — a download picking up where an interrupted one
    /// stopped. Only they can: a proxied read and a retained revision arrive
    /// in one response, so they start over and say so through
    /// [`FileDownload::resumed_from`].
    pub(crate) async fn open_file_download(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
        size_bytes: Option<u64>,
        start_offset: u64,
    ) -> Result<FileDownload, BackendError> {
        if let (Self::Remote(target), Some(size_bytes)) = (self, size_bytes) {
            if target.client.offers_direct_download(size_bytes).await {
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
    ) -> Result<EnableGrepIndexResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.enable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.enable_grep_index(namespace_id).await?),
        }
    }

    /// Disables the grep index on a namespace (admin plane).
    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.disable_grep_index(namespace_id).await,
            Self::Remote(target) => Ok(target.client.disable_grep_index(namespace_id).await?),
        }
    }

    /// Reads the namespace's grep-index lifecycle (admin plane).
    pub(crate) async fn grep_index_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.grep_index_status(namespace_id).await,
            Self::Remote(target) => Ok(target.client.grep_index_status(namespace_id).await?),
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
    /// The two arms advance the same wait differently, and that is the whole
    /// difference between them. An embedded profile has no maintenance host,
    /// so it *is* the host: it runs the index job's bounded steps itself. A
    /// remote profile's server drives its own index, so this only watches
    /// the status endpoint. Both stop at the target they were given and
    /// never chase a head that keeps moving.
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
                    let state = target.client.grep_index_status(namespace_id).await?.state;
                    let reached = state.is_built_through(target_seq);
                    let elapsed_ms = timer.monotonic_now_ms().saturating_sub(started_ms);
                    if reached || budget.spent(steps, elapsed_ms) {
                        return Ok(GrepWaitProgress {
                            state,
                            steps,
                            reached,
                        });
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

    /// Writes a file from a payload read once from its source, in bounded
    /// memory whichever transport answers.
    ///
    /// The payload is opened per arm rather than shared: the two runtimes
    /// spell a byte stream in their own terms, and only one arm ever runs.
    /// `progress` counts what the payload gives up, so both arms report the
    /// same bytes for the same file.
    /// `journal` is where a remote profile records a direct multipart upload
    /// so an interrupted one can pick up. Only that transport has anything
    /// to record: an embedded profile stages through its own runtime with no
    /// session to rejoin, and a proxied upload is one request with no parts
    /// to have half-finished.
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
    pub(crate) async fn read_upload_status(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadStatusResponse, BackendError> {
        match self {
            Self::Embedded(_) => Err(upload_sessions_need_a_remote_profile()),
            Self::Remote(target) => Ok(target
                .client
                .read_upload_status(namespace_id, upload_id)
                .await?),
        }
    }

    /// Commits content an upload session already completed, moving no bytes.
    pub(crate) async fn commit_completed_upload(
        &self,
        spec: &NamespacePath,
        content_ref: ContentRef,
        validated_content_token: Option<String>,
        options: &PutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(_) => Err(upload_sessions_need_a_remote_profile()),
            Self::Remote(target) => Ok(target
                .client
                .commit_completed_upload(spec, content_ref, validated_content_token, options)
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

    /// Recovers a deleted file or subtree to the spec's path; `inode_id`
    /// and `deleted_at_seq` are the identity and committed sequence the
    /// delete reported.
    pub(crate) async fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        options: &UndeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        match self {
            Self::Embedded(target) => {
                target
                    .backend
                    .undelete(spec, inode_id, deleted_at_seq, options)
                    .await
            }
            Self::Remote(target) => Ok(target
                .client
                .undelete(spec, inode_id, deleted_at_seq, options)
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
    ) -> Result<CreateCheckpointResponse, BackendError> {
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

    /// Lists the namespace's active checkpoint pins, oldest first.
    pub(crate) async fn list_checkpoints(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ListCheckpointsResponse, BackendError> {
        match self {
            Self::Embedded(target) => target.backend.list_checkpoints(namespace_id).await,
            Self::Remote(target) => Ok(target.client.list_checkpoints(namespace_id).await?),
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

    /// Proves the profile's object store honours the object-store contract
    /// LoonFS depends on, and reports what it found check by check.
    ///
    /// Store-scoped: it names no namespace. An embedded profile probes the
    /// store it is configured with; a remote profile asks its server to
    /// probe the store that server is configured with, which is the store
    /// the answer is about either way.
    pub(crate) async fn probe_store(&self) -> Result<StoreProbeResponse, BackendError> {
        match self {
            Self::Embedded(target) => Ok(target.backend.probe_store().await),
            Self::Remote(target) => Ok(target.client.probe_store(&StoreProbeRequest {}).await?),
        }
    }

    /// Hosts `jobs` for `namespaces` until `shutdown` resolves, then settles
    /// what it admitted.
    ///
    /// This is the explicit half of the coverage contract: automatic
    /// maintenance reaches the namespaces a process touches and the ones a
    /// host is assigned, and this is where a namespace nobody is writing to
    /// gets assigned. Only an embedded profile can host it — a remote
    /// profile's server is already hosting its own.
    ///
    /// `poll_interval_ms` overrides how long an assignment rests before it
    /// is asserted again; `None` keeps the host's own cadence.
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

    /// Runs every `{job, namespace}` key to a settled conclusion, or until
    /// `budget` runs out, and reports where each one got to.
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
    ) -> Result<ChangesResponse, BackendError> {
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
