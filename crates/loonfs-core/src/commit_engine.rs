//! [`NamespaceCommitEngine`] publishes a batch of validated mutation
//! candidates as one WAL segment and one head compare-and-swap, then returns
//! one result per candidate.

use crate::checkpoint::MetadataTableCache;
use crate::commit::CommitFingerprint;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataViewError, Result, WriterFence};
use crate::metadata::MetadataState;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::options::DeleteNamespaceOptions;
use crate::path::write::{commit_fingerprint, CommitRequest, FilesystemOperation};
use crate::protocol::{
    load_publish_metadata_view, PublishTailOptions, PublishTailProjection, PublishTailWeight,
    PublishViewEffect,
};
use crate::storage::content_admission::{ContentAdmission, ContentTokenError, PreparedContent};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::wire::control::{AcquiredWriter, HeadState};
use loonfs_api::{ChangeSeq, CommitId, ContentId, DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// One namespace mutation together with the result of preparing any content
/// it references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitCandidate {
    request: CommitRequest,
    content: ContentPreparation,
}

/// The result of preparing external content referenced by a mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentPreparation {
    Ready(Vec<ContentAdmission>),
    Rejected(ContentPreparationError),
}

/// A typed failure to prepare content referenced by a mutation candidate.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ContentPreparationError {
    /// Tokens rejected before publication, paired with their content IDs. The
    /// list is non-empty and includes every rejection so the caller can replace
    /// the correct tokens.
    #[error("content tokens were rejected: {}", rejected_token_reasons(.0))]
    ContentToken(Vec<(ContentId, ContentTokenError)>),
    /// No prepared proof covers the referenced content.
    #[error("content object `{content_id}` is not prepared for publication")]
    ContentNotPrepared { content_id: ContentId },
}

/// Formats all token rejections as one message, pairing each content ID with its reason.
fn rejected_token_reasons(rejections: &[(ContentId, ContentTokenError)]) -> String {
    rejections
        .iter()
        .map(|(content_id, error)| format!("`{content_id}`: {error}"))
        .collect::<Vec<_>>()
        .join("; ")
}

impl CommitCandidate {
    /// Wraps a mutation request with no attached content proofs.
    pub fn new(request: CommitRequest) -> Self {
        Self {
            request,
            content: ContentPreparation::Ready(Vec::new()),
        }
    }

    /// Wraps a mutation request with opaque proofs for its prepared content.
    pub fn prepared(request: CommitRequest, content: Vec<PreparedContent>) -> Self {
        Self {
            request,
            content: ContentPreparation::Ready(
                content
                    .into_iter()
                    .map(PreparedContent::into_admission)
                    .collect(),
            ),
        }
    }

    /// Wraps a mutation request whose content preparation failed.
    pub fn rejected(request: CommitRequest, error: ContentPreparationError) -> Self {
        Self {
            request,
            content: ContentPreparation::Rejected(error),
        }
    }

    pub(crate) fn request(&self) -> &CommitRequest {
        &self.request
    }

    pub(crate) fn content_preparation(&self) -> &ContentPreparation {
        &self.content
    }

    /// Returns the idempotency key carried by the mutation request.
    pub fn commit_id(&self) -> &CommitId {
        &self.request.commit_id
    }

    /// Computes semantic identity from the request alone, without applying
    /// current operational request limits.
    pub fn semantic_identity(&self, namespace_id: &NamespaceId) -> Result<CommitFingerprint> {
        commit_fingerprint(namespace_id, &self.request)
    }

    pub(crate) fn validate_request_limits(&self) -> Result<()> {
        // Apply limits to the complete request because the serialized publisher
        // processes every operation before releasing the write path.
        if self.request.operations.len() > crate::limits::MAX_COMMIT_OPERATIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {} operations; maximum is {}",
                self.request.operations.len(),
                crate::limits::MAX_COMMIT_OPERATIONS
            )));
        }
        if let Some(message) = &self.request.message {
            if message.len() > crate::limits::MAX_COMMIT_MESSAGE_BYTES {
                return Err(CoreError::InvalidCommitRequest(format!(
                    "mutation message is {} bytes; maximum is {}",
                    message.len(),
                    crate::limits::MAX_COMMIT_MESSAGE_BYTES
                )));
            }
        }
        let prepared_count = match &self.content {
            ContentPreparation::Ready(content) => content.len(),
            ContentPreparation::Rejected(_) => 0,
        };
        if prepared_count > crate::limits::MAX_COMMIT_CONTENT_TOKENS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {prepared_count} prepared content proofs; maximum is {}",
                crate::limits::MAX_COMMIT_CONTENT_TOKENS
            )));
        }
        let distinct_content_refs = self
            .request
            .operations
            .iter()
            .filter_map(|operation| match operation {
                FilesystemOperation::PutFile { content_ref, .. } => Some(content_ref),
                _ => None,
            })
            .collect::<HashSet<_>>()
            .len();
        if distinct_content_refs > crate::limits::MAX_COMMIT_EXTERNAL_CONTENT_REFS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation references {distinct_content_refs} distinct external content refs; maximum is {}",
                crate::limits::MAX_COMMIT_EXTERNAL_CONTENT_REFS
            )));
        }
        Ok(())
    }
}

/// Defines the WAL-tail thresholds for checkpointing and write rejection.
/// Keeping both values in one policy prevents inconsistent configuration.
///
/// Reads do not depend on tail length. Writes are rejected only when
/// maintenance has fallen behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalTailPolicy {
    /// Visible WAL-tail length, in segments, at which a maintenance step
    /// publishes a checkpoint. The step fires at or past this length.
    pub checkpoint_at_segments: u64,
    /// WAL-tail length at which all publish operations return
    /// `maintenance_required` before adding another segment.
    pub reject_writes_at_segments: u64,
}

impl WalTailPolicy {
    /// The workspace policy: checkpoint at 32 segments, reject at the
    /// longest tail the format admits.
    pub const DEFAULT: Self = Self {
        checkpoint_at_segments: 32,
        reject_writes_at_segments: crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS,
    };
}

impl Default for WalTailPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

// Checkpointing must begin before the write-rejection threshold so
// maintenance can reduce the tail before writes are blocked.
const _: () = assert!(
    0 < WalTailPolicy::DEFAULT.checkpoint_at_segments
        && WalTailPolicy::DEFAULT.checkpoint_at_segments
            < WalTailPolicy::DEFAULT.reject_writes_at_segments,
);

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishResult {
    pub results: Vec<Result<ApiCommitResponse>>,
    /// WAL tail length observed by this publish, for opportunistic
    /// maintenance scheduling. Zero when no projection was loaded.
    pub wal_tail_segments: u64,
    /// Read state produced by a successful, unambiguous head CAS. Callers can
    /// use it to update read caches without reloading from object storage.
    pub resulting_read_state: Option<ResultingReadState>,
}

/// A read anchor plus the projected WAL tail as of one landed publish.
#[derive(Debug, Clone)]
pub struct ResultingReadState {
    pub head: HeadState,
    pub head_etag: String,
    /// Metadata basis used for replay. The published head still references this
    /// basis, so a seeded read cache matches the next store-backed read.
    pub basis: MetadataBasis,
    pub manifest_head_seq: ChangeSeq,
    pub tail_rows: Arc<MetadataState>,
}

/// Tracks writer state for one namespace session: unacquired, acquired, or
/// permanently fenced.
///
/// Object storage cannot reconstruct whether the current session was fenced.
/// Runtimes should therefore share one instance across engines for the same
/// namespace and keep it outside rebuildable caches. An engine without shared
/// state treats each one-shot commit as a separate session.
#[derive(Debug, Default)]
pub enum WriterSessionState {
    /// No epoch yet: the session's first publish acquires one.
    #[default]
    Unacquired,
    /// Writer epoch acquired on the first publish and reused for the rest of
    /// the session.
    Acquired(AcquiredWriter),
    /// Permanent fencing record for this session. Later publishes return
    /// `writer_fenced` without accessing the store. Acquiring a new writer epoch
    /// requires an explicit caller action.
    Fenced(WriterFence),
}

/// Shared handle to one namespace's [`WriterSessionState`].
pub type SharedWriterSessionState = Arc<Mutex<WriterSessionState>>;

#[derive(Debug, Clone)]
pub struct NamespaceCommitEngine {
    namespace_id: NamespaceId,
    publish_tail_projection: Option<PublishTailProjection>,
    /// This session's epoch and fencing for the namespace; see
    /// [`WriterSessionState`].
    session: SharedWriterSessionState,
    /// Local monotonic source for the self-enforced publish budget.
    timer: Arc<dyn MonotonicTimer>,
    /// Shared cache of decoded blocks used by publish-view reads. Blocks are
    /// keyed by segment digest, while the head ETag check verifies that the view
    /// is current.
    table_cache: Option<Arc<MetadataTableCache>>,
}

impl NamespaceCommitEngine {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            publish_tail_projection: None,
            session: SharedWriterSessionState::default(),
            timer: Arc::new(StdMonotonicTimer::default()),
            table_cache: None,
        }
    }

    /// Uses runtime-managed session state so the writer epoch and fenced status
    /// persist across engine instances.
    pub fn writer_session(mut self, session: SharedWriterSessionState) -> Self {
        self.session = session;
        self
    }

    fn lock_session(&self) -> std::sync::MutexGuard<'_, WriterSessionState> {
        // Treat a poisoned lock as fatal because another thread panicked while
        // updating the session state.
        self.session
            .lock()
            .expect("writer session state lock should not be poisoned")
    }

    #[cfg(test)]
    pub(crate) fn monotonic_timer(mut self, timer: Arc<dyn MonotonicTimer>) -> Self {
        self.timer = timer;
        self
    }

    pub fn table_cache(mut self, table_cache: Arc<MetadataTableCache>) -> Self {
        self.table_cache = Some(table_cache);
        self
    }

    /// Clears only the rebuildable tail projection. Writer epoch and fencing
    /// remain in session state and are not reset by cache invalidation.
    pub fn invalidate_projection(&mut self) {
        self.publish_tail_projection = None;
    }

    /// Returns the retained tail projection's memory weight, or `None` when no
    /// projection is cached. Runtimes can sum this value across namespace engines
    /// to enforce a global cache limit.
    pub fn retained_tail_weight(&self) -> Option<PublishTailWeight> {
        self.publish_tail_projection
            .as_ref()
            .map(PublishTailProjection::weight)
    }

    /// Returns the session's writer epoch, acquiring it on first use. A fenced
    /// session fails immediately without accessing the store or acquiring a new
    /// epoch.
    async fn session_writer_epoch<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        context: &MutationContext,
    ) -> Result<AcquiredWriter> {
        let already_acquired = match &*self.lock_session() {
            WriterSessionState::Fenced(fence) => {
                return Err(CoreError::WriterFenced(fence.clone()))
            }
            WriterSessionState::Acquired(acquired_writer) => Some(acquired_writer.clone()),
            WriterSessionState::Unacquired => None,
        };
        if let Some(acquired_writer) = already_acquired {
            return Ok(acquired_writer);
        }
        let acquired_writer = acquire_writer_epoch(store, &self.namespace_id, context)
            .await
            .map_err(CoreError::WriterEpoch)?;
        let mut session = self.lock_session();
        if let WriterSessionState::Fenced(fence) = &*session {
            // Another engine fenced the shared session while this engine was acquiring
            // the epoch. Preserve the fenced state.
            return Err(CoreError::WriterFenced(fence.clone()));
        }
        *session = WriterSessionState::Acquired(acquired_writer.clone());
        Ok(acquired_writer)
    }

    /// Deletes the namespace using this writer session (format spec,
    /// "Tombstones and deletion").
    ///
    /// Deletion advances the head, so it uses the same writer-session checks as
    /// [`Self::publish_batch`]. Fenced sessions fail before accessing the store,
    /// and a takeover detected during the tombstone CAS permanently fences the
    /// session.
    pub async fn delete_namespace<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        options: DeleteNamespaceOptions,
        context: &MutationContext,
    ) -> Result<DeleteNamespaceResponse> {
        let acquired_writer = self.session_writer_epoch(store, context).await?;
        let deleted = crate::namespace::delete::delete_namespace(
            store,
            &self.namespace_id,
            options,
            acquired_writer,
        )
        .await;
        if let Err(CoreError::WriterFenced(fence)) = &deleted {
            *self.lock_session() = WriterSessionState::Fenced(fence.clone());
        }
        deleted
    }

    pub async fn publish_batch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<CommitCandidate>,
        context: &MutationContext,
        tail_options: &PublishTailOptions,
    ) -> NamespaceCommitEnginePublishResult {
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishResult {
                results: Vec::new(),
                wal_tail_segments: 0,
                resulting_read_state: None,
            };
        }

        let candidate_count = candidates.len();
        let acquired_writer = match self.session_writer_epoch(store, context).await {
            Ok(value) => value,
            Err(error) => {
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, error),
                    wal_tail_segments: 0,
                    resulting_read_state: None,
                };
            }
        };

        let (publish_view, projection) = match load_publish_metadata_view(
            store,
            self.table_cache.as_deref(),
            &self.namespace_id,
            acquired_writer,
            self.publish_tail_projection.as_ref(),
            tail_options,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                self.invalidate_projection();
                if let CoreError::WriterFenced(fence) = &error {
                    *self.lock_session() = WriterSessionState::Fenced(fence.clone());
                }
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, error),
                    wal_tail_segments: 0,
                    resulting_read_state: None,
                };
            }
        };

        let reject_writes_at_segments = WalTailPolicy::DEFAULT.reject_writes_at_segments;
        // `wal_tail_segments` is the tail this publish would extend, so
        // rejecting at the bound is what keeps the landed tail inside it.
        if projection.wal_tail_segments >= reject_writes_at_segments {
            let wal_tail_segments = projection.wal_tail_segments;
            self.publish_tail_projection = Some(projection);
            let error = MetadataViewError::MaintenanceRequired {
                namespace_id: self.namespace_id.clone(),
                reason: format!(
                    "wal tail has {wal_tail_segments} segments; publishes resume once maintenance brings it back under {reject_writes_at_segments}"
                ),
            };
            return NamespaceCommitEnginePublishResult {
                results: repeated_error(candidate_count, CoreError::from(error)),
                wal_tail_segments,
                resulting_read_state: None,
            };
        }

        let published = crate::protocol::publish_namespace_commits_batch_against_publish_view(
            store,
            &self.namespace_id,
            &candidates,
            context,
            &publish_view,
            self.timer.as_ref(),
        )
        .await;
        let resulting_head = match &published.effect {
            PublishViewEffect::Advanced { head, .. } => Some(head.clone()),
            PublishViewEffect::Unchanged | PublishViewEffect::Invalidated => None,
        };
        let wal_tail_segments =
            self.update_publish_tail_projection(projection, published.effect, tail_options);
        // Seedable only when the CAS landed unambiguously and the updated
        // projection survived (it carries the post-publish tail and etag).
        let resulting_read_state = match (resulting_head, self.publish_tail_projection.as_ref()) {
            (Some(head), Some(projection)) if projection.head_seq() == head.seq => {
                Some(ResultingReadState {
                    head,
                    head_etag: projection.head_etag().to_owned(),
                    basis: projection.basis().clone(),
                    manifest_head_seq: projection.manifest_head_seq(),
                    tail_rows: Arc::new(projection.tail_state.clone()),
                })
            }
            _ => None,
        };
        NamespaceCommitEnginePublishResult {
            results: published.results,
            wal_tail_segments,
            resulting_read_state,
        }
    }

    /// Folds one batch's effect into the retained tail projection, and
    /// reports the WAL-tail length the caller schedules maintenance on.
    fn update_publish_tail_projection(
        &mut self,
        mut projection: PublishTailProjection,
        effect: PublishViewEffect,
        tail_options: &PublishTailOptions,
    ) -> u64 {
        match effect {
            // Nothing landed, so the loaded projection still describes the
            // tail exactly.
            PublishViewEffect::Unchanged => {
                let wal_tail_segments = projection.wal_tail_segments;
                self.publish_tail_projection = Some(projection);
                wal_tail_segments
            }
            PublishViewEffect::Invalidated => {
                self.invalidate_projection();
                projection.wal_tail_segments
            }
            // A landed batch is exactly one new WAL segment.
            PublishViewEffect::Advanced {
                records,
                head,
                head_etag,
            } => {
                projection.wal_tail_segments = projection.wal_tail_segments.saturating_add(1);
                let wal_tail_segments = projection.wal_tail_segments;
                debug_assert!(
                    wal_tail_segments <= crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS,
                    "a landed publish left {wal_tail_segments} unflushed segments, \
                     more than the head can describe"
                );
                // The head advanced, but without the etag its
                // compare-and-swap acknowledged there is nothing to re-anchor
                // the projection to.
                let Some(head_etag) = head_etag else {
                    self.invalidate_projection();
                    return wal_tail_segments;
                };
                for record in &records {
                    projection.tail_state.apply_committed_wal_record_mut(record);
                }
                projection.reanchor(head.seq, head_etag);
                if projection.within_limits(tail_options) {
                    self.publish_tail_projection = Some(projection);
                } else {
                    self.invalidate_projection();
                }
                wal_tail_segments
            }
        }
    }
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<ApiCommitResponse>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

/// Publishes one batch through a fresh, uncached commit engine.
pub(crate) async fn publish_namespace_commits_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<CommitCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse>> {
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch(store, candidates, context, &PublishTailOptions::default())
        .await
        .results
}

/// Deletes a namespace through a fresh, uncached commit engine: a one-shot
/// session that acquires its own epoch, exactly like a one-shot publish.
pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    context: &MutationContext,
) -> Result<DeleteNamespaceResponse> {
    NamespaceCommitEngine::new(namespace_id.clone())
        .delete_namespace(store, options, context)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::limits::WAL_PUBLISH_BUDGET_MS;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::read_head_object;
    use futures::StreamExt;
    use loonfs_api::{ChangeSeq, ContentRef, ContentStoreId, WriterEpoch};
    use loonfs_objectstore::keys::wal_segment_prefix;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::stores::{CountingStore, OperationClass};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    fn context(writer_id: &str) -> MutationContext {
        MutationContext {
            writer_id: writer_id.to_owned(),
            now_ms: 1_000,
        }
    }

    fn create_dir_request(commit_id: &str, name: &str) -> CommitRequest {
        CommitRequest::single(
            CommitId::parse(commit_id).expect("valid commit id"),
            None,
            FilesystemOperation::CreateDirectory {
                path: loonfs_api::AbsolutePath::parse(format!("/{name}")).expect("valid path"),
                parents: false,
            },
        )
    }

    #[test]
    fn semantic_identity_excludes_content_preparation() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let request = create_dir_request("same-mutation", "docs");
        let ready = CommitCandidate::new(request.clone());
        let rejected = CommitCandidate::rejected(
            request,
            ContentPreparationError::ContentToken(vec![(
                ContentId::generate(),
                ContentTokenError::Expired,
            )]),
        );

        assert_eq!(
            ready.semantic_identity(&namespace_id).expect("identity"),
            rejected.semantic_identity(&namespace_id).expect("identity")
        );
    }

    #[test]
    fn semantic_identity_ignores_current_request_limits() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let oversized_ops = CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("too-many-ops").expect("valid commit id"),
            message: None,
            operations: (0..=crate::limits::MAX_COMMIT_OPERATIONS)
                .map(|index| FilesystemOperation::CreateDirectory {
                    path: loonfs_api::AbsolutePath::parse(format!("/dir-{index}"))
                        .expect("valid path"),
                    parents: false,
                })
                .collect(),
        });
        oversized_ops
            .semantic_identity(&namespace_id)
            .expect("operation limits must not affect identity");

        let content_ref = ContentRef::blob_v1(ContentId::generate(), b"proof");
        let admission = ContentAdmission::for_durable_content_write(
            ContentStoreId::parse("cs_00000000000000000000000000000001").expect("content store id"),
            content_ref,
        );
        let prepared = PreparedContent::from_admission(admission);
        let oversized_proofs = CommitCandidate::prepared(
            create_dir_request("too-many-proofs", "docs"),
            vec![prepared; crate::limits::MAX_COMMIT_CONTENT_TOKENS + 1],
        );
        oversized_proofs
            .semantic_identity(&namespace_id)
            .expect("prepared proof limits must not affect identity");

        let oversized_message = CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("too-long-message").expect("valid commit id"),
            message: Some("m".repeat(crate::limits::MAX_COMMIT_MESSAGE_BYTES + 1)),
            operations: vec![FilesystemOperation::CreateDirectory {
                path: loonfs_api::AbsolutePath::parse("/docs").expect("valid path"),
                parents: false,
            }],
        });
        oversized_message
            .semantic_identity(&namespace_id)
            .expect("message limits must not affect identity");
    }

    /// A batch past the operation ceiling is refused before it can occupy
    /// the publisher.
    #[test]
    fn a_batch_past_the_operation_ceiling_is_rejected() {
        let oversized = CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("oversized-batch").expect("valid commit id"),
            message: None,
            operations: (0..=crate::limits::MAX_COMMIT_OPERATIONS)
                .map(|index| FilesystemOperation::CreateDirectory {
                    path: loonfs_api::AbsolutePath::parse(format!("/dir-{index}"))
                        .expect("valid path"),
                    parents: false,
                })
                .collect(),
        });

        let error = oversized
            .validate_request_limits()
            .expect_err("the batch is over the operation ceiling");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);

        let at_ceiling = CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("largest-batch").expect("valid commit id"),
            message: None,
            operations: (0..crate::limits::MAX_COMMIT_OPERATIONS)
                .map(|index| FilesystemOperation::CreateDirectory {
                    path: loonfs_api::AbsolutePath::parse(format!("/dir-{index}"))
                        .expect("valid path"),
                    parents: false,
                })
                .collect(),
        });
        at_ceiling
            .validate_request_limits()
            .expect("a batch at the ceiling is admitted");
    }

    /// A message past the byte ceiling is refused before it can enter the
    /// durable record or the fingerprint path.
    #[test]
    fn a_message_past_the_byte_ceiling_is_rejected() {
        let operations = vec![FilesystemOperation::CreateDirectory {
            path: loonfs_api::AbsolutePath::parse("/docs").expect("valid path"),
            parents: false,
        }];

        let oversized = CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("oversized-message").expect("valid commit id"),
            message: Some("m".repeat(crate::limits::MAX_COMMIT_MESSAGE_BYTES + 1)),
            operations: operations.clone(),
        });
        let error = oversized
            .validate_request_limits()
            .expect_err("the message is over the byte ceiling");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);

        let at_ceiling = CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("largest-message").expect("valid commit id"),
            message: Some("m".repeat(crate::limits::MAX_COMMIT_MESSAGE_BYTES)),
            operations,
        });
        at_ceiling
            .validate_request_limits()
            .expect("a message at the ceiling is admitted");
    }

    fn create_dir(commit_id: &str, display_name: &str) -> CommitCandidate {
        CommitCandidate::new(create_dir_request(commit_id, display_name))
    }

    async fn wal_segment_count(store: &LocalFsStore, namespace_id: &NamespaceId) -> usize {
        store
            .list_prefix_stream(&wal_segment_prefix(namespace_id.as_str()))
            .collect::<Vec<_>>()
            .await
            .len()
    }

    #[tokio::test]
    async fn commit_engine_is_terminally_fenced_after_takeover() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a");
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        let mut engine_a = NamespaceCommitEngine::new(namespace_id.clone());
        let first = engine_a
            .publish_batch(
                &store,
                vec![create_dir("from-a-first", "alpha")],
                &writer_a,
                &PublishTailOptions::default(),
            )
            .await;
        first.results[0].as_ref().expect("writer a first commit");

        // Writer B's session acquires the epoch; A's cached epoch is now
        // superseded.
        let writer_b = context("writer-b");
        let mut engine_b = NamespaceCommitEngine::new(namespace_id.clone());
        let takeover = engine_b
            .publish_batch(
                &store,
                vec![create_dir("from-b-first", "beta")],
                &writer_b,
                &PublishTailOptions::default(),
            )
            .await;
        takeover.results[0]
            .as_ref()
            .expect("writer b takeover commit");
        let epoch_after_takeover = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state
            .writer_epoch;

        // A is fenced terminally: both attempts fail with writer_fenced, the
        // second without ever reaching the store, and the session never
        // bumps the epoch back.
        for attempt in 0..2 {
            let fenced = engine_a
                .publish_batch(
                    &store,
                    vec![create_dir("from-a-second", "gamma")],
                    &writer_a,
                    &PublishTailOptions::default(),
                )
                .await;
            let error = fenced.results[0].as_ref().expect_err("fenced publish");
            assert_eq!(error.code(), ErrorCode::WriterFenced, "attempt {attempt}");
        }
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state;
        assert_eq!(head.writer_epoch, epoch_after_takeover);
        assert_eq!(head.writer.expect("writer block").writer_id, "writer-b");
    }

    /// A takeover that lands while the loser is mid-load is still a fence,
    /// not a head race. The loser must be told so — `stale_head` would send a
    /// permanently fenced session back to retry.
    #[tokio::test]
    async fn fencing_during_publish_view_load_still_reports_writer_fenced() {
        use loonfs_test_support::stores::{BlockingStore, KeyPredicate};
        use std::sync::Arc as StdArc;

        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a");

        // Block the WAL tail read: it sits between the head snapshot that the
        // fence check uses and the closing etag recheck.
        let store = StdArc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::prefix(wal_segment_prefix(namespace_id.as_str())),
            OperationClass::Read,
        ));

        bootstrap_namespace(store.inner(), &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        let mut engine_a = NamespaceCommitEngine::new(namespace_id.clone());
        engine_a
            .publish_batch(
                store.inner(),
                vec![create_dir("from-a-first", "alpha")],
                &writer_a,
                &PublishTailOptions::default(),
            )
            .await
            .results[0]
            .as_ref()
            .expect("writer a first commit");
        // Force a fresh manifest load on the next publish.
        engine_a.invalidate_projection();

        store.block_next();
        let blocked_store = StdArc::clone(&store);
        let publish_a = tokio::spawn(async move {
            let mut engine = engine_a;
            let result = engine
                .publish_batch(
                    blocked_store.as_ref(),
                    vec![create_dir("from-a-second", "gamma")],
                    &writer_a,
                    &PublishTailOptions::default(),
                )
                .await;
            result.results[0].as_ref().err().map(|error| error.code())
        });

        // A has snapshotted a head that still names it. Writer B takes the
        // epoch while A is parked mid-load, so A is fenced by the time it
        // rechecks the etag.
        store.wait_until_blocked().await;
        let writer_b = context("writer-b");
        let mut engine_b = NamespaceCommitEngine::new(namespace_id.clone());
        engine_b
            .publish_batch(
                store.inner(),
                vec![create_dir("from-b-first", "beta")],
                &writer_b,
                &PublishTailOptions::default(),
            )
            .await
            .results[0]
            .as_ref()
            .expect("writer b takeover commit");
        store.release();

        let code = publish_a.await.expect("join publish a");
        assert_eq!(code, Some(ErrorCode::WriterFenced));
    }

    #[tokio::test]
    async fn shared_session_keeps_fencing_across_engine_rebuilds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a");
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        let session = SharedWriterSessionState::default();
        let mut engine_a1 =
            NamespaceCommitEngine::new(namespace_id.clone()).writer_session(Arc::clone(&session));
        engine_a1
            .publish_batch(
                &store,
                vec![create_dir("from-a-first", "alpha")],
                &writer_a,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .remove(0)
            .expect("writer a first commit");

        let writer_b = context("writer-b");
        let mut engine_b = NamespaceCommitEngine::new(namespace_id.clone());
        engine_b
            .publish_batch(
                &store,
                vec![create_dir("from-b-first", "beta")],
                &writer_b,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .remove(0)
            .expect("writer b takeover commit");

        let fenced = engine_a1
            .publish_batch(
                &store,
                vec![create_dir("from-a-second", "gamma")],
                &writer_a,
                &PublishTailOptions::default(),
            )
            .await;
        let error = fenced.results[0].as_ref().expect_err("fenced publish");
        assert_eq!(error.code(), ErrorCode::WriterFenced);
        let epoch_after_fencing = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state
            .writer_epoch;

        // A rebuilt engine — cache eviction, cache-disabled mode — shares
        // the session state, so the session stays terminally fenced and
        // never touches the head.
        drop(engine_a1);
        let mut engine_a2 =
            NamespaceCommitEngine::new(namespace_id.clone()).writer_session(session);
        let still_fenced = engine_a2
            .publish_batch(
                &store,
                vec![create_dir("from-a-third", "delta")],
                &writer_a,
                &PublishTailOptions::default(),
            )
            .await;
        let error = still_fenced.results[0]
            .as_ref()
            .expect_err("rebuilt engine stays fenced");
        assert_eq!(error.code(), ErrorCode::WriterFenced);
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state;
        assert_eq!(head.writer_epoch, epoch_after_fencing);
        assert_eq!(head.writer.expect("writer block").writer_id, "writer-b");
    }

    /// Advances an entire publish budget per reading, so every publish
    /// observes an expired budget between segment PUT and head CAS.
    #[derive(Debug)]
    struct ExpiredBudgetTimer(AtomicU64);

    impl MonotonicTimer for ExpiredBudgetTimer {
        fn monotonic_now_ms(&self) -> u64 {
            self.0
                .fetch_add(WAL_PUBLISH_BUDGET_MS + 1_000, Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn publish_over_budget_abandons_the_segment_and_a_retry_rebuilds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a");
        bootstrap_namespace(&store, &namespace_id, &writer, false)
            .await
            .expect("bootstrap");
        let head_before = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state;

        let mut over_budget = NamespaceCommitEngine::new(namespace_id.clone())
            .monotonic_timer(Arc::new(ExpiredBudgetTimer(AtomicU64::new(0))));
        let abandoned = over_budget
            .publish_batch(
                &store,
                vec![create_dir("budgeted", "alpha")],
                &writer,
                &PublishTailOptions::default(),
            )
            .await;
        let error = abandoned.results[0]
            .as_ref()
            .expect_err("over-budget publish must abandon");
        assert!(
            matches!(
                error,
                CoreError::HeadPublish(
                    crate::commit::CommitHeadPublishError::PublishBudgetExceeded { .. }
                )
            ),
            "unexpected error: {error:?}"
        );
        // Retryable exactly like a stale head, so existing retry loops
        // rebuild the commit.
        assert_eq!(error.code(), ErrorCode::StaleHead);

        // The head did not advance; the written segment is an orphan for GC.
        let head_after = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state;
        assert_eq!(head_after.seq, head_before.seq);
        assert_eq!(head_after.visible_wal_tip, head_before.visible_wal_tip);
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 1);

        // A retry with a healthy budget republishes the same commit as a
        // fresh segment; the orphan stays behind.
        let mut healthy = NamespaceCommitEngine::new(namespace_id.clone());
        let retried = healthy
            .publish_batch(
                &store,
                vec![create_dir("budgeted", "alpha")],
                &writer,
                &PublishTailOptions::default(),
            )
            .await;
        let response = retried.results[0].as_ref().expect("rebuilt publish");
        assert_eq!(response.committed_seq, ChangeSeq(1));
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 2);
        let head_final = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state;
        assert_eq!(head_final.seq, ChangeSeq(1));
        // Two engines are two sessions, and each acquires its own epoch: the
        // abandoned attempt took 1, the retry took 2.
        assert_eq!(head_final.writer_epoch, WriterEpoch(2));
    }

    #[tokio::test]
    async fn publish_views_reuse_cached_table_blocks_across_publishes() {
        use crate::cache::MetadataTableCacheConfig;
        let temp_dir = tempdir().expect("tempdir");
        let store =
            CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a");
        bootstrap_namespace(&store, &namespace_id, &writer, false)
            .await
            .expect("bootstrap");
        let mut seed = NamespaceCommitEngine::new(namespace_id.clone());
        seed.publish_batch(
            &store,
            vec![create_dir("seed-commit", "docs")],
            &writer,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .remove(0)
        .expect("seed publish");
        crate::checkpoint::create_checkpoint(
            &store,
            &namespace_id,
            loonfs_api::wire::control::CheckpointOwner::User {
                name: "test-pin".to_owned(),
            },
            None,
            &writer,
        )
        .await
        .expect("checkpoint");

        // Without a cache, every publish view re-fetches the table blocks
        // its validation walks need.
        let mut uncached = NamespaceCommitEngine::new(namespace_id.clone());
        store.reset();
        uncached
            .publish_batch(
                &store,
                vec![create_dir("uncached-a", "alpha")],
                &writer,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .remove(0)
            .expect("uncached publish a");
        assert!(
            store.count(OperationClass::Read) > 0,
            "publish validation should read table blocks"
        );
        store.reset();
        uncached
            .publish_batch(
                &store,
                vec![create_dir("uncached-b", "beta")],
                &writer,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .remove(0)
            .expect("uncached publish b");
        assert!(
            store.count(OperationClass::Read) > 0,
            "without a cache the next publish re-fetches the same blocks"
        );

        let cache = Arc::new(MetadataTableCache::new(MetadataTableCacheConfig::default()));
        let mut cached = NamespaceCommitEngine::new(namespace_id.clone()).table_cache(cache);
        store.reset();
        cached
            .publish_batch(
                &store,
                vec![create_dir("cached-a", "gamma")],
                &writer,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .remove(0)
            .expect("cached publish a");
        assert!(
            store.count(OperationClass::Read) > 0,
            "the first cached publish fills the cache"
        );
        store.reset();
        cached
            .publish_batch(
                &store,
                vec![create_dir("cached-b", "delta")],
                &writer,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .remove(0)
            .expect("cached publish b");
        assert_eq!(
            store.count(OperationClass::Read),
            0,
            "a warm cache serves every publish-view table read"
        );
    }
}
