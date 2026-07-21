//! [`NamespaceCommitEngine`]: publishes batches of classified mutation
//! candidates — one WAL segment, one head compare-and-swap, one result per
//! candidate.

use crate::checkpoint::MetadataTableCache;
use crate::commit::{core_commit_fingerprint_for_v0_request, SemanticMutationIdentity};
use crate::context::MutationContext;
use crate::error::{
    CoreError, MetadataProjectionLoadError, MetadataViewError, Result, WriterFence,
};
use crate::metadata::MetadataState;
use crate::namespace::catalog::{load_namespace_catalog_entry, VerifiedNamespaceCatalogEntry};
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::write::{path_intent_fingerprint_for_path_intent, PathMutationIntent};
use crate::protocol::{load_publish_metadata_view, PublishTailOptions, PublishTailProjection};
use crate::storage::content_admission::{ContentAdmission, ContentTokenError, PreparedContent};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loonfs_api::wire::control::{AcquiredWriter, HeadState};
use loonfs_api::{ChangeSeq, CommitId, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// One namespace mutation together with the result of preparing any content
/// it references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceMutationCandidate {
    mutation: NamespaceMutation,
    content: ContentPreparation,
}

/// The semantic mutation carried by a publication candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceMutation {
    Commit(ApiCommitRequest),
    Path(PathMutationIntent),
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
    /// A supplied wire token was rejected before publication.
    #[error("content token was rejected: {0}")]
    ContentToken(#[from] ContentTokenError),
    /// No prepared proof covers the referenced content.
    #[error("content ref `{content_ref_digest}` is not prepared for publication")]
    ContentNotPrepared { content_ref_digest: String },
}

impl NamespaceMutationCandidate {
    /// Wraps an explicit semantic commit with no attached content proofs.
    pub fn commit(request: ApiCommitRequest) -> Self {
        Self {
            mutation: NamespaceMutation::Commit(request),
            content: ContentPreparation::Ready(Vec::new()),
        }
    }

    /// Wraps an explicit semantic commit with opaque proofs for its prepared
    /// content.
    pub fn commit_prepared(request: ApiCommitRequest, content: Vec<PreparedContent>) -> Self {
        Self {
            mutation: NamespaceMutation::Commit(request),
            content: ContentPreparation::Ready(
                content
                    .into_iter()
                    .map(PreparedContent::into_admission)
                    .collect(),
            ),
        }
    }

    /// Wraps a path mutation with no attached content proofs.
    pub fn path(intent: PathMutationIntent) -> Self {
        Self {
            mutation: NamespaceMutation::Path(intent),
            content: ContentPreparation::Ready(Vec::new()),
        }
    }

    /// Wraps a path mutation with opaque proofs for its prepared content.
    pub fn path_prepared(intent: PathMutationIntent, content: Vec<PreparedContent>) -> Self {
        Self {
            mutation: NamespaceMutation::Path(intent),
            content: ContentPreparation::Ready(
                content
                    .into_iter()
                    .map(PreparedContent::into_admission)
                    .collect(),
            ),
        }
    }

    /// Wraps a semantic mutation whose content preparation failed.
    pub fn rejected(mutation: NamespaceMutation, error: ContentPreparationError) -> Self {
        Self {
            mutation,
            content: ContentPreparation::Rejected(error),
        }
    }

    pub(crate) fn mutation(&self) -> &NamespaceMutation {
        &self.mutation
    }

    pub(crate) fn content_preparation(&self) -> &ContentPreparation {
        &self.content
    }

    /// Returns the idempotency key carried by the semantic mutation.
    pub fn commit_id(&self) -> &CommitId {
        match &self.mutation {
            NamespaceMutation::Commit(request) => &request.commit_id,
            NamespaceMutation::Path(intent) => intent.commit_id(),
        }
    }

    /// Computes semantic identity from the mutation alone.
    pub fn semantic_identity(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<SemanticMutationIdentity> {
        self.validate_request_limits()?;
        match &self.mutation {
            NamespaceMutation::Commit(request) => {
                core_commit_fingerprint_for_v0_request(namespace_id, request)
                    .map(SemanticMutationIdentity::CoreCommit)
                    .map_err(|err| {
                        CoreError::Internal(format!("failed to fingerprint commit request: {err}"))
                    })
            }
            NamespaceMutation::Path(intent) => {
                path_intent_fingerprint_for_path_intent(namespace_id, intent)
                    .map(SemanticMutationIdentity::PathIntent)
            }
        }
    }

    pub(crate) fn validate_request_limits(&self) -> Result<()> {
        let NamespaceMutation::Commit(request) = &self.mutation else {
            return Ok(());
        };
        if request.ops.len() > crate::limits::MAX_COMMIT_OPERATIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "commit has {} operations; maximum is {}",
                request.ops.len(),
                crate::limits::MAX_COMMIT_OPERATIONS
            )));
        }
        let prepared_count = match &self.content {
            ContentPreparation::Ready(content) => content.len(),
            ContentPreparation::Rejected(_) => 0,
        };
        if prepared_count > crate::limits::MAX_COMMIT_CONTENT_TOKENS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "commit has {prepared_count} prepared content proofs; maximum is {}",
                crate::limits::MAX_COMMIT_CONTENT_TOKENS
            )));
        }
        let distinct_content_refs = request
            .ops
            .iter()
            .filter_map(|op| match op {
                loonfs_api::v0::CommitOp::CreateFile { content_ref, .. }
                | loonfs_api::v0::CommitOp::ReplaceFile { content_ref, .. } => Some(content_ref),
                _ => None,
            })
            .collect::<HashSet<_>>()
            .len();
        if distinct_content_refs > crate::limits::MAX_COMMIT_EXTERNAL_CONTENT_REFS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "commit references {distinct_content_refs} distinct external content refs; maximum is {}",
                crate::limits::MAX_COMMIT_EXTERNAL_CONTENT_REFS
            )));
        }
        Ok(())
    }
}

/// The WAL-tail maintenance policy: one authority for "when do we
/// checkpoint?" and "when do we stop accepting writes?", so the two
/// thresholds cannot drift apart.
///
/// Reads never gate on tail length; the rejection only asks writers to wait
/// for the maintenance a deployment failed to run (format spec,
/// "Maintenance operations").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalTailPolicy {
    /// Visible WAL-tail length, in segments, at which a maintenance tick
    /// publishes a checkpoint. The tick fires at or past this length.
    pub checkpoint_at_segments: u64,
    /// Visible WAL-tail length past which (strictly greater than) every
    /// publish surface rejects with `maintenance_required`.
    pub reject_writes_at_segments: u64,
}

impl WalTailPolicy {
    /// The workspace policy: checkpoint at 32 segments, reject past 128.
    pub const DEFAULT: Self = Self {
        checkpoint_at_segments: 32,
        reject_writes_at_segments: 128,
    };
}

impl Default for WalTailPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

// The ordering invariant `0 < checkpoint < reject` holds by construction:
// a tick must be able to relieve backpressure before writes stop.
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
    /// The read state this publish produced, present when the head CAS
    /// landed unambiguously: callers can seed read caches with it instead
    /// of invalidating them and rebuilding from the store.
    pub resulting_read_state: Option<ResultingReadState>,
}

/// A read anchor plus the projected WAL tail as of one landed publish.
#[derive(Debug, Clone)]
pub struct ResultingReadState {
    pub head: HeadState,
    pub head_etag: String,
    pub manifest_id: ManifestId,
    pub manifest_object_id: ManifestObjectId,
    pub manifest_head_seq: ChangeSeq,
    pub tail_rows: Arc<MetadataState>,
}

/// Writer-session state for one namespace: the epoch this session acquired
/// and its terminal fencing record.
///
/// This is authoritative session state, not a cache of durable state —
/// nothing in the store can rebuild "this session was fenced". Runtimes keep
/// one shared instance per namespace in a registry that outlives every
/// rebuildable cache (invalidation, LRU eviction, cache-disabled
/// configurations) and hand it to each engine they build for the namespace.
/// An engine built without one gets private state, which keeps the
/// documented one-shot semantics: each one-shot commit is its own
/// acquisition decision.
#[derive(Debug, Default)]
pub struct WriterSessionState {
    /// Epoch acquired lazily on this session's first publish and reused for
    /// its lifetime; no per-publish acquisition CAS.
    acquired_writer: Option<AcquiredWriter>,
    /// Terminal fencing record. Once another session supersedes our epoch,
    /// every later publish fails with `writer_fenced` without touching the
    /// store; the session never reacquires on its own. Reacquisition is an
    /// explicit caller decision, left to a future takeover API.
    fenced: Option<WriterFence>,
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
    /// Shared decoded-block cache for publish-view table reads. Blocks are
    /// content-addressed by segment digest, so cached entries can never
    /// serve stale state; freshness stays enforced by the head etag check.
    table_cache: Option<Arc<MetadataTableCache>>,
    /// The namespace's immutable catalog pair, loaded on this session's
    /// first publish and reused for its lifetime.
    catalog_entry: Option<VerifiedNamespaceCatalogEntry>,
}

impl NamespaceCommitEngine {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            publish_tail_projection: None,
            session: SharedWriterSessionState::default(),
            timer: Arc::new(StdMonotonicTimer::default()),
            table_cache: None,
            catalog_entry: None,
        }
    }

    /// Attaches the runtime's session state for this namespace, so the
    /// acquired epoch and fencing outlive this engine instance.
    pub fn writer_session(mut self, session: SharedWriterSessionState) -> Self {
        self.session = session;
        self
    }

    fn lock_session(&self) -> std::sync::MutexGuard<'_, WriterSessionState> {
        // Poisoning is propagated as a panic: every critical section is a
        // plain field read or write, so a poisoned lock means another
        // thread panicked mid-update.
        self.session
            .lock()
            .expect("writer session state lock poisoned")
    }

    #[cfg(test)]
    pub(crate) fn with_monotonic_timer(mut self, timer: Arc<dyn MonotonicTimer>) -> Self {
        self.timer = timer;
        self
    }

    pub fn table_cache(mut self, table_cache: Arc<MetadataTableCache>) -> Self {
        self.table_cache = Some(table_cache);
        self
    }

    pub fn catalog_entry(mut self, catalog_entry: VerifiedNamespaceCatalogEntry) -> Self {
        self.catalog_entry = Some(catalog_entry);
        self
    }

    pub fn invalidate(&mut self) {
        // Drops only the tail projection. The acquired epoch and fencing
        // are session state, not cached state: the epoch's validity is
        // re-checked against the head on every publish view load, and a
        // fenced session stays fenced.
        self.publish_tail_projection = None;
    }

    pub async fn publish_batch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
    ) -> NamespaceCommitEnginePublishResult {
        self.publish_batch_with_tail_options(
            store,
            candidates,
            context,
            &PublishTailOptions::default(),
        )
        .await
    }

    pub async fn publish_batch_with_tail_options<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
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
        let already_acquired = {
            let session = self.lock_session();
            if let Some(fence) = &session.fenced {
                let fence = fence.clone();
                drop(session);
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, CoreError::WriterFenced(fence)),
                    wal_tail_segments: 0,
                    resulting_read_state: None,
                };
            }
            session.acquired_writer.clone()
        };
        let acquired_writer = match already_acquired {
            Some(value) => value,
            None => match acquire_writer_epoch(store, &self.namespace_id, context).await {
                Ok(value) => {
                    let mut session = self.lock_session();
                    if let Some(fence) = session.fenced.clone() {
                        // Another engine sharing this session observed
                        // fencing while we were acquiring; the session
                        // stays fenced.
                        drop(session);
                        return NamespaceCommitEnginePublishResult {
                            results: repeated_error(
                                candidate_count,
                                CoreError::WriterFenced(fence),
                            ),
                            wal_tail_segments: 0,
                            resulting_read_state: None,
                        };
                    }
                    session.acquired_writer = Some(value.clone());
                    drop(session);
                    value
                }
                Err(error) => {
                    return NamespaceCommitEnginePublishResult {
                        results: repeated_error(candidate_count, CoreError::WriterEpoch(error)),
                        wal_tail_segments: 0,
                        resulting_read_state: None,
                    };
                }
            },
        };

        let catalog_entry = match &self.catalog_entry {
            Some(value) => value.clone(),
            None => match load_namespace_catalog_entry(store, &self.namespace_id).await {
                Ok(value) => {
                    self.catalog_entry = Some(value.clone());
                    value
                }
                Err(error) => {
                    return NamespaceCommitEnginePublishResult {
                        results: repeated_error(
                            candidate_count,
                            CoreError::MetadataProjection(MetadataProjectionLoadError::from(error)),
                        ),
                        wal_tail_segments: 0,
                        resulting_read_state: None,
                    };
                }
            },
        };

        let (publish_view, projection) = match load_publish_metadata_view(
            store,
            self.table_cache.as_deref(),
            Some(catalog_entry),
            &self.namespace_id,
            Some(acquired_writer),
            self.publish_tail_projection.as_ref(),
            tail_options,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                self.invalidate();
                if let CoreError::WriterFenced(fence) = &error {
                    let mut session = self.lock_session();
                    session.fenced = Some(fence.clone());
                    session.acquired_writer = None;
                }
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, error),
                    wal_tail_segments: 0,
                    resulting_read_state: None,
                };
            }
        };

        let reject_writes_at_segments = WalTailPolicy::DEFAULT.reject_writes_at_segments;
        if projection.wal_tail_segments > reject_writes_at_segments {
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

        let published = crate::protocol::publish_namespace_mutations_batch_against_publish_view(
            store,
            &self.namespace_id,
            &candidates,
            context,
            &publish_view,
            self.timer.as_ref(),
        )
        .await;
        let resulting_head = published.resulting_head.clone();
        let wal_tail_segments =
            self.update_publish_tail_projection(projection, &published, tail_options);
        // Seedable only when the CAS landed unambiguously and the updated
        // projection survived (it carries the post-publish tail and etag).
        let resulting_read_state = match (resulting_head, self.publish_tail_projection.as_ref()) {
            (Some(head), Some(projection)) if projection.head_seq == head.seq => {
                Some(ResultingReadState {
                    head,
                    head_etag: projection.head_etag.clone(),
                    manifest_id: projection.manifest_id,
                    manifest_object_id: projection.manifest_object_id.clone(),
                    manifest_head_seq: projection.manifest_head_seq,
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

    fn update_publish_tail_projection(
        &mut self,
        mut projection: PublishTailProjection,
        published: &crate::protocol::PublishBatchAgainstViewResult,
        tail_options: &PublishTailOptions,
    ) -> u64 {
        if !published.published_records.is_empty() {
            projection.wal_tail_segments = projection.wal_tail_segments.saturating_add(1);
        }
        let wal_tail_segments = projection.wal_tail_segments;
        let Some(resulting_head) = published.resulting_head.clone() else {
            if published.can_reuse_loaded_projection {
                self.publish_tail_projection = Some(projection);
            } else {
                self.invalidate();
            }
            return wal_tail_segments;
        };
        let Some(resulting_head_etag) = published.resulting_head_etag.clone() else {
            self.invalidate();
            return wal_tail_segments;
        };
        for record in &published.published_records {
            projection.tail_state.apply_committed_wal_record_mut(record);
        }
        projection.head_seq = resulting_head.seq;
        projection.head_etag = resulting_head_etag;
        if projection.within_limits(tail_options) {
            self.publish_tail_projection = Some(projection);
        } else {
            self.invalidate();
        }
        wal_tail_segments
    }
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<ApiCommitResponse>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

/// Publishes one batch through a fresh, uncached commit engine.
pub(crate) async fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse>> {
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch(store, candidates, context)
        .await
        .results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{mint_content_token, verify_content_token};
    use crate::error::ErrorCode;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::read_head_object;
    use futures::StreamExt;
    use loonfs_api::v0::ValidatedContentToken;
    use loonfs_api::{ChangeSeq, ContentRef, InodeId, WriterEpoch};
    use loonfs_objectstore::keys::wal_segment_prefix;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    fn context(writer_id: &str, writer_session_id: &str) -> MutationContext {
        MutationContext {
            writer_id: writer_id.to_owned(),
            writer_session_id: writer_session_id.to_owned(),
            writer_version: "publisher-test/0.1.0".to_owned(),
            now_ms: 1_000,
        }
    }

    #[test]
    fn semantic_identity_excludes_content_preparation() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let intent = PathMutationIntent::CreateDir {
            commit_id: CommitId::parse("same-mutation").expect("valid commit id"),
            absolute_path: loonfs_api::AbsolutePath::parse("/docs").expect("path"),
            parents: false,
        };
        let ready = NamespaceMutationCandidate::path(intent.clone());
        let rejected = NamespaceMutationCandidate::rejected(
            NamespaceMutation::Path(intent),
            ContentPreparationError::ContentToken(ContentTokenError::Expired),
        );

        assert_eq!(
            ready.semantic_identity(&namespace_id).expect("identity"),
            rejected.semantic_identity(&namespace_id).expect("identity")
        );
    }

    #[test]
    fn candidate_validation_rejects_commit_operation_and_prepared_proof_limits() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let oversized_ops = NamespaceMutationCandidate::commit(ApiCommitRequest {
            commit_id: CommitId::parse("too-many-ops").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: (0..=crate::limits::MAX_COMMIT_OPERATIONS)
                .map(|index| loonfs_api::v0::CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: format!("dir-{index}"),
                })
                .collect(),
            message: None,
        });
        let operations_error = oversized_ops
            .semantic_identity(&namespace_id)
            .expect_err("operation limit must reject before admission");
        assert_eq!(operations_error.code(), ErrorCode::InvalidRequest);

        let content_ref = ContentRef::whole_file_v0(b"proof");
        let token =
            mint_content_token("secret", &namespace_id, &content_ref, 1_000).expect("mint token");
        let prepared = verify_content_token(
            "secret",
            &namespace_id,
            &ValidatedContentToken { content_ref, token },
            1_000,
        )
        .expect("verify token");
        let oversized_proofs = NamespaceMutationCandidate::commit_prepared(
            ApiCommitRequest {
                commit_id: CommitId::parse("too-many-proofs").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![loonfs_api::v0::CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: "docs".to_owned(),
                }],
                message: None,
            },
            vec![prepared; crate::limits::MAX_COMMIT_CONTENT_TOKENS + 1],
        );
        let proofs_error = oversized_proofs
            .semantic_identity(&namespace_id)
            .expect_err("prepared proof limit must reject before admission");
        assert_eq!(proofs_error.code(), ErrorCode::InvalidRequest);
    }

    fn create_dir(commit_id: &str, display_name: &str) -> NamespaceMutationCandidate {
        NamespaceMutationCandidate::commit(ApiCommitRequest {
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![loonfs_api::v0::CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: display_name.to_owned(),
            }],
            message: None,
        })
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
        let writer_a = context("writer-a", "session-a");
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        let mut engine_a = NamespaceCommitEngine::new(namespace_id.clone());
        let first = engine_a
            .publish_batch(&store, vec![create_dir("from-a-first", "alpha")], &writer_a)
            .await;
        first.results[0].as_ref().expect("writer a first commit");

        // Writer B's session acquires the epoch; A's cached epoch is now
        // superseded.
        let writer_b = context("writer-b", "session-b");
        let mut engine_b = NamespaceCommitEngine::new(namespace_id.clone());
        let takeover = engine_b
            .publish_batch(&store, vec![create_dir("from-b-first", "beta")], &writer_b)
            .await;
        takeover.results[0]
            .as_ref()
            .expect("writer b takeover commit");
        let epoch_after_takeover = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
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
                )
                .await;
            let error = fenced.results[0].as_ref().expect_err("fenced publish");
            assert_eq!(error.code(), ErrorCode::WriterFenced, "attempt {attempt}");
        }
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.writer_epoch, epoch_after_takeover);
        assert_eq!(head.writer.expect("writer block").writer_id, "writer-b");
    }

    #[tokio::test]
    async fn shared_session_keeps_fencing_across_engine_rebuilds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a", "session-a");
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        let session = SharedWriterSessionState::default();
        let mut engine_a1 =
            NamespaceCommitEngine::new(namespace_id.clone()).writer_session(Arc::clone(&session));
        engine_a1
            .publish_batch(&store, vec![create_dir("from-a-first", "alpha")], &writer_a)
            .await
            .results
            .remove(0)
            .expect("writer a first commit");

        let writer_b = context("writer-b", "session-b");
        let mut engine_b = NamespaceCommitEngine::new(namespace_id.clone());
        engine_b
            .publish_batch(&store, vec![create_dir("from-b-first", "beta")], &writer_b)
            .await
            .results
            .remove(0)
            .expect("writer b takeover commit");

        let fenced = engine_a1
            .publish_batch(
                &store,
                vec![create_dir("from-a-second", "gamma")],
                &writer_a,
            )
            .await;
        let error = fenced.results[0].as_ref().expect_err("fenced publish");
        assert_eq!(error.code(), ErrorCode::WriterFenced);
        let epoch_after_fencing = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state
            .writer_epoch;

        // A rebuilt engine — cache eviction, cache-disabled mode — shares
        // the session state, so the session stays terminally fenced and
        // never touches the head.
        drop(engine_a1);
        let mut engine_a2 =
            NamespaceCommitEngine::new(namespace_id.clone()).writer_session(session);
        let still_fenced = engine_a2
            .publish_batch(&store, vec![create_dir("from-a-third", "delta")], &writer_a)
            .await;
        let error = still_fenced.results[0]
            .as_ref()
            .expect_err("rebuilt engine stays fenced");
        assert_eq!(error.code(), ErrorCode::WriterFenced);
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
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
                .fetch_add(crate::protocol::PUBLISH_BUDGET_MS + 1_000, Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn publish_over_budget_abandons_the_segment_and_a_retry_rebuilds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a", "session-a");
        bootstrap_namespace(&store, &namespace_id, &writer, false)
            .await
            .expect("bootstrap");
        let head_before = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;

        let mut over_budget = NamespaceCommitEngine::new(namespace_id.clone())
            .with_monotonic_timer(Arc::new(ExpiredBudgetTimer(AtomicU64::new(0))));
        let abandoned = over_budget
            .publish_batch(&store, vec![create_dir("budgeted", "alpha")], &writer)
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
            .envelope
            .state;
        assert_eq!(head_after.seq, head_before.seq);
        assert_eq!(head_after.visible_wal_tip, head_before.visible_wal_tip);
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 1);

        // A retry with a healthy budget republishes the same commit as a
        // fresh segment; the orphan stays behind.
        let mut healthy = NamespaceCommitEngine::new(namespace_id.clone());
        let retried = healthy
            .publish_batch(&store, vec![create_dir("budgeted", "alpha")], &writer)
            .await;
        let response = retried.results[0].as_ref().expect("rebuilt publish");
        assert_eq!(response.committed_seq, ChangeSeq(1));
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 2);
        let head_final = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head_final.seq, ChangeSeq(1));
        assert_eq!(head_final.writer_epoch, WriterEpoch(0));
    }

    #[tokio::test]
    async fn publish_views_reuse_cached_table_blocks_across_publishes() {
        use crate::cache::MetadataTableCacheConfig;
        use crate::checkpoint::tests::MetadataSstGetCountingStore;

        let temp_dir = tempdir().expect("tempdir");
        let store =
            MetadataSstGetCountingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a", "session-a");
        bootstrap_namespace(&store, &namespace_id, &writer, false)
            .await
            .expect("bootstrap");
        let mut seed = NamespaceCommitEngine::new(namespace_id.clone());
        seed.publish_batch(&store, vec![create_dir("seed-commit", "docs")], &writer)
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
        store.reset_metadata_sst_gets();
        uncached
            .publish_batch(&store, vec![create_dir("uncached-a", "alpha")], &writer)
            .await
            .results
            .remove(0)
            .expect("uncached publish a");
        assert!(
            store.metadata_sst_gets() > 0,
            "publish validation should read table blocks"
        );
        store.reset_metadata_sst_gets();
        uncached
            .publish_batch(&store, vec![create_dir("uncached-b", "beta")], &writer)
            .await
            .results
            .remove(0)
            .expect("uncached publish b");
        assert!(
            store.metadata_sst_gets() > 0,
            "without a cache the next publish re-fetches the same blocks"
        );

        let cache = Arc::new(MetadataTableCache::new(MetadataTableCacheConfig::default()));
        let mut cached = NamespaceCommitEngine::new(namespace_id.clone()).table_cache(cache);
        store.reset_metadata_sst_gets();
        cached
            .publish_batch(&store, vec![create_dir("cached-a", "gamma")], &writer)
            .await
            .results
            .remove(0)
            .expect("cached publish a");
        assert!(
            store.metadata_sst_gets() > 0,
            "the first cached publish fills the cache"
        );
        store.reset_metadata_sst_gets();
        cached
            .publish_batch(&store, vec![create_dir("cached-b", "delta")], &writer)
            .await
            .results
            .remove(0)
            .expect("cached publish b");
        assert_eq!(
            store.metadata_sst_gets(),
            0,
            "a warm cache serves every publish-view table read"
        );
    }
}
