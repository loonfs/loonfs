//! [`NamespaceCommitEngine`] publishes a batch of validated mutation
//! candidates as one numbered WAL put, then returns
//! one result per candidate.

use crate::authorize::CommitAuthority;
use crate::checkpoint::MetadataSegmentCache;
use crate::commit::CommitFingerprint;
use crate::context::MutationContext;
use crate::error::{CoreError, Result, WriterFence};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::options::DeleteNamespaceOptions;
use crate::path::write::{commit_fingerprint, CommitRequest, FilesystemOperation};
use crate::protocol::{
    load_publish_metadata_view, PublishTailOptions, PublishTailProjection, PublishTailWeight,
    PublishViewEffect,
};
use crate::storage::content_admission::{ContentTokenError, PreparedContent};
use crate::storage::inline_content::InlineContent;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::ProjectedWalTail;
use loonfs_api::v0::Commit;
use loonfs_api::wire::control::AcquiredWriter;
use loonfs_api::wire::wal::{MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES};
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
    maintenance: bool,
    inline_content: Vec<InlineContent>,
    inline_identity_content_ids: HashSet<ContentId>,
}

/// The result of preparing external content referenced by a mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContentPreparation {
    Ready(Vec<PreparedContent>),
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
    /// A commit no subject check applies to. Only maintenance builds one.
    pub fn maintenance(request: CommitRequest) -> Self {
        Self {
            request,
            content: ContentPreparation::Ready(Vec::new()),
            maintenance: true,
            inline_content: Vec::new(),
            inline_identity_content_ids: HashSet::new(),
        }
    }

    pub(crate) fn authority(&self) -> CommitAuthority<'_> {
        if self.maintenance {
            CommitAuthority::Maintenance
        } else {
            CommitAuthority::Subject(self.request.subject.as_ref())
        }
    }

    /// Wraps a mutation request with no attached content proofs.
    pub fn new(request: CommitRequest) -> Self {
        Self {
            request,
            content: ContentPreparation::Ready(Vec::new()),
            maintenance: false,
            inline_content: Vec::new(),
            inline_identity_content_ids: HashSet::new(),
        }
    }

    /// Wraps a mutation request with opaque proofs for its prepared content.
    pub fn prepared(request: CommitRequest, content: Vec<PreparedContent>) -> Self {
        let mut candidate = Self::new(request);
        let mut proofs = Vec::new();
        for value in content {
            match value.inline_content() {
                Some(inline) => candidate.inline_content.push(inline.clone()),
                None => proofs.push(value),
            }
        }
        candidate.content = ContentPreparation::Ready(proofs);
        candidate
    }

    /// Wraps a mutation request whose content preparation failed.
    pub fn rejected(request: CommitRequest, error: ContentPreparationError) -> Self {
        Self::new(request).reject_content_preparation(error)
    }

    /// Rejects new publication while preserving the identity needed for receipt replay.
    pub fn reject_content_preparation(mut self, error: ContentPreparationError) -> Self {
        self.inline_identity_content_ids.extend(
            self.inline_content
                .drain(..)
                .map(|value| value.content_ref().content_id.clone()),
        );
        self.content = ContentPreparation::Rejected(error);
        self
    }

    /// Takes `inline_content` as the content bytes carried by the commit.
    pub fn with_inline_content(
        request: CommitRequest,
        content: Vec<PreparedContent>,
        inline_content: Vec<InlineContent>,
    ) -> Self {
        let mut candidate = Self::prepared(request, content);
        candidate.inline_content.extend(inline_content);
        candidate
    }

    /// Returns values that publication still needs to carry in the WAL.
    pub fn inline_content(&self) -> &[InlineContent] {
        &self.inline_content
    }

    /// Lists inline values in the order their operations first name them.
    pub fn ordered_inline_content(&self, namespace_id: &NamespaceId) -> Result<Vec<InlineContent>> {
        let Some(namespace_generation) = self
            .inline_content
            .first()
            .map(|value| value.content_ref().owner_generation)
        else {
            return Ok(Vec::new());
        };
        crate::protocol::validate_inline_content_references(
            &self.request,
            &self.inline_content,
            namespace_id,
            namespace_generation,
        )?;
        let mut values: std::collections::HashMap<_, _> = self
            .inline_content
            .iter()
            .map(|value| (&value.content_ref().content_id, value))
            .collect();
        Ok(self
            .request
            .operations
            .iter()
            .filter_map(FilesystemOperation::content_ref)
            .filter_map(|reference| values.remove(&reference.content_id).cloned())
            .collect())
    }

    /// Replaces an inline value with a staged proof without changing its identity form.
    pub fn stage_inline_content(&mut self, content_id: &ContentId, proof: PreparedContent) {
        let reference = proof.content_ref();
        for operation in &mut self.request.operations {
            match operation {
                FilesystemOperation::PutFile {
                    content_ref: Some(content_ref),
                    ..
                }
                | FilesystemOperation::CreateFileByInode {
                    content_ref: Some(content_ref),
                    ..
                }
                | FilesystemOperation::PutFileRevisionByInode {
                    content_ref: Some(content_ref),
                    ..
                } if &content_ref.content_id == content_id => *content_ref = reference.clone(),
                _ => {}
            }
        }
        self.inline_content
            .retain(|value| &value.content_ref().content_id != content_id);
        self.inline_identity_content_ids
            .insert(reference.content_id.clone());
        if let ContentPreparation::Ready(proofs) = &mut self.content {
            proofs.push(proof);
        }
    }

    pub fn inline_content_bytes(&self) -> usize {
        self.inline_content.iter().fold(0usize, |total, value| {
            total.saturating_add(value.bytes().len())
        })
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

    /// Computes semantic identity without applying current operational request limits.
    pub fn semantic_identity(&self, namespace_id: &NamespaceId) -> Result<CommitFingerprint> {
        commit_fingerprint(
            namespace_id,
            &self.request,
            &self
                .inline_content
                .iter()
                .map(|value| value.content_ref().content_id.clone())
                .chain(self.inline_identity_content_ids.iter().cloned())
                .collect(),
        )
    }

    /// Estimates retained request data and prepared proofs for publication
    /// admission. Counts inline storage plus JSON payload bytes without
    /// allocating a second request. Allocator slack and publication working
    /// copies are excluded; this is a sizing policy, not a heap measurement.
    pub fn estimated_retained_bytes(&self) -> Result<usize> {
        self.estimated_retained_bytes_with_inline_content(&self.inline_content)
    }

    /// Estimates retained bytes after selected inline values are staged.
    pub fn estimated_retained_bytes_after_inline_staging(
        &self,
        namespace_id: &NamespaceId,
        retained_inline_content: &[InlineContent],
        staged_inline_content: &[InlineContent],
    ) -> Result<usize> {
        let mut bytes =
            self.estimated_retained_bytes_with_inline_content(retained_inline_content)?;
        if matches!(&self.content, ContentPreparation::Ready(_)) {
            bytes = bytes.saturating_add(
                staged_inline_content
                    .len()
                    .saturating_mul(std::mem::size_of::<PreparedContent>()),
            );
            for value in staged_inline_content {
                bytes = bytes
                    .saturating_add(PreparedContent::estimated_owned_staging_payload_bytes(
                        namespace_id,
                        value.content_ref(),
                    ))
                    .saturating_add(std::mem::size_of::<ContentId>())
                    .saturating_add(value.content_ref().content_id.as_str().len());
            }
        }
        Ok(bytes)
    }

    fn estimated_retained_bytes_with_inline_content(
        &self,
        retained_inline_content: &[InlineContent],
    ) -> Result<usize> {
        let mut bytes = RequestByteCounter(
            std::mem::size_of_val(self)
                .saturating_add(std::mem::size_of_val(self.request.operations.as_slice()))
                .saturating_add(std::mem::size_of_val(self.request.preconditions.as_slice())),
        );
        serde_json::to_writer(
            &mut bytes,
            &(
                &self.request.commit_id,
                &self.request.actor_id,
                &self.request.message,
                &self.request.operations,
                &self.request.preconditions,
            ),
        )
        .map_err(|error| CoreError::InvalidCommitRequest(error.to_string()))?;
        if let Some(subject) = &self.request.subject {
            serde_json::to_writer(&mut bytes, &subject.subject_id)
                .map_err(|error| CoreError::InvalidCommitRequest(error.to_string()))?;
            for principal_id in subject.principals.iter() {
                bytes.0 = bytes
                    .0
                    .saturating_add(std::mem::size_of::<loonfs_api::PrincipalId>());
                serde_json::to_writer(&mut bytes, principal_id)
                    .map_err(|error| CoreError::InvalidCommitRequest(error.to_string()))?;
            }
        }
        match &self.content {
            ContentPreparation::Ready(proofs) => {
                bytes.0 = bytes
                    .0
                    .saturating_add(std::mem::size_of_val(proofs.as_slice()));
                for proof in proofs {
                    bytes.0 = bytes.0.saturating_add(proof.estimated_payload_bytes());
                }
            }
            ContentPreparation::Rejected(ContentPreparationError::ContentToken(rejections)) => {
                bytes.0 = bytes
                    .0
                    .saturating_add(std::mem::size_of_val(rejections.as_slice()));
                for (content_id, error) in rejections {
                    bytes.0 = bytes.0.saturating_add(content_id.as_str().len());
                    if let ContentTokenError::Codec(message) = error {
                        bytes.0 = bytes.0.saturating_add(message.len());
                    }
                }
            }
            ContentPreparation::Rejected(ContentPreparationError::ContentNotPrepared {
                content_id,
            }) => {
                bytes.0 = bytes.0.saturating_add(content_id.as_str().len());
            }
        }
        bytes.0 = bytes
            .0
            .saturating_add(std::mem::size_of_val(retained_inline_content));
        for value in retained_inline_content {
            bytes.0 = bytes.0.saturating_add(value.bytes().len());
            serde_json::to_writer(&mut bytes, value.content_ref())
                .map_err(|error| CoreError::InvalidCommitRequest(error.to_string()))?;
        }
        for content_id in &self.inline_identity_content_ids {
            bytes.0 = bytes
                .0
                .saturating_add(std::mem::size_of::<ContentId>())
                .saturating_add(content_id.as_str().len());
        }
        Ok(bytes.0)
    }

    /// Bounds the bytes this request adds to an encoded WAL document, excluding
    /// the document envelope.
    pub fn wal_record_bytes_upper_bound(&self) -> usize {
        crate::commit_wal_size::estimated_wal_record_bytes(&self.request, &self.inline_content)
    }

    /// Bounds the WAL record after selected values are retained inline.
    pub fn wal_record_bytes_upper_bound_with_inline_content(
        &self,
        inline_content: &[InlineContent],
    ) -> usize {
        crate::commit_wal_size::estimated_wal_record_bytes(&self.request, inline_content)
    }

    pub(crate) fn validate_request_limits(&self) -> Result<()> {
        // Apply limits to the complete request because the serialized publisher
        // processes every operation before releasing the write path.
        self.validate_request_has_operations()?;
        if self.request.operations.len() > crate::limits::MAX_COMMIT_OPERATIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {} operations; maximum is {}",
                self.request.operations.len(),
                crate::limits::MAX_COMMIT_OPERATIONS
            )));
        }
        if self.request.preconditions.len() > crate::limits::MAX_COMMIT_PRECONDITIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {} preconditions; maximum is {}",
                self.request.preconditions.len(),
                crate::limits::MAX_COMMIT_PRECONDITIONS
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
        for value in &self.inline_content {
            let size_bytes = value.bytes().len();
            if size_bytes > MAX_WAL_INLINE_CONTENT_BYTES {
                let content_id = &value.content_ref().content_id;
                return Err(CoreError::InvalidCommitRequest(format!(
                    "inline content `{content_id}` is {size_bytes} bytes; maximum is {}",
                    MAX_WAL_INLINE_CONTENT_BYTES
                )));
            }
        }
        let inline_bytes = self.inline_content_bytes();
        if inline_bytes > MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {inline_bytes} inline content bytes; maximum is {}",
                MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES
            )));
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
            .filter_map(FilesystemOperation::content_ref)
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

    pub(crate) fn validate_request_has_operations(&self) -> Result<()> {
        if self.request.operations.is_empty() {
            return Err(CoreError::InvalidCommitRequest(
                "mutation request carries no operations".to_owned(),
            ));
        }
        Ok(())
    }
}

struct RequestByteCounter(usize);

impl std::io::Write for RequestByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishResult {
    pub results: Vec<Result<Commit>>,
    /// WAL tail length observed by this publish, for opportunistic
    /// maintenance scheduling. Zero when no projection was loaded.
    pub wal_tail_segments: u64,
    pub wal_tail_inline_bytes: usize,
    /// Whether this attempt loaded a tail that makes the two counts current.
    pub wal_tail_observed: bool,
    /// Read state produced by a successful, unambiguous WAL put. Callers can
    /// use it to update read caches without reloading from object storage.
    pub resulting_read_state: Option<ResultingReadState>,
}

/// The tail a fold publishes: the head it was replayed against, the basis
/// the manifest builds on, and the rows themselves. Cheap to take: an `Arc`
/// clone and two small clones.
#[derive(Debug, Clone)]
pub struct WalFoldInput {
    pub head: NamespaceReadState,
    pub basis: MetadataBasis,
    pub retention_floor_seq: ChangeSeq,
    pub tail_state: Arc<ProjectedWalTail>,
    pub wal_tail_segments: u64,
    pub wal_tail_inline_bytes: usize,
}

/// A read anchor plus the projected WAL tail as of one landed publish.
#[derive(Debug, Clone)]
pub struct ResultingReadState {
    pub head: NamespaceReadState,
    /// Metadata basis used for replay. The published head still references this
    /// basis, so a seeded read cache matches the next store-backed read.
    pub basis: MetadataBasis,
    pub manifest_head_seq: ChangeSeq,
    pub tail: Arc<ProjectedWalTail>,
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
    projection_loaded_ms: Option<u64>,
    /// This session's epoch and fencing for the namespace; see
    /// [`WriterSessionState`].
    session: SharedWriterSessionState,
    /// Local monotonic source for the self-enforced publish budget.
    timer: Arc<dyn MonotonicTimer>,
    /// Shared cache of decoded blocks used by publish-view reads. Blocks are
    /// keyed by segment digest. Successor probes verify that the view is current.
    segment_cache: Option<Arc<MetadataSegmentCache>>,
}

impl NamespaceCommitEngine {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            publish_tail_projection: None,
            projection_loaded_ms: None,
            session: SharedWriterSessionState::default(),
            timer: Arc::new(StdMonotonicTimer::default()),
            segment_cache: None,
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

    pub fn segment_cache(mut self, segment_cache: Arc<MetadataSegmentCache>) -> Self {
        self.segment_cache = Some(segment_cache);
        self
    }

    /// Clears only the rebuildable tail projection. Writer epoch and fencing
    /// remain in session state and are not reset by cache invalidation.
    pub fn invalidate_projection(&mut self) {
        self.publish_tail_projection = None;
        self.projection_loaded_ms = None;
    }

    /// The generation of the head this engine last published against, without
    /// touching the store.
    pub fn cached_generation(&self) -> Option<loonfs_api::NamespaceGeneration> {
        self.publish_tail_projection
            .as_ref()
            .map(|projection| projection.head.generation)
    }

    /// Returns the retained tail projection's memory weight, or `None` when no
    /// projection is cached. Runtimes can sum this value across namespace engines
    /// to enforce a global cache limit.
    pub fn retained_tail_weight(&self) -> Option<PublishTailWeight> {
        self.publish_tail_projection
            .as_ref()
            .map(PublishTailProjection::weight)
    }

    /// Whether the retained tail contains a receipt, without loading a projection.
    pub fn has_retained_commit_receipt(&self, commit_id: &CommitId) -> bool {
        self.publish_tail_projection
            .as_ref()
            .is_some_and(|projection| {
                projection
                    .tail_state
                    .rows
                    .find_commit_receipt(commit_id)
                    .is_some()
            })
    }

    /// The retained projection as a fold input, or `None` when the engine
    /// holds no projection.
    ///
    pub fn wal_fold_input(&self) -> Option<WalFoldInput> {
        self.publish_tail_projection
            .as_ref()
            .map(|projection| WalFoldInput {
                head: projection.head.clone(),
                basis: projection.basis().clone(),
                retention_floor_seq: projection.retention_floor_seq,
                tail_state: Arc::clone(&projection.tail_state),
                wal_tail_segments: projection.wal_tail_segments,
                wal_tail_inline_bytes: projection.tail_state.inline_bytes(),
            })
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
        let acquired_writer = acquire_writer_epoch(store, &self.namespace_id, context).await?;
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
    /// "Deleting a namespace").
    ///
    /// Deletion publishes a new manifest with the same writer-session checks as
    /// [`Self::publish_batch`]. Fenced sessions fail before accessing the store,
    /// and a takeover detected during manifest publication permanently fences the
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
            context,
        )
        .await;
        self.invalidate_projection();
        if let Err(CoreError::WriterFenced(fence)) = &deleted {
            *self.lock_session() = WriterSessionState::Fenced(fence.clone());
        }
        deleted
    }

    pub async fn publish_batch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: impl AsRef<[CommitCandidate]>,
        context: &MutationContext,
        tail_options: &PublishTailOptions,
    ) -> NamespaceCommitEnginePublishResult {
        let candidates = candidates.as_ref();
        let mut result =
            Box::pin(self.publish_batch_inner(store, candidates, context, tail_options)).await;
        for _ in 1..crate::limits::CONTENTION_RETRY_LIMIT {
            if !result.results.iter().any(|result| {
                matches!(
                    result,
                    Err(CoreError::WalPublish(
                        crate::commit::WalPublishError::StaleHead
                    ))
                )
            }) {
                break;
            }
            result =
                Box::pin(self.publish_batch_inner(store, candidates, context, tail_options)).await;
        }
        result
    }

    async fn publish_batch_inner<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: &[CommitCandidate],
        context: &MutationContext,
        tail_options: &PublishTailOptions,
    ) -> NamespaceCommitEnginePublishResult {
        let attempt_started_ms = self.timer.monotonic_now_ms();
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishResult {
                results: Vec::new(),
                wal_tail_segments: 0,
                wal_tail_inline_bytes: 0,
                wal_tail_observed: false,
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
                    wal_tail_inline_bytes: 0,
                    wal_tail_observed: false,
                    resulting_read_state: None,
                };
            }
        };

        if self.projection_loaded_ms.is_some_and(|loaded_ms| {
            attempt_started_ms.saturating_sub(loaded_ms) >= crate::limits::WAL_PUBLISH_BUDGET_MS
        }) || self
            .publish_tail_projection
            .as_ref()
            .is_some_and(|projection| {
                projection.wal_tail_segments >= crate::limits::FOLD_AT_WAL_SEGMENTS
            })
        {
            self.invalidate_projection();
        }
        let projection_loaded_ms = self.projection_loaded_ms.unwrap_or(attempt_started_ms);
        let (publish_view, projection) = match load_publish_metadata_view(
            store,
            self.segment_cache.as_deref(),
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
                    wal_tail_inline_bytes: 0,
                    wal_tail_observed: false,
                    resulting_read_state: None,
                };
            }
        };

        let published = crate::protocol::publish_namespace_commits_batch_against_publish_view(
            store,
            &self.namespace_id,
            candidates,
            context,
            &publish_view,
            crate::protocol::PublicationClock {
                timer: self.timer.as_ref(),
                attempt_started_ms,
                tip_observed_ms: projection_loaded_ms,
            },
        )
        .await;
        self.projection_loaded_ms = Some(projection_loaded_ms);
        let (wal_tail_segments, wal_tail_inline_bytes, resulting_read_state) =
            self.update_publish_tail_projection(projection, published.effect, tail_options);
        NamespaceCommitEnginePublishResult {
            results: published.results,
            wal_tail_segments,
            wal_tail_inline_bytes,
            wal_tail_observed: true,
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
    ) -> (u64, usize, Option<ResultingReadState>) {
        let state = match effect {
            PublishViewEffect::Unchanged => None,
            PublishViewEffect::Invalidated => {
                self.invalidate_projection();
                return (
                    projection.wal_tail_segments,
                    projection.tail_state.inline_bytes(),
                    None,
                );
            }
            PublishViewEffect::Advanced {
                records,
                inline_content,
                head,
            } => {
                projection.wal_tail_segments += 1;
                let tail_state = Arc::make_mut(&mut projection.tail_state);
                for value in inline_content {
                    tail_state
                        .insert_inline_content(value.content_ref().clone(), value.bytes().clone());
                }
                for record in &records {
                    if let Err(error) = tail_state.apply_commit(record) {
                        // The WAL is already durable. Discard this cache; replay
                        // and folding will report the accounting error.
                        tracing::error!(%error, "could not update the committed WAL projection");
                        self.invalidate_projection();
                        return (
                            projection.wal_tail_segments,
                            tail_state.inline_bytes(),
                            None,
                        );
                    }
                }
                projection.reanchor(head.clone());
                Some(ResultingReadState {
                    head,
                    basis: projection.basis().clone(),
                    manifest_head_seq: projection.manifest_head_seq(),
                    tail: Arc::clone(&projection.tail_state),
                })
            }
        };
        let count = projection.wal_tail_segments;
        let inline_bytes = projection.tail_state.inline_bytes();
        if projection.within_limits(tail_options) {
            self.publish_tail_projection = Some(projection);
        } else {
            self.invalidate_projection();
        }
        (count, inline_bytes, state)
    }
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<Commit>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

/// Publishes one batch through a fresh, uncached commit engine.
pub(crate) async fn publish_namespace_commits_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<CommitCandidate>,
    context: &MutationContext,
) -> Vec<Result<Commit>> {
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
#[path = "commit_engine_content_tests.rs"]
mod content_tests;

#[cfg(test)]
#[path = "commit_engine_inline_tests.rs"]
mod inline_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::limits::WAL_PUBLISH_BUDGET_MS;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::load_namespace_read_state;
    use futures::StreamExt;
    use loonfs_api::{
        ChangeSeq, ContentRef, ContentStoreId, PrincipalId, PrincipalScope, PrincipalSet, Subject,
        SubjectId, WriterEpoch,
    };
    use loonfs_objectstore::keys::wal_segment_prefix;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::stores::{OperationClass, RecordingStore};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    fn context(writer_id: &str) -> MutationContext {
        MutationContext {
            writer_id: loonfs_api::WriterId::parse(writer_id).expect("writer id"),
            now_ms: 1_000,
        }
    }

    fn create_dir_request(commit_id: &str, name: &str) -> CommitRequest {
        CommitRequest::single(
            CommitId::parse(commit_id).expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectory {
                path: loonfs_api::AbsolutePath::parse(format!("/{name}")).expect("valid path"),
                parents: false,
            },
        )
    }

    #[test]
    fn admission_weight_includes_annotation_proofs_and_preparation_errors() {
        let request = create_dir_request("weight", "docs");
        let baseline = CommitCandidate::new(request.clone())
            .estimated_retained_bytes()
            .expect("weight");
        let mut annotated = request.clone();
        annotated.message = Some(String::new());
        let empty_annotation = CommitCandidate::new(annotated.clone())
            .estimated_retained_bytes()
            .expect("weight");
        annotated.message = Some("x".repeat(4096));
        assert!(
            CommitCandidate::new(annotated)
                .estimated_retained_bytes()
                .expect("weight")
                >= empty_annotation + 4096
        );
        let proof = PreparedContent::for_durable_content_write(
            NamespaceId::parse("demo").expect("namespace"),
            ContentStoreId::parse("cs_00000000000000000000000000000001").expect("store"),
            ContentRef::blob_v1(
                loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
                loonfs_api::NamespaceGeneration(1),
                ContentId::generate(),
                b"proof",
            ),
        );
        let prepared = CommitCandidate::prepared(request.clone(), vec![proof; 100]);
        assert!(
            prepared.estimated_retained_bytes().expect("weight")
                > baseline + 100 * std::mem::size_of::<PreparedContent>()
        );
        let rejected = CommitCandidate::rejected(
            request,
            ContentPreparationError::ContentToken(vec![
                (
                    ContentId::generate(),
                    ContentTokenError::Codec("x".repeat(4096))
                );
                10
            ]),
        );
        assert!(rejected.estimated_retained_bytes().expect("weight") > baseline + 40960);
    }

    #[test]
    fn principals_add_to_candidate_admission_weight() {
        let mut candidate = CommitCandidate::new(create_dir_request("subject-weight", "docs"));
        let before = candidate.estimated_retained_bytes().expect("weight");
        let principals = (0..64)
            .map(|index| {
                PrincipalId::parse(format!("{index:03}{}", "x".repeat(253))).expect("principal")
            })
            .collect::<BTreeSet<_>>();
        candidate.request.subject = Some(Subject {
            principal_scope: PrincipalScope::parse("scope").expect("scope"),
            subject_id: SubjectId::parse("subject").expect("subject"),
            principals: PrincipalSet::new(principals).expect("principals"),
        });
        let after = candidate.estimated_retained_bytes().expect("weight");
        assert!(after >= before + 64 * 256);
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
            preconditions: Vec::new(),
            commit_id: CommitId::parse("too-many-ops").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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

        let content_ref = ContentRef::blob_v1(
            loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
            loonfs_api::NamespaceGeneration(1),
            ContentId::generate(),
            b"proof",
        );
        let prepared = PreparedContent::for_durable_content_write(
            namespace_id.clone(),
            ContentStoreId::parse("cs_00000000000000000000000000000001").expect("content store id"),
            content_ref,
        );
        let oversized_proofs = CommitCandidate::prepared(
            create_dir_request("too-many-proofs", "docs"),
            vec![prepared; crate::limits::MAX_COMMIT_CONTENT_TOKENS + 1],
        );
        oversized_proofs
            .semantic_identity(&namespace_id)
            .expect("prepared proof limits must not affect identity");

        let oversized_message = CommitCandidate::new(CommitRequest {
            preconditions: Vec::new(),
            commit_id: CommitId::parse("too-long-message").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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

    #[test]
    fn a_batch_past_the_operation_ceiling_is_rejected() {
        let oversized = CommitCandidate::new(CommitRequest {
            preconditions: Vec::new(),
            commit_id: CommitId::parse("oversized-batch").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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
            preconditions: Vec::new(),
            commit_id: CommitId::parse("largest-batch").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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

    #[test]
    fn a_message_past_the_byte_ceiling_is_rejected() {
        let operations = vec![FilesystemOperation::CreateDirectory {
            path: loonfs_api::AbsolutePath::parse("/docs").expect("valid path"),
            parents: false,
        }];

        let oversized = CommitCandidate::new(CommitRequest {
            preconditions: Vec::new(),
            commit_id: CommitId::parse("oversized-message").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: Some("m".repeat(crate::limits::MAX_COMMIT_MESSAGE_BYTES + 1)),
            operations: operations.clone(),
        });
        let error = oversized
            .validate_request_limits()
            .expect_err("the message is over the byte ceiling");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);

        let at_ceiling = CommitCandidate::new(CommitRequest {
            preconditions: Vec::new(),
            commit_id: CommitId::parse("largest-message").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
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
            .list_prefix_stream(&wal_segment_prefix(namespace_id))
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
        bootstrap_namespace(
            &store,
            &namespace_id,
            &writer_a,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
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
        let epoch_after_takeover = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("read head")
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
        let head = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("read head");
        assert_eq!(head.writer_epoch, epoch_after_takeover);
        assert_eq!(
            head.writer.expect("writer block").writer_id.as_str(),
            "writer-b"
        );
    }

    #[tokio::test]
    async fn fencing_during_publish_view_load_still_reports_writer_fenced() {
        use loonfs_test_support::stores::{BlockingStore, KeyPredicate};
        use std::sync::Arc as StdArc;

        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a");

        // Block the WAL tail read: it sits between the read state that the
        // fence check uses and the closing etag recheck.
        let store = StdArc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
            OperationClass::Read,
        ));

        bootstrap_namespace(
            store.inner(),
            &namespace_id,
            &writer_a,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
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

        // A has read a head that still names it. Writer B takes the
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
        bootstrap_namespace(
            &store,
            &namespace_id,
            &writer_a,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
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
        let epoch_after_fencing = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("read head")
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
        let head = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("read head");
        assert_eq!(head.writer_epoch, epoch_after_fencing);
        assert_eq!(
            head.writer.expect("writer block").writer_id.as_str(),
            "writer-b"
        );
    }

    /// Advances an entire publish budget per reading, so every publish
    /// observes an expired budget between segment PUT and WAL put.
    #[derive(Debug)]
    struct ExpiredBudgetTimer(AtomicU64);

    impl MonotonicTimer for ExpiredBudgetTimer {
        fn monotonic_now_ms(&self) -> u64 {
            self.0
                .fetch_add(WAL_PUBLISH_BUDGET_MS + 1_000, Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn publish_over_budget_writes_no_segment_and_a_retry_rebuilds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a");
        bootstrap_namespace(
            &store,
            &namespace_id,
            &writer,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
        .await
        .expect("bootstrap");
        let mut over_budget = NamespaceCommitEngine::new(namespace_id.clone())
            .monotonic_timer(Arc::new(ExpiredBudgetTimer(AtomicU64::new(0))));
        over_budget
            .session_writer_epoch(&store, &writer)
            .await
            .expect("acquire");
        let head_before = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("head");
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
                CoreError::WalPublish(crate::commit::WalPublishError::PublishBudgetExceeded { .. })
            ),
            "unexpected error: {error:?}"
        );
        // Retryable exactly like a stale head, so existing retry loops
        // rebuild the commit.
        assert_eq!(error.code(), ErrorCode::StaleHead);

        let head_after = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("read head");
        assert_eq!(head_after.seq, head_before.seq);
        assert_eq!(head_after.wal_no, head_before.wal_no);
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 1);

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
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 3);
        let head_final = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("read head");
        assert_eq!(head_final.seq, ChangeSeq(1));
        // Two engines are two sessions, and each acquires its own epoch: the
        // abandoned attempt took 1, the retry took 2.
        assert_eq!(head_final.writer_epoch, WriterEpoch(2));
    }

    #[tokio::test]
    async fn publish_views_reuse_cached_segment_blocks_across_publishes() {
        use crate::cache::MetadataSegmentCacheConfig;
        let temp_dir = tempdir().expect("tempdir");
        let store =
            RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a");
        bootstrap_namespace(
            &store,
            &namespace_id,
            &writer,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
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
                expires_at_ms: None,
            },
            &writer,
        )
        .await
        .expect("checkpoint");

        // Without a cache, every publish view re-fetches the segment blocks
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
            "publish validation should read segment blocks"
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

        let cache = Arc::new(MetadataSegmentCache::new(
            MetadataSegmentCacheConfig::default(),
        ));
        let mut cached = NamespaceCommitEngine::new(namespace_id.clone()).segment_cache(cache);
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
            "a warm cache serves every publish-view segment read"
        );
    }
}
