//! [`NamespaceCommitEngine`] publishes a batch of validated mutation
//! candidates as one numbered WAL put, then returns
//! one result per candidate.

use crate::authorize::CommitAuthority;
use crate::commit::CommitFingerprint;
use crate::context::MutationContext;
use crate::error::{CoreError, Result, WriterFence};
use crate::manifest::{MetadataSegmentCache, PublishedManifest};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::writer_epoch::acquire_writer;
use crate::namespace::writer_epoch::AcquiredWriter;
use crate::options::DeleteNamespaceOptions;
use crate::path::write::{commit_fingerprint, CommitIntent, FilesystemOperation};
use crate::protocol::{
    load_publish_metadata_view, PublishTailPosition, PublishViewEffect, ReanchorOutcome,
};
use crate::storage::content_admission::{ContentTokenError, PreparedContent};
use crate::storage::inline_content::InlineContent;
use crate::time::{Deadline, MonotonicTimer, Observation, StdMonotonicTimer};
use crate::wal::settle_publish_attempt;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::ObjectStore;
use loonfs_types::api::v0::Commit;
use loonfs_types::format::wal::{
    MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
};
use loonfs_types::{
    CommitId, ContentId, ContentRef, DeleteNamespaceResponse, InlineContentIdentity, ManifestNo,
    NamespaceId,
};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// One namespace mutation together with the result of preparing any content
/// it references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitCandidate {
    intent: CommitIntent,
    content: ContentPreparation,
    maintenance: bool,
    inline_content: Vec<InlineContent>,
    inline_content_identities: BTreeMap<ContentId, InlineContentIdentity>,
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
    /// Uses the handle subject to authorize this publication.
    pub fn with_subject(mut self, subject: loonfs_types::Subject) -> Self {
        self.intent.subject = Some(subject);
        self.maintenance = false;
        self
    }

    /// Supplies the subject used to own content staged for this candidate.
    pub fn subject(&self) -> Option<&loonfs_types::Subject> {
        self.intent.subject.as_ref()
    }

    /// A commit no subject check applies to. Only maintenance builds one.
    pub fn maintenance(intent: CommitIntent) -> Self {
        Self {
            intent,
            content: ContentPreparation::Ready(Vec::new()),
            maintenance: true,
            inline_content: Vec::new(),
            inline_content_identities: BTreeMap::new(),
        }
    }

    pub(crate) fn authority(&self) -> CommitAuthority<'_> {
        if self.maintenance {
            CommitAuthority::Maintenance
        } else {
            CommitAuthority::Subject(self.intent.subject.as_ref())
        }
    }

    /// Wraps a mutation request with no attached content proofs.
    pub fn new(intent: CommitIntent) -> Self {
        Self {
            intent,
            content: ContentPreparation::Ready(Vec::new()),
            maintenance: false,
            inline_content: Vec::new(),
            inline_content_identities: BTreeMap::new(),
        }
    }

    /// Wraps a mutation request with opaque proofs for its prepared content.
    pub fn prepared(intent: CommitIntent, content: Vec<PreparedContent>) -> Self {
        let mut candidate = Self::new(intent);
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
    pub fn rejected(intent: CommitIntent, error: ContentPreparationError) -> Self {
        Self::new(intent).reject_content_preparation(error)
    }

    /// Rejects new publication while preserving the identity needed for receipt replay.
    pub fn reject_content_preparation(mut self, error: ContentPreparationError) -> Self {
        self.inline_content_identities
            .extend(self.inline_content.drain(..).map(|value| {
                (
                    value.content_ref().content_id.clone(),
                    InlineContentIdentity::from_bytes(value.bytes()),
                )
            }));
        self.content = ContentPreparation::Rejected(error);
        self
    }

    /// Takes `inline_content` as the content bytes carried by the commit.
    pub fn with_inline_content(
        intent: CommitIntent,
        content: Vec<PreparedContent>,
        inline_content: Vec<InlineContent>,
    ) -> Self {
        let mut candidate = Self::prepared(intent, content);
        candidate.inline_content.extend(inline_content);
        candidate
    }

    /// Returns values that publication still needs to carry in the WAL.
    pub fn inline_content(&self) -> &[InlineContent] {
        &self.inline_content
    }

    /// Lists inline values in the order their operations first name them.
    pub fn ordered_inline_content(&self, namespace_id: &NamespaceId) -> Result<Vec<InlineContent>> {
        crate::protocol::validate_inline_content_references(
            &self.intent,
            &self.inline_content,
            namespace_id,
        )?;
        let mut values: std::collections::HashMap<_, _> = self
            .inline_content
            .iter()
            .map(|value| (&value.content_ref().content_id, value))
            .collect();
        Ok(self
            .intent
            .operations
            .iter()
            .filter_map(FilesystemOperation::content_ref)
            .filter_map(|reference| values.remove(&reference.content_id).cloned())
            .collect())
    }

    /// Replaces an inline value with a staged proof without changing its identity form.
    pub fn stage_inline_content(&mut self, content_id: &ContentId, proof: PreparedContent) {
        let reference = proof.content_ref();
        for operation in &mut self.intent.operations {
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
        if let Some(value) = self
            .inline_content
            .iter()
            .find(|value| &value.content_ref().content_id == content_id)
        {
            self.inline_content_identities.insert(
                reference.content_id.clone(),
                InlineContentIdentity::from_bytes(value.bytes()),
            );
        }
        self.inline_content
            .retain(|value| &value.content_ref().content_id != content_id);
        if let ContentPreparation::Ready(proofs) = &mut self.content {
            proofs.push(proof);
        }
    }

    pub(crate) fn revision_layout(
        &self,
        content_ref: &ContentRef,
    ) -> Option<loonfs_types::ContentLayout> {
        let ContentPreparation::Ready(proofs) = &self.content else {
            return None;
        };
        proofs
            .iter()
            .find(|proof| proof.content_ref() == content_ref)
            .map(|_| loonfs_types::ContentLayout {
                extents: vec![loonfs_types::ContentExtent {
                    owner_namespace_id: content_ref.owner_namespace_id.clone(),
                    content_id: content_ref.content_id.clone(),
                    object: loonfs_types::ExtentObject::Whole,
                    offset: 0,
                    length: content_ref.size_bytes,
                }],
            })
    }

    /// Request bytes before planning adds any resident bytes a fresh chain copies.
    pub fn inline_content_bytes(&self) -> usize {
        self.inline_content
            .iter()
            .fold(self.appended_bytes(), |total, value| {
                total.saturating_add(value.bytes().len())
            })
    }

    /// Bytes the request's appends add. They always travel in the WAL.
    pub fn appended_bytes(&self) -> usize {
        self.intent
            .operations
            .iter()
            .filter_map(FilesystemOperation::appended_content)
            .fold(0usize, |total, bytes| total.saturating_add(bytes.len()))
    }

    /// Rejects an append of no bytes or of more than one WAL piece holds,
    /// naming the operation.
    pub fn validate_appended_content(&self) -> Result<()> {
        for (index, operation) in self.intent.operations.iter().enumerate() {
            let Some(bytes) = operation.appended_content() else {
                continue;
            };
            let error = if bytes.is_empty() {
                CoreError::InvalidCommitField {
                    field: "inline_content",
                    message: "an append carries at least 1 byte".to_owned(),
                    precondition_index: None,
                }
            } else if bytes.len() > MAX_WAL_INLINE_CONTENT_BYTES {
                CoreError::AppendTooLarge {
                    size_bytes: bytes.len() as u64,
                    max_bytes: MAX_WAL_INLINE_CONTENT_BYTES as u64,
                }
            } else {
                continue;
            };
            return Err(error.at_operation(index));
        }
        Ok(())
    }

    pub(crate) fn intent(&self) -> &CommitIntent {
        &self.intent
    }

    pub(crate) fn content_preparation(&self) -> &ContentPreparation {
        &self.content
    }

    /// Returns the idempotency key carried by the mutation request.
    pub fn commit_id(&self) -> &CommitId {
        &self.intent.commit_id
    }

    /// Computes semantic identity without applying current operational request limits.
    pub fn semantic_identity(&self, namespace_id: &NamespaceId) -> Result<CommitFingerprint> {
        commit_fingerprint(
            namespace_id,
            &self.intent,
            &self
                .inline_content
                .iter()
                .map(|value| {
                    (
                        value.content_ref().content_id.clone(),
                        InlineContentIdentity::from_bytes(value.bytes()),
                    )
                })
                .chain(
                    self.inline_content_identities
                        .iter()
                        .map(|(id, identity)| (id.clone(), identity.clone())),
                )
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
                        value,
                    ))
                    .saturating_add(std::mem::size_of::<ContentId>())
                    .saturating_add(std::mem::size_of::<InlineContentIdentity>())
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
                .saturating_add(std::mem::size_of_val(self.intent.operations.as_slice()))
                .saturating_add(std::mem::size_of_val(self.intent.preconditions.as_slice())),
        );
        serde_json::to_writer(
            &mut bytes,
            &(
                &self.intent.commit_id,
                &self.intent.actor_id,
                &self.intent.message,
                &self.intent.operations,
                &self.intent.preconditions,
            ),
        )
        .map_err(|error| CoreError::InvalidCommitRequest(error.to_string()))?;
        if let Some(subject) = &self.intent.subject {
            serde_json::to_writer(&mut bytes, &subject.subject_id)
                .map_err(|error| CoreError::InvalidCommitRequest(error.to_string()))?;
            for principal_id in subject.principals.iter() {
                bytes.0 = bytes
                    .0
                    .saturating_add(std::mem::size_of::<loonfs_types::PrincipalId>());
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
        for content_id in self.inline_content_identities.keys() {
            bytes.0 = bytes
                .0
                .saturating_add(std::mem::size_of::<ContentId>())
                .saturating_add(std::mem::size_of::<InlineContentIdentity>())
                .saturating_add(content_id.as_str().len());
        }
        Ok(bytes.0)
    }

    /// Bounds the request's WAL bytes before planning adds carried layouts.
    /// Publication adds their extent bounds before accepting the plan.
    pub fn wal_record_bytes_upper_bound(&self) -> usize {
        crate::commit_wal_size::estimated_wal_record_bytes(&self.intent, &self.inline_content)
    }

    /// Bounds the request after inline selection, before carried layouts.
    pub fn wal_record_bytes_upper_bound_with_inline_content(
        &self,
        inline_content: &[InlineContent],
    ) -> usize {
        crate::commit_wal_size::estimated_wal_record_bytes(&self.intent, inline_content)
    }

    pub(crate) fn validate_request_limits(&self) -> Result<()> {
        // Apply limits to the complete request because the serialized publisher
        // processes every operation before releasing the write path.
        self.validate_request_has_operations()?;
        if self.intent.operations.len() > crate::limits::MAX_COMMIT_OPERATIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {} operations; maximum is {}",
                self.intent.operations.len(),
                crate::limits::MAX_COMMIT_OPERATIONS
            )));
        }
        if self.intent.preconditions.len() > crate::limits::MAX_COMMIT_PRECONDITIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {} preconditions; maximum is {}",
                self.intent.preconditions.len(),
                crate::limits::MAX_COMMIT_PRECONDITIONS
            )));
        }
        if let Some(message) = &self.intent.message {
            if message.len() > crate::limits::MAX_COMMIT_MESSAGE_BYTES {
                return Err(CoreError::InvalidCommitRequest(format!(
                    "mutation message is {} bytes; maximum is {}",
                    message.len(),
                    crate::limits::MAX_COMMIT_MESSAGE_BYTES
                )));
            }
        }
        self.validate_appended_content()?;
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
        if inline_bytes > MAX_WAL_OBJECT_INLINE_CONTENT_BYTES {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {inline_bytes} inline content bytes; maximum is {}",
                MAX_WAL_OBJECT_INLINE_CONTENT_BYTES
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
            .intent
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
        if self.intent.operations.is_empty() {
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
    /// The WAL tail this attempt loaded, for opportunistic maintenance
    /// scheduling, or `None` when it loaded no projection.
    pub wal_tail: Option<WalTailReport>,
}

/// The WAL tail one publish attempt loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalTailReport {
    /// Unfolded WAL objects.
    pub objects: u64,
    /// Inline content bytes in the tail, including the inline bytes of a put
    /// whose outcome is unknown.
    pub inline_bytes: usize,
    /// Where the attempt found the tail.
    pub source: TailSource,
}

/// Where a publish attempt found the WAL tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailSource {
    /// The engine's position, which kept the tail from an earlier
    /// publication unit.
    Cached,
    /// The store.
    Store,
}

/// The tail a fold publishes: the head it was replayed against, the basis
/// the manifest builds on, and the rows themselves. Cheap to take: an `Arc`
/// clone and two small clones.
#[derive(Debug, Clone)]
pub struct WalFoldInput {
    pub head: NamespaceReadState,
    pub basis: MetadataBasis,
    pub tail_state: Arc<ProjectedWalTail>,
    pub wal_tail_objects: u64,
    pub wal_tail_inline_bytes: usize,
}

/// The head that reads in a writer session's runtime start from, taken from
/// the session's commit engine.
#[derive(Debug, Clone)]
pub struct ReadPosition {
    pub head: NamespaceReadState,
    pub basis: MetadataBasis,
    /// The projected WAL tail above `basis` at `head`, which the engine
    /// keeps with its position. Reads from the position start from this
    /// tail without replaying the WAL.
    pub tail: Arc<ProjectedWalTail>,
    /// When a store observation last confirmed `basis` current: the
    /// discovery that loaded it, or a HEAD that found no successor.
    pub basis_checked: Observation,
}

/// Tracks writer state for one namespace session: unacquired, acquired, or
/// permanently fenced.
///
/// Object storage cannot reconstruct whether the current session was fenced.
/// Runtimes should therefore share one instance across engines for the same
/// namespace and keep it outside rebuildable caches. An engine without shared
/// state treats each one-shot commit as a separate session.
#[derive(Debug, Default)]
pub enum WriterSessionStatus {
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

/// Shared handle to one namespace's [`WriterSessionStatus`].
pub type SharedWriterSessionStatus = Arc<Mutex<WriterSessionStatus>>;

#[derive(Debug)]
pub struct NamespaceCommitEngine {
    content_writes: Arc<tokio::sync::Semaphore>,
    namespace_id: NamespaceId,
    publish_tail: Option<PublishTailPosition>,
    projection_observed: Option<Observation>,
    acquired_anchor: Option<NamespaceReadAnchor>,
    fold_observed: Option<Observation>,
    /// The newest manifest this runtime published while a fold was in
    /// progress. [`Self::record_wal_fold`] applies it once the fold finishes.
    published_during_fold: Option<PublishedManifest>,
    /// When a store observation last confirmed the projection's basis manifest
    /// current: the discovery that loaded it, or a HEAD that found no successor.
    basis_checked: Option<Observation>,
    /// This session's epoch and fencing for the namespace; see
    /// [`WriterSessionStatus`].
    session: SharedWriterSessionStatus,
    /// Local monotonic source for the self-enforced publish budget.
    timer: Arc<dyn MonotonicTimer>,
    /// Shared cache of decoded blocks used by publish-view reads. Blocks are
    /// keyed by segment digest. Successor probes verify that the view is current.
    segment_cache: Option<Arc<MetadataSegmentCache>>,
}

impl NamespaceCommitEngine {
    pub fn new(namespace_id: NamespaceId, content_writes: Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            namespace_id,
            content_writes,
            publish_tail: None,
            projection_observed: None,
            acquired_anchor: None,
            fold_observed: None,
            published_during_fold: None,
            basis_checked: None,
            session: SharedWriterSessionStatus::default(),
            timer: Arc::new(StdMonotonicTimer::default()),
            segment_cache: None,
        }
    }

    /// Uses runtime-managed session state so the writer epoch and fenced status
    /// persist across engine instances.
    pub fn writer_session(mut self, session: SharedWriterSessionStatus) -> Self {
        self.session = session;
        self
    }

    fn lock_session(&self) -> std::sync::MutexGuard<'_, WriterSessionStatus> {
        // Treat a poisoned lock as fatal because another thread panicked while
        // updating the session state.
        self.session
            .lock()
            .expect("writer session state lock should not be poisoned")
    }

    /// Uses the runtime clock for publication budgets.
    pub fn monotonic_timer(mut self, timer: Arc<dyn MonotonicTimer>) -> Self {
        self.timer = timer;
        self
    }

    pub fn segment_cache(mut self, segment_cache: Arc<MetadataSegmentCache>) -> Self {
        self.segment_cache = Some(segment_cache);
        self
    }

    /// Drops the position and its tail, so the next unit reads the tail from
    /// the store. Writer epoch and fencing remain in session state and are
    /// not reset.
    pub fn invalidate_projection(&mut self) {
        self.publish_tail = None;
        self.acquired_anchor = None;
        self.projection_observed = None;
        self.basis_checked = None;
        self.published_during_fold = None;
    }

    /// The basis manifest that needs a successor check, either because its
    /// last confirmation expired or because this attempt probed the tip.
    fn unconfirmed_basis(&self, attempt: &Observation, tip_probed: bool) -> Option<ManifestNo> {
        let projection = self.publish_tail.as_ref()?;
        let confirmed_recently = !tip_probed
            && self.basis_checked.as_ref().is_some_and(|checked| {
                checked.age_at(attempt) <= crate::limits::WAL_PUBLISH_BUDGET_MS
            });
        (!confirmed_recently).then(|| projection.basis().manifest_no())
    }

    /// Probes the next WAL number only while the position's last basis
    /// confirmation is within the revalidation bound. An error reloads.
    async fn next_wal_absent<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        attempt: &Observation,
    ) -> bool {
        let Some(position) = self.publish_tail.as_ref() else {
            return false;
        };
        if !self.basis_checked.as_ref().is_some_and(|checked| {
            checked.age_at(attempt) < crate::limits::READ_REVALIDATION_BOUND_MS
        }) {
            return false;
        }
        let Ok(next) = position.head.wal_no.successor() else {
            return false;
        };
        matches!(
            crate::wal::wal_object_exists(store, &self.namespace_id, next).await,
            Ok(false)
        )
    }

    /// Checks the position's WAL tail for the commit's receipt, without
    /// store reads or writer acquisition.
    pub fn retains_commit_receipt(&self, commit_id: &CommitId) -> bool {
        self.publish_tail
            .as_ref()
            .is_some_and(|position| position.tail.find_commit_receipt(commit_id).is_some())
    }

    /// Unfolded WAL objects as of the last publication unit, or `None` when
    /// the engine has no position.
    pub fn wal_tail_objects(&self) -> Option<u64> {
        self.publish_tail
            .as_ref()
            .map(|position| position.wal_tail_objects)
    }

    /// Inline content bytes in the unfolded WAL tail as of the last
    /// publication unit, or `None` when the engine has no position.
    pub fn wal_tail_inline_bytes(&self) -> Option<usize> {
        self.publish_tail
            .as_ref()
            .map(PublishTailPosition::wal_tail_inline_bytes)
    }

    /// The position's tail as a fold input, or `None` when the engine has no
    /// position.
    pub fn wal_fold_input(&self) -> Option<WalFoldInput> {
        let position = self.publish_tail.as_ref()?;
        Some(WalFoldInput {
            head: position.head.clone(),
            basis: position.basis().clone(),
            tail_state: Arc::clone(&position.tail),
            wal_tail_objects: position.wal_tail_objects,
            wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
        })
    }

    /// Starts the session's own fold, and returns the tail to fold, or
    /// `None` when the engine has no position, so the fold loads the tail
    /// from the store.
    pub fn begin_wal_fold(&mut self) -> Option<WalFoldInput> {
        self.fold_observed = Some(Observation::now(Arc::clone(&self.timer)));
        let input = self.wal_fold_input();
        if let Some(position) = self.publish_tail.as_mut() {
            position.begin_fold();
        }
        input
    }

    /// Records how the session's own fold ended. A fold moves the position
    /// to the manifest that covers the fold's tail, whether the fold
    /// published that manifest or found it current, or drops the position
    /// when it can't move there. A fold that failed leaves the position where
    /// it was.
    pub fn record_wal_fold(
        &mut self,
        folded: std::result::Result<&crate::manifest::FoldedWalTail, &CoreError>,
    ) {
        let fold_observed = self.fold_observed.take();
        let published_during_fold = self.published_during_fold.take();
        match folded {
            Ok(folded) => {
                let moved = self.publish_tail.as_mut().map(|position| {
                    let new_basis = position.basis() != &folded.basis;
                    (position.reanchor_after_fold(folded), new_basis)
                });
                match moved {
                    Some((true, new_basis)) => {
                        // The fold published a new basis, or found it current,
                        // after the fold began, so no manifest followed it
                        // then. The position's own basis has no such check.
                        if new_basis {
                            self.basis_checked = fold_observed;
                        }
                    }
                    _ => self.invalidate_projection(),
                }
            }
            Err(_) => {
                if let Some(position) = self.publish_tail.as_mut() {
                    position.end_fold();
                }
            }
        }
        if let Some(published) = published_during_fold {
            self.record_manifest_published(published);
        }
    }

    /// Moves the position to a manifest that this runtime published, as
    /// [`Self::record_wal_fold`] does after the session's own fold, or drops
    /// the position when it can't move there (see
    /// `PublishTailPosition::reanchor_after_publish` for the rule).
    ///
    /// Later commits then plan against the new manifest, and reads in this
    /// runtime start from it, so a read's manifest check doesn't find the
    /// new manifest as a successor and drop the position. The position keeps
    /// the WAL tail above the new manifest. The time of the last manifest
    /// check stays the same. That check found no successor of the old
    /// manifest, so it happened before this runtime published the new
    /// manifest, and no manifest after the new one existed then either.
    ///
    /// A manifest that arrives during a fold waits for the fold to finish,
    /// because the fold's outcome decides which manifest the position uses
    /// first. Only the newest such manifest is kept, because applying it
    /// alone either moves the position to it or drops the position.
    pub fn record_manifest_published(&mut self, published: PublishedManifest) {
        if self
            .publish_tail
            .as_ref()
            .is_some_and(PublishTailPosition::is_folding)
        {
            let newest = match self.published_during_fold.take() {
                Some(earlier)
                    if earlier.manifest().manifest_no > published.manifest().manifest_no =>
                {
                    earlier
                }
                _ => published,
            };
            self.published_during_fold = Some(newest);
            return;
        }
        let Some(position) = self.publish_tail.as_mut() else {
            return;
        };
        match position.reanchor_after_publish(&published) {
            ReanchorOutcome::Kept | ReanchorOutcome::Moved => {}
            ReanchorOutcome::Superseded => self.invalidate_projection(),
        }
    }

    /// Returns the position that reads in this runtime start from, or `None`
    /// when the engine has no position or its session is fenced.
    ///
    /// A fenced session publishes nothing more, so its position would stay
    /// on a head that the new writer has moved past.
    pub fn read_position(&self) -> Option<ReadPosition> {
        if matches!(*self.lock_session(), WriterSessionStatus::Fenced(_)) {
            return None;
        }
        let position = self.publish_tail.as_ref()?;
        Some(ReadPosition {
            head: position.head.clone(),
            basis: position.basis().clone(),
            tail: Arc::clone(&position.tail),
            basis_checked: self.basis_checked.clone()?,
        })
    }

    /// Returns the session's writer epoch, acquiring it on first use. A fenced
    /// session fails immediately without accessing the store or acquiring a new
    /// epoch.
    async fn session_writer_epoch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        context: &MutationContext,
    ) -> Result<AcquiredWriter> {
        let already_acquired = match &*self.lock_session() {
            WriterSessionStatus::Fenced(fence) => {
                return Err(CoreError::WriterFenced(fence.clone()))
            }
            WriterSessionStatus::Acquired(acquired_writer) => Some(acquired_writer.clone()),
            WriterSessionStatus::Unacquired => None,
        };
        if let Some(acquired_writer) = already_acquired {
            return Ok(acquired_writer);
        }
        let (acquired_writer, anchor) =
            acquire_writer(store, &self.namespace_id, context, Arc::clone(&self.timer)).await?;
        self.acquired_anchor = Some(anchor);
        let mut session = self.lock_session();
        if let WriterSessionStatus::Fenced(fence) = &*session {
            // Another engine fenced the shared session while this engine was acquiring
            // the epoch. Preserve the fenced state.
            return Err(CoreError::WriterFenced(fence.clone()));
        }
        *session = WriterSessionStatus::Acquired(acquired_writer.clone());
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
        let deadline = Deadline::start(Arc::clone(&self.timer));
        let acquired_writer = self.session_writer_epoch(store, context).await?;
        let deleted = crate::namespace::delete::delete_namespace(
            store,
            &self.namespace_id,
            options,
            acquired_writer,
            context,
            &deadline,
            self.segment_cache.as_deref(),
            &self.content_writes,
        )
        .await;
        self.invalidate_projection();
        if let Err(CoreError::WriterFenced(fence)) = &deleted {
            *self.lock_session() = WriterSessionStatus::Fenced(fence.clone());
        }
        deleted
    }

    /// Retries share `batch` so content evidence continues aging.
    pub async fn publish_batch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: impl AsRef<[CommitCandidate]>,
        context: &MutationContext,
        batch: &Deadline,
    ) -> NamespaceCommitEnginePublishResult {
        let candidates = candidates.as_ref();
        let attempt = batch.observe();
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishResult {
                results: Vec::new(),
                wal_tail: None,
            };
        }

        let candidate_count = candidates.len();
        let acquired_writer = match self.session_writer_epoch(store, context).await {
            Ok(value) => value,
            Err(error) => {
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, error),
                    wal_tail: None,
                };
            }
        };

        let tip_expired = self.projection_observed.as_ref().is_some_and(|observed| {
            observed.age_at(&attempt) > crate::limits::WAL_PUBLISH_BUDGET_MS
        });
        if tip_expired && !self.next_wal_absent(store, &attempt).await {
            self.invalidate_projection();
        }
        // A landed put confirms the tip, not the manifest the batch was planned
        // against: a fold or compaction (including our own) may have succeeded.
        // Another writer must claim a successor manifest before writing WAL.
        // The WAL probe also checks for an unobserved put by this session.
        // Check the successor after that probe, and bound the answer by the
        // previous basis confirmation so a collected successor cannot look
        // absent. Either probe finding an object or failing reloads the view.
        if let Some(manifest_no) = self.unconfirmed_basis(&attempt, tip_expired) {
            let successor = crate::namespace::read_anchor::manifest_has_successor(
                store,
                &self.namespace_id,
                manifest_no,
            )
            .await;
            if matches!(successor, Ok(false))
                && self
                    .basis_checked
                    .as_ref()
                    .is_some_and(Observation::is_within_revalidation_bound)
            {
                self.basis_checked = Some(attempt.clone());
                if tip_expired {
                    self.projection_observed = Some(attempt.clone());
                }
            } else {
                self.invalidate_projection();
            }
        }
        let acquired_anchor = self.acquired_anchor.take();
        let cached_position = match acquired_anchor {
            Some(_) => {
                self.publish_tail = None;
                None
            }
            None => self.publish_tail.take(),
        };
        let loaded = load_publish_metadata_view(
            store,
            self.segment_cache.as_deref(),
            &self.namespace_id,
            acquired_writer,
            cached_position,
            acquired_anchor,
            Arc::clone(&self.timer),
        )
        .await;
        let publish_view = match loaded {
            Ok(value) => value,
            Err(error) => {
                self.invalidate_projection();
                if let CoreError::WriterFenced(fence) = &error {
                    *self.lock_session() = WriterSessionStatus::Fenced(fence.clone());
                }
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, error),
                    wal_tail: None,
                };
            }
        };
        let wal_tail_discovered = publish_view.tail_discovered;
        let projection_observed = if wal_tail_discovered {
            // Discovery confirmed the basis and observed the tip no earlier
            // than this attempt began.
            self.basis_checked = Some(attempt.clone());
            attempt.clone()
        } else {
            self.projection_observed
                .clone()
                .unwrap_or_else(|| attempt.clone())
        };

        let published = crate::protocol::publish_namespace_commits_batch_against_publish_view(
            store,
            &self.namespace_id,
            candidates,
            context,
            &publish_view,
            crate::protocol::PublicationClock {
                batch,
                attempt: attempt.clone(),
                tip: projection_observed.clone(),
            },
            &self.content_writes,
        )
        .await;
        let position = publish_view.into_position();
        self.projection_observed = Some(projection_observed);
        let unknown_inline_bytes = published.unconfirmed_inline_bytes;
        let update = self.update_publish_tail_projection(position, published.effect);
        // A put that landed is itself an observation of the tip it created, made no
        // earlier than this attempt began; the next put's budget runs from it.
        if update.landed && self.publish_tail.is_some() {
            self.projection_observed = Some(attempt);
        }
        NamespaceCommitEnginePublishResult {
            results: published.results,
            wal_tail: Some(WalTailReport {
                objects: update.wal_tail_objects,
                inline_bytes: update.wal_tail_inline_bytes + unknown_inline_bytes,
                source: if wal_tail_discovered {
                    TailSource::Store
                } else {
                    TailSource::Cached
                },
            }),
        }
    }

    /// Folds one batch's effect into the unit's position and its tail, keeps
    /// the position, and reports the WAL-tail length the caller schedules
    /// maintenance on.
    fn update_publish_tail_projection(
        &mut self,
        mut position: PublishTailPosition,
        effect: PublishViewEffect,
    ) -> TailProjectionUpdate {
        let advanced = matches!(effect, PublishViewEffect::Advanced { .. });
        match effect {
            PublishViewEffect::Unchanged => {}
            PublishViewEffect::Invalidated => {
                self.invalidate_projection();
                return TailProjectionUpdate {
                    wal_tail_objects: position.wal_tail_objects,
                    wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
                    landed: false,
                };
            }
            PublishViewEffect::Advanced { records, head } => {
                position.wal_tail_objects += 1;
                let applied = Arc::make_mut(&mut position.tail)
                    .push_object(&self.namespace_id, &records)
                    .and_then(|chunk| position.apply_fold_object(chunk));
                if let Err(error) = applied {
                    // The WAL is already durable. Discard the position;
                    // replay and folding will report the accounting error.
                    tracing::warn!(
                        namespace_id = %self.namespace_id,
                        error = %error,
                        "could not update the committed WAL projection"
                    );
                    self.invalidate_projection();
                    return TailProjectionUpdate {
                        wal_tail_objects: position.wal_tail_objects,
                        wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
                        landed: false,
                    };
                }
                position.reanchor(head);
            }
        }
        let update = TailProjectionUpdate {
            wal_tail_objects: position.wal_tail_objects,
            wal_tail_inline_bytes: position.wal_tail_inline_bytes(),
            landed: advanced,
        };
        self.publish_tail = Some(position);
        update
    }
}

/// What folding one batch into the tail projection left: the WAL-tail length
/// the caller schedules maintenance on, and whether the batch's put landed
/// and moved the position.
struct TailProjectionUpdate {
    wal_tail_objects: u64,
    wal_tail_inline_bytes: usize,
    landed: bool,
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<Commit>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

/// Publishes one batch through a fresh commit engine, which keeps its
/// position across the batch's retries.
pub(crate) async fn publish_namespace_commits_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<CommitCandidate>,
    context: &MutationContext,
    content_writes: Arc<tokio::sync::Semaphore>,
    timer: Arc<dyn MonotonicTimer>,
) -> Vec<Result<Commit>> {
    let mut engine =
        NamespaceCommitEngine::new(namespace_id.clone(), content_writes).monotonic_timer(timer);
    let batch = Deadline::start(Arc::clone(&engine.timer));
    let mut results = vec![None; candidates.len()];
    let mut pending: Vec<_> = candidates.into_iter().enumerate().collect();
    for _ in 0..crate::limits::MAX_CONTENTION_ATTEMPTS {
        let (indices, attempted): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
        let observed = engine
            .publish_batch(store, &attempted, context, &batch)
            .await
            .results;
        pending =
            settle_publish_attempt(&mut results, indices.into_iter().zip(attempted), observed);
        if pending.is_empty() {
            break;
        }
    }
    results
        .into_iter()
        .map(|result| result.expect("each candidate should receive a publication result"))
        .collect()
}

/// Deletes a namespace through a fresh commit engine: a one-shot session
/// that acquires its own epoch, exactly like a one-shot publish.
pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    context: &MutationContext,
    content_writes: Arc<tokio::sync::Semaphore>,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<DeleteNamespaceResponse> {
    NamespaceCommitEngine::new(namespace_id.clone(), content_writes)
        .monotonic_timer(timer)
        .delete_namespace(store, options, context)
        .await
}

#[cfg(test)]
mod tests;
