//! A commit candidate: one namespace mutation with the result of preparing the
//! content it references, and the request limits a candidate is checked
//! against before planning.

use super::super::CommitFingerprint;
use crate::authorize::CommitAuthority;
use crate::error::{CoreError, Result};
use crate::heap_bytes::{btree_map_node_bytes, HeapBytes};
use crate::path::write::{
    commit_fingerprint, merge_fingerprint, CommitIntent, FilesystemOperation,
};
use crate::storage::content_admission::{ContentTokenError, PreparedContent};
use crate::storage::inline_content::InlineContent;
use loonfs_types::format::wal::{
    MergeRecord, MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
};
use loonfs_types::{
    ChangeSeq, CommitId, ContentId, ContentRef, FileContent, InlineContentIdentity, InodeId,
    NamespaceId,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use thiserror::Error;

/// One namespace mutation together with the result of preparing any content
/// it references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitCandidate {
    request: CandidateRequest,
    /// The merge that the commit is, if it's one. The commit's identity is
    /// then its merge record in place of its operations and preconditions.
    merge: Option<MergeIntent>,
    pub(crate) content: ContentPreparation,
    pub(crate) inline_content: Vec<InlineContent>,
    inline_content_identities: BTreeMap<ContentId, InlineContentIdentity>,
}

/// A merge's record, the fork point that the merge's range is checked
/// against, the IDs that the inodes it creates have in the fork, and the
/// moves that a temporary name splits in two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeIntent {
    /// The fork and the range of its commits that the merge applies.
    pub(crate) record: MergeRecord,
    /// The sequence of this namespace that the fork was made at, from the
    /// fork's manifest (`fork_basis.source_head_seq`). The fork's first merge
    /// starts after it.
    pub(crate) fork_point_seq: ChangeSeq,
    /// The fork's ID of the inode that each create operation makes, by the
    /// operation's index. A merge creates each inode under its ID in the
    /// fork, so an inode has one ID in both namespaces.
    pub(crate) created_inode_ids: BTreeMap<usize, InodeId>,
    /// The legs of each move that the merge makes through a temporary name,
    /// by operation index.
    pub(crate) temporary_moves: BTreeMap<usize, TemporaryMove>,
}

/// One leg of a merge's move that a temporary name splits in two. The
/// temporary binding doesn't survive the commit, so the writer checks the
/// subject's rights against the inode's move from its directory before the
/// first leg to its directory after the second, and not against the
/// temporary directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TemporaryMove {
    /// The move to the temporary name. The writer checks the remove right on
    /// the inode's directory, and not the temporary directory's create right
    /// or the change of the inode's rights.
    Away,
    /// The move from the temporary name to the inode's place. The writer
    /// checks the create right on the inode's new directory, and the change
    /// of the inode's rights from `from_parent`, the directory that the
    /// inode left in the first leg, and not the temporary directory's remove
    /// right.
    Back { from_parent: InodeId },
}

/// What a candidate commits, and whose authority it commits under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CandidateRequest {
    /// Filesystem operations, authorized against the intent's subject.
    Intent(CommitIntent),
    /// Filesystem operations that maintenance commits with no subject check.
    Maintenance(CommitIntent),
}

impl CandidateRequest {
    fn intent(&self) -> &CommitIntent {
        match self {
            Self::Intent(intent) | Self::Maintenance(intent) => intent,
        }
    }

    fn intent_mut(&mut self) -> &mut CommitIntent {
        match self {
            Self::Intent(intent) | Self::Maintenance(intent) => intent,
        }
    }
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
        let mut intent = match self.request {
            CandidateRequest::Intent(intent) | CandidateRequest::Maintenance(intent) => intent,
        };
        intent.subject = Some(subject);
        self.request = CandidateRequest::Intent(intent);
        self
    }

    /// Supplies the subject used to own content staged for this candidate.
    pub fn subject(&self) -> Option<&loonfs_types::Subject> {
        self.intent().subject.as_ref()
    }

    /// A commit no subject check applies to. Only maintenance builds one.
    pub fn maintenance(intent: CommitIntent) -> Self {
        Self::with_request(CandidateRequest::Maintenance(intent))
    }

    pub(crate) fn authority(&self) -> CommitAuthority<'_> {
        match &self.request {
            CandidateRequest::Intent(intent) => CommitAuthority::Subject(intent.subject.as_ref()),
            CandidateRequest::Maintenance(_) => CommitAuthority::Maintenance,
        }
    }

    /// Wraps a mutation request with no attached content proofs.
    pub fn new(intent: CommitIntent) -> Self {
        Self::with_request(CandidateRequest::Intent(intent))
    }

    fn with_request(request: CandidateRequest) -> Self {
        Self {
            request,
            merge: None,
            content: ContentPreparation::Ready(Vec::new()),
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

    /// Makes the candidate a merge of `record`, from a fork made at
    /// `fork_point_seq`, whose create operations make the inodes that
    /// `created_inode_ids` lists by operation index. A commit request has no
    /// merge record, so a merge can't arrive from a request body, and only
    /// the merge planner builds one.
    pub(crate) fn with_merge(
        mut self,
        record: MergeRecord,
        fork_point_seq: ChangeSeq,
        created_inode_ids: BTreeMap<usize, InodeId>,
    ) -> Self {
        self.merge = Some(MergeIntent {
            record,
            fork_point_seq,
            created_inode_ids,
            temporary_moves: BTreeMap::new(),
        });
        self
    }

    /// Marks the legs of the merge's moves that go through a temporary name,
    /// by operation index. A candidate that isn't a merge doesn't change.
    pub(crate) fn with_temporary_moves(
        mut self,
        temporary_moves: BTreeMap<usize, TemporaryMove>,
    ) -> Self {
        if let Some(merge) = &mut self.merge {
            merge.temporary_moves = temporary_moves;
        }
        self
    }

    /// The merge that the commit is, if it's one.
    pub(crate) fn merge(&self) -> Option<&MergeIntent> {
        self.merge.as_ref()
    }

    /// Returns whether the commit is a merge, which LoonFS publishes in a
    /// WAL object of its own.
    pub fn is_merge(&self) -> bool {
        self.merge.is_some()
    }

    /// Returns the error for a commit whose estimated WAL bytes exceed the
    /// WAL object's limit: `MergeTooLarge` for a merge, which its caller splits
    /// by the fork's commit range, and `CommitTooLarge` for any other commit.
    pub fn too_large_error(&self, estimated_bytes: usize, max_bytes: usize) -> CoreError {
        if self.is_merge() {
            CoreError::MergeTooLarge {
                bound: crate::error::MergeBound::WalObject,
                estimated_bytes,
                max_bytes,
                split_at_seq: None,
            }
        } else {
            CoreError::CommitTooLarge {
                estimated_bytes,
                max_bytes,
            }
        }
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

    /// Takes `inline_content` as the content bytes that the commit includes.
    pub fn with_inline_content(
        intent: CommitIntent,
        content: Vec<PreparedContent>,
        inline_content: Vec<InlineContent>,
    ) -> Self {
        let mut candidate = Self::prepared(intent, content);
        candidate.inline_content.extend(inline_content);
        candidate
    }

    /// Returns values that publication still needs to write in the WAL.
    pub fn inline_content(&self) -> &[InlineContent] {
        &self.inline_content
    }

    /// Lists inline values in the order their operations first name them.
    pub fn ordered_inline_content(&self) -> Result<Vec<InlineContent>> {
        let intent = self.intent();
        super::super::candidates::validate_inline_content_references(intent, &self.inline_content)?;
        let mut values: std::collections::HashMap<_, _> = self
            .inline_content
            .iter()
            .map(|value| (&value.content_ref().content_id, value))
            .collect();
        Ok(intent
            .operations
            .iter()
            .filter_map(FilesystemOperation::content_ref)
            .filter_map(|reference| values.remove(&reference.content_id).cloned())
            .collect())
    }

    /// Replaces an inline value with a staged proof without changing its identity form.
    pub fn stage_inline_content(&mut self, content_id: &ContentId, proof: PreparedContent) {
        let reference = proof.content_ref();
        for operation in &mut self.request.intent_mut().operations {
            match operation {
                FilesystemOperation::PutFile {
                    content: FileContent::Ref(content_ref),
                    ..
                }
                | FilesystemOperation::CreateFileByInode {
                    content: FileContent::Ref(content_ref),
                    ..
                }
                | FilesystemOperation::PutFileRevisionByInode {
                    content: FileContent::Ref(content_ref),
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

    /// Indexes the proofs that prepared the candidate's references by
    /// reference, the first proof of each, so that planning finds the layout
    /// that a revision records without a scan of every proof. A merge has a
    /// proof for each file it creates, and a scan for each file would make
    /// its planning quadratic.
    pub(crate) fn proofs_by_reference(&self) -> HashMap<&ContentRef, &PreparedContent> {
        let ContentPreparation::Ready(proofs) = &self.content else {
            return HashMap::new();
        };
        let mut by_reference = HashMap::with_capacity(proofs.len());
        for proof in proofs {
            by_reference.entry(proof.content_ref()).or_insert(proof);
        }
        by_reference
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
        self.intent()
            .operations
            .iter()
            .filter_map(FilesystemOperation::appended_content)
            .fold(0usize, |total, bytes| total.saturating_add(bytes.len()))
    }

    /// Rejects an append of no bytes or of more than one WAL piece can contain,
    /// naming the operation.
    pub fn validate_appended_content(&self) -> Result<()> {
        for (index, operation) in self.intent().operations.iter().enumerate() {
            let Some(bytes) = operation.appended_content() else {
                continue;
            };
            let error = if bytes.is_empty() {
                CoreError::InvalidCommitField {
                    field: "inline_content",
                    message: "an append carries at least 1 byte".to_owned(),
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

    /// The filesystem request the candidate commits.
    pub(crate) fn intent(&self) -> &CommitIntent {
        self.request.intent()
    }

    #[cfg(test)]
    pub(crate) fn intent_mut(&mut self) -> &mut CommitIntent {
        self.request.intent_mut()
    }

    pub(crate) fn content_preparation(&self) -> &ContentPreparation {
        &self.content
    }

    /// Returns the idempotency key in the mutation request.
    pub fn commit_id(&self) -> &CommitId {
        &self.intent().commit_id
    }

    /// Computes semantic identity without applying current operational request limits.
    pub fn semantic_identity(&self, namespace_id: &NamespaceId) -> CommitFingerprint {
        if let Some(merge) = &self.merge {
            return merge_fingerprint(namespace_id, self.intent(), &merge.record);
        }
        commit_fingerprint(
            namespace_id,
            self.intent(),
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

    /// Weighs the candidate for publication admission: its own size and the
    /// heap that it owns, which includes the request, the prepared proofs or
    /// rejections, and the inline content bytes.
    pub fn estimated_retained_bytes(&self) -> usize {
        std::mem::size_of_val(self) + self.heap_bytes()
    }

    /// Weighs the candidate as [`Self::estimated_retained_bytes`] does once
    /// the publisher stages `staged_inline_content`. Staging a value replaces
    /// the value's heap with a prepared proof and records the value's
    /// identity, and the inline vector keeps its capacity. This is the weight
    /// that admission guesses before staging; once the values are staged, the
    /// charge moves to the staged candidate's
    /// [`Self::estimated_retained_bytes`].
    pub fn estimated_retained_bytes_after_inline_staging(
        &self,
        staged_inline_content: &[InlineContent],
    ) -> usize {
        let proof_bytes = match &self.content {
            ContentPreparation::Ready(_) => std::mem::size_of::<PreparedContent>(),
            ContentPreparation::Rejected(_) => 0,
        };
        let staged_bytes = staged_inline_content
            .iter()
            .map(|value| {
                let content_id = &value.content_ref().content_id;
                proof_bytes
                    + value.content_ref().heap_bytes()
                    + std::mem::size_of::<(ContentId, InlineContentIdentity)>()
                    + content_id.heap_bytes()
            })
            .sum::<usize>();
        let left_bytes = staged_inline_content
            .iter()
            .map(HeapBytes::heap_bytes)
            .sum::<usize>();
        self.estimated_retained_bytes() - left_bytes + staged_bytes
    }

    /// Bounds the request's WAL bytes before planning adds carried layouts.
    /// Publication adds their extent bounds before accepting the plan.
    pub fn wal_record_bytes_upper_bound(&self) -> usize {
        self.wal_record_bytes_upper_bound_with_inline_content(&self.inline_content)
    }

    /// Bounds the request after inline selection, before carried layouts.
    /// A merge's bound includes the layout that each of its revision deltas
    /// records, from the merge's proofs, so it's the merge's planned WAL
    /// bytes. One WAL object stores the merge alone, and the merge allowance
    /// is charged a multiple of these bytes.
    pub fn wal_record_bytes_upper_bound_with_inline_content(
        &self,
        inline_content: &[InlineContent],
    ) -> usize {
        let intent = self.intent();
        match &self.merge {
            Some(merge) => {
                let proofs = self.proofs_by_reference();
                super::super::wal_size::merge_wal_record_bytes(
                    intent,
                    &merge.record,
                    |content_ref| {
                        proofs
                            .get(content_ref)
                            .and_then(|proof| proof.revision_layout_extents())
                    },
                )
            }
            None => {
                super::super::wal_size::estimated_wal_record_bytes(intent, None, inline_content)
            }
        }
    }

    /// Bounds the planned WAL record, which can include layouts that the
    /// request bound leaves out.
    pub(crate) fn planned_wal_record_bytes(
        &self,
        deltas: &[loonfs_types::format::wal::WalCommitDelta],
        appended: &[loonfs_types::format::wal::WalInlineContent],
    ) -> usize {
        super::super::wal_size::planned_wal_record_bytes(
            self.intent(),
            self.merge.as_ref().map(|merge| &merge.record),
            &self.inline_content,
            deltas,
            appended,
        )
    }

    pub(crate) fn validate_request_limits(&self) -> Result<()> {
        self.validate_request_has_operations()?;
        if self.merge.is_some() {
            self.validate_message_limit()?;
            return self.validate_merge_content();
        }
        // Apply limits to the complete request because the serialized publisher
        // processes every operation before releasing the write path.
        let intent = self.intent();
        if intent.operations.len() > crate::limits::MAX_COMMIT_OPERATIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {} operations; maximum is {}",
                intent.operations.len(),
                crate::limits::MAX_COMMIT_OPERATIONS
            )));
        }
        if intent.preconditions.len() > crate::limits::MAX_COMMIT_PRECONDITIONS {
            return Err(CoreError::InvalidCommitRequest(format!(
                "mutation has {} preconditions; maximum is {}",
                intent.preconditions.len(),
                crate::limits::MAX_COMMIT_PRECONDITIONS
            )));
        }
        self.validate_message_limit()?;
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
        let distinct_content_refs = intent
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

    fn validate_message_limit(&self) -> Result<()> {
        if let Some(message) = &self.intent().message {
            if message.len() > crate::limits::MAX_COMMIT_MESSAGE_BYTES {
                return Err(CoreError::InvalidCommitRequest(format!(
                    "mutation message is {} bytes; maximum is {}",
                    message.len(),
                    crate::limits::MAX_COMMIT_MESSAGE_BYTES
                )));
            }
        }
        Ok(())
    }

    /// Refuses a merge that includes inline content or appended bytes. A merge
    /// refers to content that the fork already wrote, and LoonFS planned it
    /// itself, so the counts that limit a request body don't apply to it,
    /// and the WAL object's size bounds it instead.
    fn validate_merge_content(&self) -> Result<()> {
        if !self.inline_content.is_empty()
            || self
                .intent()
                .operations
                .iter()
                .any(|operation| operation.appended_content().is_some())
        {
            return Err(CoreError::InvalidCommitRequest(
                "a merge includes no inline content or appended bytes".to_owned(),
            ));
        }
        Ok(())
    }

    /// Refuses a commit with no operations, except a merge, whose record
    /// advances the merge index when the fork's net change over its range is
    /// empty.
    pub(crate) fn validate_request_has_operations(&self) -> Result<()> {
        if self.intent().operations.is_empty() && self.merge.is_none() {
            return Err(CoreError::InvalidCommitRequest(
                "mutation request carries no operations".to_owned(),
            ));
        }
        Ok(())
    }
}

impl HeapBytes for CommitCandidate {
    fn heap_bytes(&self) -> usize {
        self.intent().heap_bytes()
            + self.merge.as_ref().map_or(0, MergeIntent::heap_bytes)
            + self.content.heap_bytes()
            + self.inline_content.heap_bytes()
            + self
                .inline_content_identities
                .keys()
                .map(HeapBytes::heap_bytes)
                .fold(
                    btree_map_node_bytes(&self.inline_content_identities),
                    usize::saturating_add,
                )
    }
}

impl HeapBytes for MergeIntent {
    fn heap_bytes(&self) -> usize {
        self.record.heap_bytes()
            + btree_map_node_bytes(&self.created_inode_ids)
            + btree_map_node_bytes(&self.temporary_moves)
    }
}

impl HeapBytes for ContentPreparation {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Ready(proofs) => proofs.heap_bytes(),
            Self::Rejected(ContentPreparationError::ContentToken(rejections)) => {
                rejections.heap_bytes()
            }
            Self::Rejected(ContentPreparationError::ContentNotPrepared { content_id }) => {
                content_id.heap_bytes()
            }
        }
    }
}
