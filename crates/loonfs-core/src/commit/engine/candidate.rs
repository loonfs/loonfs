//! A commit candidate: one namespace mutation with the result of preparing the
//! content it references, and the request limits a candidate is checked
//! against before planning.

use super::super::CommitFingerprint;
use crate::authorize::CommitAuthority;
use crate::error::{CoreError, Result};
use crate::path::write::{commit_fingerprint, CommitIntent, FilesystemOperation};
use crate::storage::content_admission::{ContentTokenError, PreparedContent};
use crate::storage::inline_content::InlineContent;
use loonfs_types::format::wal::{
    MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
};
use loonfs_types::{
    CommitId, ContentId, ContentRef, FileContent, InlineContentIdentity, NamespaceId,
};
use std::collections::{BTreeMap, HashSet};
use thiserror::Error;

/// One namespace mutation together with the result of preparing any content
/// it references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitCandidate {
    request: CandidateRequest,
    pub(crate) content: ContentPreparation,
    pub(crate) inline_content: Vec<InlineContent>,
    inline_content_identities: BTreeMap<ContentId, InlineContentIdentity>,
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
    pub fn ordered_inline_content(&self, namespace_id: &NamespaceId) -> Result<Vec<InlineContent>> {
        let intent = self.intent();
        super::super::candidates::validate_inline_content_references(
            intent,
            &self.inline_content,
            namespace_id,
        )?;
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
        let intent = self.intent();
        let mut bytes = RequestByteCounter(
            std::mem::size_of_val(self)
                .saturating_add(std::mem::size_of_val(intent.operations.as_slice()))
                .saturating_add(std::mem::size_of_val(intent.preconditions.as_slice())),
        );
        serde_json::to_writer(
            &mut bytes,
            &(
                &intent.commit_id,
                &intent.actor_id,
                &intent.message,
                &intent.operations,
                &intent.preconditions,
            ),
        )
        .map_err(|error| CoreError::InvalidCommitRequest(error.to_string()))?;
        if let Some(subject) = &intent.subject {
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

    /// Bounds the request's WAL bytes before planning adds carried layouts
    /// and `advance_subtree` deltas. Publication adds their bounds before
    /// accepting the plan.
    pub fn wal_record_bytes_upper_bound(&self) -> usize {
        self.wal_record_bytes_upper_bound_with_inline_content(&self.inline_content)
    }

    /// Bounds the request after inline selection, before carried layouts and
    /// `advance_subtree` deltas.
    pub fn wal_record_bytes_upper_bound_with_inline_content(
        &self,
        inline_content: &[InlineContent],
    ) -> usize {
        super::super::wal_size::estimated_wal_record_bytes(self.intent(), inline_content)
    }

    /// Bounds the planned WAL record, which can include layouts and
    /// `advance_subtree` deltas that the request bound leaves out.
    pub(crate) fn planned_wal_record_bytes(
        &self,
        deltas: &[loonfs_types::format::wal::WalCommitDelta],
        appended: &[loonfs_types::format::wal::WalInlineContent],
    ) -> usize {
        super::super::wal_size::planned_wal_record_bytes(
            self.intent(),
            &self.inline_content,
            deltas,
            appended,
        )
    }

    pub(crate) fn validate_request_limits(&self) -> Result<()> {
        // Apply limits to the complete request because the serialized publisher
        // processes every operation before releasing the write path.
        self.validate_request_has_operations()?;
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
        if let Some(message) = &intent.message {
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

    pub(crate) fn validate_request_has_operations(&self) -> Result<()> {
        if self.intent().operations.is_empty() {
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
