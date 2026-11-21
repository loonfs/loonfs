//! Generates stable fingerprints for filesystem mutations
//! ([Appendix B](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#appendix-b-semantic-commit-fingerprints)).
//! A fingerprint lets LoonFS determine whether two requests that use the same
//! commit ID describe the same mutation.
//!
//! The publisher stores this fingerprint in the commit row and compares
//! it when the same commit ID is submitted again.
//!
//! The commit ID is not part of the fingerprint input. The commit ID selects
//! a receipt, which identifies the commit row containing the fingerprint.

use crate::digest::is_sha256_hex;
use crate::id_macros::{string_id, validation_error};
use crate::options::RestoreDeletedDestination;
use crate::wal::MergeRecord;
use crate::{
    AccessGrants, AccessRevisionNo, AccessRight, ActorId, AttributeKey, AttributeValue,
    AttributesRevisionNo, CommitPrecondition, CommitSeq, ContentId, ContentRef, ContentRefKind,
    DeleteDirectoryBehavior, DestinationBehavior, FileContent, FilesystemOperation, InodeId,
    NamespaceId, RevisionNo,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use thiserror::Error;

/// Domain separator included in every mutation fingerprint input.
const COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.commit.semantic.v1";

/// Domain separator included in every merge fingerprint input.
const MERGE_FINGERPRINT_DOMAIN: &str = "loonfs.commit.merge.v1";

/// Format version and hash algorithm stored with each fingerprint.
///
/// Storing both values lets a later format use different encoding rules or a
/// different hash without changing existing fingerprints.
const FINGERPRINT_SCHEME: &str = "v1:sha256";

validation_error!(
    CommitFingerprintValidationError,
    "invalid commit fingerprint {value:?}: {reason}"
);

string_id! {
    /// The semantic identity of one mutation request, spelled `v1:sha256:` and
    /// 64 lowercase hex characters.
    CommitFingerprint,
    error = CommitFingerprintValidationError,
    validate = validate_commit_fingerprint,
}

fn validate_commit_fingerprint(value: &str) -> Result<(), CommitFingerprintValidationError> {
    if value
        .strip_prefix(FINGERPRINT_SCHEME)
        .and_then(|digest| digest.strip_prefix(':'))
        .is_some_and(is_sha256_hex)
    {
        return Ok(());
    }
    // A longer value can't be a fingerprint, and echoing it would make the
    // error as large as the bad input.
    let echoed = if value.len() <= FINGERPRINT_SCHEME.len() + 1 + 64 {
        value
    } else {
        ""
    };
    Err(CommitFingerprintValidationError::new(
        echoed,
        format!("must be `{FINGERPRINT_SCHEME}:` followed by 64 lowercase hex characters"),
    ))
}

fn fingerprint_bytes(bytes: &[u8]) -> CommitFingerprint {
    let digest = Sha256::digest(bytes);
    CommitFingerprint(format!(
        "{FINGERPRINT_SCHEME}:{}",
        crate::hex::hex_encode_bytes(&digest)
    ))
}

/// Canonical preimage for one operation inside a mutation fingerprint.
///
/// The serde representation is durable contract
/// ([Appendix B](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#appendix-b-semantic-commit-fingerprints)):
/// the same normalized request must fingerprint identically across releases.
/// A pinned-value test below fails if the encoding drifts.
///
/// The serialized variant names, the field names, and the field order below
/// are all part of that preimage under the [`COMMIT_FINGERPRINT_DOMAIN`] tag.
/// Operation and field names follow [`FilesystemOperation`]. Optional fields
/// are always present, using `null` when unset. This explicit representation
/// keeps request serialization defaults and transport details out of identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OperationFingerprintInput<'a> {
    CreateDirectory {
        path: &'a str,
        parents: bool,
    },
    PutFile {
        path: &'a str,
        behavior: DestinationBehavior,
        content_ref: ContentRefFingerprintInput<'a>,
        expected_inode_id: Option<InodeId>,
        expected_revision_no: Option<RevisionNo>,
    },
    CreateDirectoryByInode {
        parent_inode_id: InodeId,
        display_name: &'a str,
    },
    CreateFileByInode {
        parent_inode_id: InodeId,
        display_name: &'a str,
        content_ref: ContentRefFingerprintInput<'a>,
    },
    PutFileRevisionByInode {
        inode_id: InodeId,
        content_ref: ContentRefFingerprintInput<'a>,
        expected_revision_no: RevisionNo,
    },
    AppendFile {
        path: &'a str,
        inline_content: AppendedContentFingerprintInput,
        expected_inode_id: Option<InodeId>,
        expected_revision_no: Option<RevisionNo>,
    },
    AppendFileByInode {
        inode_id: InodeId,
        inline_content: AppendedContentFingerprintInput,
        expected_revision_no: Option<RevisionNo>,
    },
    MoveByInode {
        inode_id: InodeId,
        expected_binding_version: &'a str,
        destination_parent_inode_id: InodeId,
        destination_display_name: &'a str,
        behavior: DestinationBehavior,
        expected_destination_inode_id: Option<InodeId>,
        expected_destination_revision_no: Option<RevisionNo>,
    },
    DeleteByInode {
        inode_id: InodeId,
        expected_binding_version: &'a str,
        behavior: DeleteDirectoryBehavior,
    },
    // Identity covers the complete caller-visible logical request. A changed
    // delete precondition must conflict instead of replaying the old receipt
    // without checking the new precondition.
    DeletePath {
        path: &'a str,
        behavior: DeleteDirectoryBehavior,
        expected_inode_id: Option<InodeId>,
    },
    MovePath {
        source_path: &'a str,
        destination_path: &'a str,
        behavior: DestinationBehavior,
        expected_destination_inode_id: Option<InodeId>,
        expected_destination_revision_no: Option<RevisionNo>,
    },
    CopyPath {
        source_path: &'a str,
        destination_path: &'a str,
        behavior: DestinationBehavior,
        expected_destination_inode_id: Option<InodeId>,
        expected_destination_revision_no: Option<RevisionNo>,
    },
    CopyByInode {
        inode_id: InodeId,
        destination_parent_inode_id: InodeId,
        destination_display_name: &'a str,
        behavior: DestinationBehavior,
        expected_destination_inode_id: Option<InodeId>,
        expected_destination_revision_no: Option<RevisionNo>,
    },
    RestoreRevision {
        path: &'a str,
        source_revision_no: RevisionNo,
    },
    RestoreRevisionByInode {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
    },
    RestoreDeleted {
        inode_id: InodeId,
        deletion_seq: CommitSeq,
        destination_path: Option<&'a str>,
        destination_parent_inode_id: Option<InodeId>,
        destination_display_name: Option<&'a str>,
    },
    // Both preconditions join the preimage for the same reason the delete precondition
    // does: a changed expectation is a different logical request. `set` is a
    // map, so it serializes key-ordered whatever order the caller sent; the
    // translation below sorts and deduplicates `remove` so two spellings of
    // one removal set reach the same preimage.
    UpdateAttributes {
        path: &'a str,
        set: BTreeMap<&'a str, &'a str>,
        remove: Vec<&'a str>,
        expected_inode_id: Option<InodeId>,
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    },
    UpdateAttributesByInode {
        inode_id: InodeId,
        set: BTreeMap<&'a str, &'a str>,
        remove: Vec<&'a str>,
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    },
    UpdateAccess {
        path: &'a str,
        boundary: bool,
        grants: BTreeMap<&'a str, Vec<&'static str>>,
        expected_inode_id: Option<InodeId>,
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
    UpdateAccessByInode {
        inode_id: InodeId,
        boundary: bool,
        grants: BTreeMap<&'a str, Vec<&'static str>>,
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PreconditionFingerprintInput<'a> {
    NamespaceHead {
        expected_head_seq: CommitSeq,
    },
    FileRevision {
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
    },
    PathBinding {
        path: &'a str,
        expected_inode_id: InodeId,
        expected_binding_version: Option<&'a str>,
    },
    InodeBinding {
        inode_id: InodeId,
        expected_binding_version: &'a str,
    },
    PathAbsence {
        path: &'a str,
    },
    NameAbsence {
        parent_inode_id: InodeId,
        display_name: &'a str,
    },
    AttributesRevision {
        inode_id: InodeId,
        expected_attributes_revision_no: AttributesRevisionNo,
    },
    AccessRevision {
        inode_id: InodeId,
        expected_access_revision_no: AccessRevisionNo,
    },
}

fn precondition_fingerprint_input(
    precondition: &CommitPrecondition,
) -> PreconditionFingerprintInput<'_> {
    match precondition {
        CommitPrecondition::NamespaceHead { expected_head_seq } => {
            PreconditionFingerprintInput::NamespaceHead {
                expected_head_seq: *expected_head_seq,
            }
        }
        CommitPrecondition::FileRevision {
            inode_id,
            expected_revision_no,
        } => PreconditionFingerprintInput::FileRevision {
            inode_id: *inode_id,
            expected_revision_no: *expected_revision_no,
        },
        CommitPrecondition::PathBinding {
            path,
            expected_inode_id,
            expected_binding_version,
        } => PreconditionFingerprintInput::PathBinding {
            path: path.as_str(),
            expected_inode_id: *expected_inode_id,
            expected_binding_version: expected_binding_version
                .as_ref()
                .map(|value| value.as_str()),
        },
        CommitPrecondition::InodeBinding {
            inode_id,
            expected_binding_version,
        } => PreconditionFingerprintInput::InodeBinding {
            inode_id: *inode_id,
            expected_binding_version: expected_binding_version.as_str(),
        },
        CommitPrecondition::PathAbsence { path } => PreconditionFingerprintInput::PathAbsence {
            path: path.as_str(),
        },
        CommitPrecondition::NameAbsence {
            parent_inode_id,
            display_name,
        } => PreconditionFingerprintInput::NameAbsence {
            parent_inode_id: *parent_inode_id,
            display_name: display_name.as_str(),
        },
        CommitPrecondition::AttributesRevision {
            inode_id,
            expected_attributes_revision_no,
        } => PreconditionFingerprintInput::AttributesRevision {
            inode_id: *inode_id,
            expected_attributes_revision_no: *expected_attributes_revision_no,
        },
        CommitPrecondition::AccessRevision {
            inode_id,
            expected_access_revision_no,
        } => PreconditionFingerprintInput::AccessRevision {
            inode_id: *inode_id,
            expected_access_revision_no: *expected_access_revision_no,
        },
    }
}

/// Canonical preimage for the content a put attaches: the reference form for
/// an uploaded object, or the inline form for bytes the request includes
/// ([Content in a fingerprint](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#b2-content-in-a-fingerprint)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ContentRefFingerprintInput<'a> {
    BlobV1 {
        content_id: &'a str,
        size_bytes: u64,
    },
    InlineV1 {
        sha256: String,
        size_bytes: u64,
    },
}

/// Canonical preimage for the bytes an append adds. Planning determines which
/// content object receives them, so the bytes are their only identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AppendedContentFingerprintInput {
    AppendV1 { sha256: String, size_bytes: u64 },
}

impl AppendedContentFingerprintInput {
    fn of(bytes: &[u8]) -> Self {
        Self::AppendV1 {
            sha256: crate::hex::hex_encode_bytes(&Sha256::digest(bytes)),
            size_bytes: bytes.len() as u64,
        }
    }
}

/// Request-byte identity retained after inline content is staged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineContentIdentity {
    sha256: [u8; 32],
    size_bytes: u64,
}

impl InlineContentIdentity {
    /// Hashes request bytes independently of the store's checksum algorithm.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            sha256: Sha256::digest(bytes).into(),
            size_bytes: bytes.len() as u64,
        }
    }

    fn preimage(&self) -> ContentRefFingerprintInput<'static> {
        ContentRefFingerprintInput::InlineV1 {
            sha256: crate::hex::hex_encode_bytes(&self.sha256),
            size_bytes: self.size_bytes,
        }
    }
}

fn content_ref_fingerprint_input<'a>(
    content_ref: &'a ContentRef,
    inline_content: &BTreeMap<ContentId, InlineContentIdentity>,
) -> ContentRefFingerprintInput<'a> {
    if let Some(identity) = inline_content.get(&content_ref.content_id) {
        identity.preimage()
    } else {
        match content_ref.kind {
            ContentRefKind::BlobV1 => ContentRefFingerprintInput::BlobV1 {
                content_id: content_ref.content_id.as_str(),
                size_bytes: content_ref.size_bytes,
            },
        }
    }
}

fn content_fingerprint_input<'a>(
    content: &'a FileContent,
    inline_content_identities: &BTreeMap<ContentId, InlineContentIdentity>,
) -> ContentRefFingerprintInput<'a> {
    match content {
        FileContent::Ref(reference) => {
            content_ref_fingerprint_input(reference, inline_content_identities)
        }
        FileContent::Inline(bytes) => InlineContentIdentity::from_bytes(bytes).preimage(),
    }
}

/// Normalizes one operation into its durable semantic representation.
/// Attribute removals are a sorted set; content checksums are verification
/// evidence and excluded from identity in the reference form.
fn operation_fingerprint_input<'a>(
    operation: &'a FilesystemOperation,
    inline_content_identities: &BTreeMap<ContentId, InlineContentIdentity>,
) -> OperationFingerprintInput<'a> {
    match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            OperationFingerprintInput::CreateDirectory {
                path: path.as_str(),
                parents: *parents,
            }
        }
        FilesystemOperation::PutFile {
            path,
            content,
            behavior,
            expected_inode_id,
            expected_revision_no,
        } => OperationFingerprintInput::PutFile {
            path: path.as_str(),
            behavior: *behavior,
            content_ref: content_fingerprint_input(content, inline_content_identities),
            expected_inode_id: *expected_inode_id,
            expected_revision_no: *expected_revision_no,
        },
        FilesystemOperation::CreateDirectoryByInode {
            parent_inode_id,
            display_name,
        } => OperationFingerprintInput::CreateDirectoryByInode {
            parent_inode_id: *parent_inode_id,
            display_name: display_name.as_str(),
        },
        FilesystemOperation::CreateFileByInode {
            parent_inode_id,
            display_name,
            content,
        } => OperationFingerprintInput::CreateFileByInode {
            parent_inode_id: *parent_inode_id,
            display_name: display_name.as_str(),
            content_ref: content_fingerprint_input(content, inline_content_identities),
        },
        FilesystemOperation::PutFileRevisionByInode {
            inode_id,
            content,
            expected_revision_no,
        } => OperationFingerprintInput::PutFileRevisionByInode {
            inode_id: *inode_id,
            content_ref: content_fingerprint_input(content, inline_content_identities),
            expected_revision_no: *expected_revision_no,
        },
        FilesystemOperation::AppendFile {
            path,
            inline_content,
            expected_inode_id,
            expected_revision_no,
        } => OperationFingerprintInput::AppendFile {
            path: path.as_str(),
            inline_content: AppendedContentFingerprintInput::of(inline_content),
            expected_inode_id: *expected_inode_id,
            expected_revision_no: *expected_revision_no,
        },
        FilesystemOperation::AppendFileByInode {
            inode_id,
            inline_content,
            expected_revision_no,
        } => OperationFingerprintInput::AppendFileByInode {
            inode_id: *inode_id,
            inline_content: AppendedContentFingerprintInput::of(inline_content),
            expected_revision_no: *expected_revision_no,
        },
        FilesystemOperation::MoveByInode {
            inode_id,
            expected_binding_version,
            destination_parent_inode_id,
            destination_display_name,
            precondition,
        } => OperationFingerprintInput::MoveByInode {
            inode_id: *inode_id,
            expected_binding_version: expected_binding_version.as_str(),
            destination_parent_inode_id: *destination_parent_inode_id,
            destination_display_name: destination_display_name.as_str(),
            behavior: precondition.behavior,
            expected_destination_inode_id: precondition.expected_inode_id,
            expected_destination_revision_no: precondition.expected_revision_no,
        },
        FilesystemOperation::DeleteByInode {
            inode_id,
            expected_binding_version,
            behavior,
        } => OperationFingerprintInput::DeleteByInode {
            inode_id: *inode_id,
            expected_binding_version: expected_binding_version.as_str(),
            behavior: *behavior,
        },
        FilesystemOperation::DeletePath {
            path,
            behavior,
            expected_inode_id,
        } => OperationFingerprintInput::DeletePath {
            path: path.as_str(),
            behavior: *behavior,
            expected_inode_id: *expected_inode_id,
        },
        FilesystemOperation::MovePath {
            source_path,
            destination_path,
            precondition,
        } => OperationFingerprintInput::MovePath {
            source_path: source_path.as_str(),
            destination_path: destination_path.as_str(),
            behavior: precondition.behavior,
            expected_destination_inode_id: precondition.expected_inode_id,
            expected_destination_revision_no: precondition.expected_revision_no,
        },
        FilesystemOperation::CopyPath {
            source_path,
            destination_path,
            precondition,
        } => OperationFingerprintInput::CopyPath {
            source_path: source_path.as_str(),
            destination_path: destination_path.as_str(),
            behavior: precondition.behavior,
            expected_destination_inode_id: precondition.expected_inode_id,
            expected_destination_revision_no: precondition.expected_revision_no,
        },
        FilesystemOperation::CopyByInode {
            inode_id,
            destination_parent_inode_id,
            destination_display_name,
            precondition,
        } => OperationFingerprintInput::CopyByInode {
            inode_id: *inode_id,
            destination_parent_inode_id: *destination_parent_inode_id,
            destination_display_name: destination_display_name.as_str(),
            behavior: precondition.behavior,
            expected_destination_inode_id: precondition.expected_inode_id,
            expected_destination_revision_no: precondition.expected_revision_no,
        },
        FilesystemOperation::RestoreRevision {
            path,
            source_revision_no,
        } => OperationFingerprintInput::RestoreRevision {
            path: path.as_str(),
            source_revision_no: *source_revision_no,
        },
        FilesystemOperation::RestoreRevisionByInode {
            inode_id,
            source_revision_no,
        } => OperationFingerprintInput::RestoreRevisionByInode {
            inode_id: *inode_id,
            source_revision_no: *source_revision_no,
        },
        FilesystemOperation::RestoreDeleted {
            inode_id,
            deletion_seq,
            destination,
        } => {
            let (destination_path, destination_parent_inode_id, destination_display_name) =
                match destination {
                    RestoreDeletedDestination::Recorded => (None, None, None),
                    RestoreDeletedDestination::Path(path) => (Some(path.as_str()), None, None),
                    RestoreDeletedDestination::Name {
                        parent_inode_id,
                        display_name,
                    } => (None, Some(*parent_inode_id), Some(display_name.as_str())),
                };
            OperationFingerprintInput::RestoreDeleted {
                inode_id: *inode_id,
                deletion_seq: *deletion_seq,
                destination_path,
                destination_parent_inode_id,
                destination_display_name,
            }
        }
        FilesystemOperation::UpdateAttributes {
            path,
            set,
            remove,
            expected_inode_id,
            expected_attributes_revision_no,
        } => OperationFingerprintInput::UpdateAttributes {
            path: path.as_str(),
            set: attribute_set_fingerprint_input(set),
            remove: attribute_removal_fingerprint_input(remove),
            expected_inode_id: *expected_inode_id,
            expected_attributes_revision_no: *expected_attributes_revision_no,
        },
        FilesystemOperation::UpdateAttributesByInode {
            inode_id,
            set,
            remove,
            expected_attributes_revision_no,
        } => OperationFingerprintInput::UpdateAttributesByInode {
            inode_id: *inode_id,
            set: attribute_set_fingerprint_input(set),
            remove: attribute_removal_fingerprint_input(remove),
            expected_attributes_revision_no: *expected_attributes_revision_no,
        },
        FilesystemOperation::UpdateAccess {
            path,
            boundary,
            grants,
            expected_inode_id,
            expected_access_revision_no,
        } => OperationFingerprintInput::UpdateAccess {
            path: path.as_str(),
            boundary: *boundary,
            grants: grants_fingerprint_input(grants),
            expected_inode_id: *expected_inode_id,
            expected_access_revision_no: *expected_access_revision_no,
        },
        FilesystemOperation::UpdateAccessByInode {
            inode_id,
            boundary,
            grants,
            expected_access_revision_no,
        } => OperationFingerprintInput::UpdateAccessByInode {
            inode_id: *inode_id,
            boundary: *boundary,
            grants: grants_fingerprint_input(grants),
            expected_access_revision_no: *expected_access_revision_no,
        },
    }
}

fn attribute_set_fingerprint_input(
    set: &BTreeMap<AttributeKey, AttributeValue>,
) -> BTreeMap<&str, &str> {
    set.iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

/// The wire type preserves the caller's list so validation can report
/// duplicate keys. The fingerprint uses the sorted, unique set because order
/// and duplicate entries do not change the requested mutation.
fn attribute_removal_fingerprint_input(remove: &[AttributeKey]) -> Vec<&str> {
    let mut remove: Vec<&str> = remove.iter().map(AttributeKey::as_str).collect();
    remove.sort_unstable();
    remove.dedup();
    remove
}

fn grants_fingerprint_input(grants: &AccessGrants) -> BTreeMap<&str, Vec<&'static str>> {
    grants
        .as_map()
        .iter()
        .map(|(principal, rights)| {
            (
                principal.as_str(),
                rights.iter().map(AccessRight::as_str).collect(),
            )
        })
        .collect()
}

/// The inputs of one request's semantic fingerprint, which validates a reused
/// commit ID.
///
/// A single-operation helper and a one-item batch produce the same input and
/// therefore the same fingerprint. Inline identities hash the original request
/// bytes. Other references keep their object identity.
#[derive(Debug, Clone, Copy)]
pub struct CommitFingerprintInput<'a> {
    /// The namespace that the request commits to.
    pub namespace_id: &'a NamespaceId,
    /// The actor responsible for the request.
    pub actor_id: &'a ActorId,
    /// The request's commit message.
    pub message: Option<&'a str>,
    /// The request's operations, in order.
    pub operations: &'a [FilesystemOperation],
    /// The request's preconditions, in order.
    pub preconditions: &'a [CommitPrecondition],
    /// The request-byte identities of inline content that was staged as a reference.
    pub inline_content_identities: &'a BTreeMap<ContentId, InlineContentIdentity>,
}

impl CommitFingerprintInput<'_> {
    /// Computes the semantic fingerprint of the request.
    pub fn fingerprint(&self) -> CommitFingerprint {
        fingerprint_bytes(&self.canonical_bytes())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        /// The preimage in the member order Appendix B specifies.
        #[derive(Serialize)]
        struct CanonicalCommit<'a> {
            domain: &'static str,
            namespace_id: &'a str,
            actor_id: &'a str,
            operations: Vec<OperationFingerprintInput<'a>>,
            message: Option<&'a str>,
            preconditions: Vec<PreconditionFingerprintInput<'a>>,
        }

        serde_json::to_vec(&CanonicalCommit {
            domain: COMMIT_FINGERPRINT_DOMAIN,
            namespace_id: self.namespace_id.as_str(),
            actor_id: self.actor_id.as_str(),
            operations: self
                .operations
                .iter()
                .map(|operation| {
                    operation_fingerprint_input(operation, self.inline_content_identities)
                })
                .collect(),
            message: self.message,
            preconditions: self
                .preconditions
                .iter()
                .map(precondition_fingerprint_input)
                .collect(),
        })
        .expect("a fingerprint preimage of strings and integers should encode as JSON")
    }
}

/// The inputs of one merge's semantic fingerprint, which validates a reused
/// commit ID.
///
/// The server plans a merge's operations from the merge's inputs, so the merge
/// record identifies the merge, and the preimage includes no operations and no
/// preconditions
/// ([Merge commits](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#b5-merge-commits)).
#[derive(Debug, Clone, Copy)]
pub struct MergeFingerprintInput<'a> {
    /// The namespace that the merge commits to.
    pub namespace_id: &'a NamespaceId,
    /// The actor responsible for the merge.
    pub actor_id: &'a ActorId,
    /// The fork and the range of its commits that the merge applies.
    pub merge: &'a MergeRecord,
    /// The merge's commit message.
    pub message: Option<&'a str>,
}

impl MergeFingerprintInput<'_> {
    /// Computes the semantic fingerprint of the merge.
    pub fn fingerprint(&self) -> CommitFingerprint {
        fingerprint_bytes(&self.canonical_bytes())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        /// The preimage in the member order Appendix B specifies.
        #[derive(Serialize)]
        struct CanonicalMerge<'a> {
            domain: &'static str,
            namespace_id: &'a str,
            actor_id: &'a str,
            merge: CanonicalMergeRange<'a>,
            message: Option<&'a str>,
        }

        /// The merge record's members, spelled apart from its durable encoding.
        #[derive(Serialize)]
        struct CanonicalMergeRange<'a> {
            source_namespace_id: &'a str,
            source_after_seq: u64,
            source_through_seq: u64,
        }

        serde_json::to_vec(&CanonicalMerge {
            domain: MERGE_FINGERPRINT_DOMAIN,
            namespace_id: self.namespace_id.as_str(),
            actor_id: self.actor_id.as_str(),
            merge: CanonicalMergeRange {
                source_namespace_id: self.merge.source_namespace_id.as_str(),
                source_after_seq: self.merge.source_after_seq.0,
                source_through_seq: self.merge.source_through_seq.0,
            },
            message: self.message,
        })
        .expect("a fingerprint preimage of strings and integers should encode as JSON")
    }
}

#[cfg(test)]
mod tests;
