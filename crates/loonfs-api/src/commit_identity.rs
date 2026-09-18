//! Generates stable fingerprints for filesystem mutations (format spec,
//! "Semantic commit fingerprints"). A fingerprint lets LoonFS determine
//! whether two requests that use the same commit ID describe the same
//! mutation.
//!
//! The publisher stores this fingerprint in the commit receipt and compares
//! it when the same commit ID is submitted again.
//!
//! The commit ID is not part of the fingerprint input. The commit ID selects
//! a receipt, while the fingerprint describes the mutation stored in that
//! receipt.

use crate::{
    AbsolutePath, AccessRevisionNo, AccessRight, ActorId, AttributeRevisionNo, ChangeSeq,
    ChecksumAlgorithm, CommitPrecondition, ContentId, ContentRef, ContentRefKind,
    DeleteDirectoryBehavior, DestinationBehavior, FilesystemOperation, InodeId, NamespaceId,
    RevisionNo, SubjectId,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Domain separator included in every mutation fingerprint input.
const COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.commit.semantic.v1";

/// Format version and hash algorithm stored with each fingerprint.
///
/// Storing both values lets a later format use different encoding rules or a
/// different hash without changing existing fingerprints.
const FINGERPRINT_SCHEME: &str = "v1:sha256";

/// The semantic identity of one mutation request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, serde::Deserialize)]
pub struct CommitFingerprint(String);

impl CommitFingerprint {
    /// Returns the stored fingerprint string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A request cannot be represented by the version 1 fingerprint scheme.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SemanticFingerprintError {
    /// An operation supplies both content sources or neither.
    #[error("exactly one of `content_ref` and `inline_content` is required")]
    InvalidContentSource,
    /// Canonical JSON encoding failed.
    #[error("failed to encode the commit fingerprint preimage: {0}")]
    Encode(#[from] serde_json::Error),
    /// Inline identity requires a SHA-256 checksum.
    #[error("inline content `{content_id}` requires `sha256`, found `{actual_algorithm}`")]
    InlineChecksumAlgorithm {
        /// Content whose reference cannot supply the digest.
        content_id: ContentId,
        /// Algorithm present on the reference.
        actual_algorithm: ChecksumAlgorithm,
    },
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
/// The serde representation is durable contract (format spec, "Semantic
/// commit fingerprints"): the same normalized request must fingerprint
/// identically across releases. A pinned-value test below fails if the
/// encoding drifts.
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
    MoveByInode {
        inode_id: InodeId,
        expected_binding_generation: &'a str,
        destination_parent_inode_id: InodeId,
        destination_display_name: &'a str,
        behavior: DestinationBehavior,
        expected_destination_inode_id: Option<InodeId>,
        expected_destination_revision_no: Option<RevisionNo>,
    },
    DeleteByInode {
        inode_id: InodeId,
        expected_binding_generation: &'a str,
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
    RestoreRevision {
        path: &'a str,
        source_revision_no: RevisionNo,
    },
    Undelete {
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        destination_path: Option<&'a str>,
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
        expected_attributes_revision_no: Option<AttributeRevisionNo>,
    },
    UpdateAccess {
        path: &'a str,
        boundary: bool,
        grants: BTreeMap<&'a str, Vec<&'static str>>,
        expected_inode_id: Option<InodeId>,
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PreconditionFingerprintInput<'a> {
    NamespaceHead {
        expected_head_seq: ChangeSeq,
    },
    FileRevision {
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
    },
    PathBinding {
        path: &'a str,
        expected_inode_id: InodeId,
        expected_binding_generation: Option<&'a str>,
    },
    PathAbsence {
        path: &'a str,
    },
    AttributesRevision {
        inode_id: InodeId,
        expected_attributes_revision_no: AttributeRevisionNo,
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
            expected_binding_generation,
        } => PreconditionFingerprintInput::PathBinding {
            path: path.as_str(),
            expected_inode_id: *expected_inode_id,
            expected_binding_generation: expected_binding_generation
                .as_ref()
                .map(|value| value.as_str()),
        },
        CommitPrecondition::PathAbsence { path } => PreconditionFingerprintInput::PathAbsence {
            path: path.as_str(),
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

/// Canonical preimage for the content a put attaches.
///
/// For the reference form, identity is *which object*, so the id and its
/// length are the whole of it. The checksum is evidence about those bytes,
/// pinned to the id by the verification every write and read already performs,
/// and it is left out deliberately: a reference that named the same object
/// with a differently spelled checksum would otherwise read as a different mutation.
///
/// For that form, a retry that re-runs the whole operation, upload included,
/// creates a new content object, so it is a different request and a reused
/// commit id conflicts. Retrying a commit means sending the same `ContentRef`
/// again, which replays, rather than uploading the bytes again.
///
/// Inline content has no object to name, so its bytes identify it. The form
/// follows how the request supplied the content, never where the bytes end up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ContentRefFingerprintInput<'a> {
    BlobV1 {
        content_id: &'a str,
        size_bytes: u64,
    },
    InlineV1 {
        sha256: Cow<'a, str>,
        size_bytes: u64,
    },
}

fn content_ref_fingerprint_input<'a>(
    content_ref: &'a ContentRef,
    inline_content_ids: &BTreeSet<ContentId>,
) -> Result<ContentRefFingerprintInput<'a>, SemanticFingerprintError> {
    if inline_content_ids.contains(&content_ref.content_id) {
        if content_ref.checksum.algorithm != ChecksumAlgorithm::Sha256 {
            return Err(SemanticFingerprintError::InlineChecksumAlgorithm {
                content_id: content_ref.content_id.clone(),
                actual_algorithm: content_ref.checksum.algorithm,
            });
        }
        Ok(ContentRefFingerprintInput::InlineV1 {
            sha256: Cow::Borrowed(&content_ref.checksum.value),
            size_bytes: content_ref.size_bytes,
        })
    } else {
        match content_ref.kind {
            ContentRefKind::BlobV1 => Ok(ContentRefFingerprintInput::BlobV1 {
                content_id: content_ref.content_id.as_str(),
                size_bytes: content_ref.size_bytes,
            }),
        }
    }
}

fn content_fingerprint_input<'a>(
    content_ref: Option<&'a ContentRef>,
    inline_content: Option<&[u8]>,
    inline_content_ids: &BTreeSet<ContentId>,
) -> Result<ContentRefFingerprintInput<'a>, SemanticFingerprintError> {
    match (content_ref, inline_content) {
        (Some(reference), None) => content_ref_fingerprint_input(reference, inline_content_ids),
        (None, Some(bytes)) => Ok(ContentRefFingerprintInput::InlineV1 {
            sha256: Cow::Owned(crate::Checksum::sha256(bytes).value),
            size_bytes: bytes.len() as u64,
        }),
        _ => Err(SemanticFingerprintError::InvalidContentSource),
    }
}

/// Normalizes one operation into its durable semantic representation.
/// Attribute removals are a sorted set; content checksums are verification
/// evidence and excluded from identity in the reference form.
fn operation_fingerprint_input<'a>(
    operation: &'a FilesystemOperation,
    inline_content_ids: &BTreeSet<ContentId>,
) -> Result<OperationFingerprintInput<'a>, SemanticFingerprintError> {
    Ok(match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            OperationFingerprintInput::CreateDirectory {
                path: path.as_str(),
                parents: *parents,
            }
        }
        FilesystemOperation::PutFile {
            path,
            content_ref,
            inline_content,
            behavior,
            expected_inode_id,
            expected_revision_no,
        } => OperationFingerprintInput::PutFile {
            path: path.as_str(),
            behavior: *behavior,
            content_ref: content_fingerprint_input(
                content_ref.as_ref(),
                inline_content.as_deref(),
                inline_content_ids,
            )?,
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
            content_ref,
            inline_content,
        } => OperationFingerprintInput::CreateFileByInode {
            parent_inode_id: *parent_inode_id,
            display_name: display_name.as_str(),
            content_ref: content_fingerprint_input(
                content_ref.as_ref(),
                inline_content.as_deref(),
                inline_content_ids,
            )?,
        },
        FilesystemOperation::PutFileRevisionByInode {
            inode_id,
            content_ref,
            inline_content,
            expected_revision_no,
        } => OperationFingerprintInput::PutFileRevisionByInode {
            inode_id: *inode_id,
            content_ref: content_fingerprint_input(
                content_ref.as_ref(),
                inline_content.as_deref(),
                inline_content_ids,
            )?,
            expected_revision_no: *expected_revision_no,
        },
        FilesystemOperation::MoveByInode {
            inode_id,
            expected_binding_generation,
            destination_parent_inode_id,
            destination_display_name,
            precondition,
        } => OperationFingerprintInput::MoveByInode {
            inode_id: *inode_id,
            expected_binding_generation: expected_binding_generation.as_str(),
            destination_parent_inode_id: *destination_parent_inode_id,
            destination_display_name: destination_display_name.as_str(),
            behavior: precondition.behavior,
            expected_destination_inode_id: precondition.expected_inode_id,
            expected_destination_revision_no: precondition.expected_revision_no,
        },
        FilesystemOperation::DeleteByInode {
            inode_id,
            expected_binding_generation,
            behavior,
        } => OperationFingerprintInput::DeleteByInode {
            inode_id: *inode_id,
            expected_binding_generation: expected_binding_generation.as_str(),
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
        FilesystemOperation::RestoreRevision {
            path,
            source_revision_no,
        } => OperationFingerprintInput::RestoreRevision {
            path: path.as_str(),
            source_revision_no: *source_revision_no,
        },
        FilesystemOperation::Undelete {
            inode_id,
            deletion_seq,
            destination_path,
        } => OperationFingerprintInput::Undelete {
            inode_id: *inode_id,
            deletion_seq: *deletion_seq,
            destination_path: destination_path.as_ref().map(AbsolutePath::as_str),
        },
        FilesystemOperation::UpdateAttributes {
            path,
            set,
            remove,
            expected_inode_id,
            expected_attributes_revision_no,
        } => {
            // The wire type preserves the caller's list so validation can
            // report duplicate keys. The fingerprint uses the sorted, unique
            // set because order and duplicate entries do not change the
            // requested mutation.
            let mut remove: Vec<&str> = remove.iter().map(|key| key.as_str()).collect();
            remove.sort_unstable();
            remove.dedup();
            OperationFingerprintInput::UpdateAttributes {
                path: path.as_str(),
                set: set
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect(),
                remove,
                expected_inode_id: *expected_inode_id,
                expected_attributes_revision_no: *expected_attributes_revision_no,
            }
        }
        FilesystemOperation::UpdateAccess {
            path,
            boundary,
            grants,
            expected_inode_id,
            expected_access_revision_no,
        } => OperationFingerprintInput::UpdateAccess {
            path: path.as_str(),
            boundary: *boundary,
            grants: grants
                .as_map()
                .iter()
                .map(|(principal, rights)| {
                    (
                        principal.as_str(),
                        rights.iter().map(AccessRight::as_str).collect(),
                    )
                })
                .collect(),
            expected_inode_id: *expected_inode_id,
            expected_access_revision_no: *expected_access_revision_no,
        },
    })
}

/// Computes the semantic fingerprint used to validate a reused commit ID.
///
/// A single-operation helper and a one-item batch produce the same input and
/// therefore the same fingerprint.
///
/// IDs in `inline_content_ids` use the SHA-256 checksum from their reference
/// as content identity. Other references keep their object identity.
pub fn semantic_commit_fingerprint(
    namespace_id: &NamespaceId,
    actor: &ActorId,
    subject_id: Option<&SubjectId>,
    message: Option<&str>,
    operations: &[FilesystemOperation],
    preconditions: &[CommitPrecondition],
    inline_content_ids: &BTreeSet<ContentId>,
) -> Result<CommitFingerprint, SemanticFingerprintError> {
    Ok(fingerprint_bytes(&canonical_commit_bytes(
        namespace_id,
        actor,
        subject_id,
        message,
        operations,
        preconditions,
        inline_content_ids,
    )?))
}

fn canonical_commit_bytes(
    namespace_id: &NamespaceId,
    actor: &ActorId,
    subject_id: Option<&SubjectId>,
    message: Option<&str>,
    operations: &[FilesystemOperation],
    preconditions: &[CommitPrecondition],
    inline_content_ids: &BTreeSet<ContentId>,
) -> Result<Vec<u8>, SemanticFingerprintError> {
    #[derive(Serialize)]
    struct CanonicalCommit<'a> {
        domain: &'static str,
        namespace_id: &'a str,
        actor_id: &'a str,
        subject_id: Option<&'a str>,
        operations: Vec<OperationFingerprintInput<'a>>,
        message: Option<&'a str>,
        preconditions: Vec<PreconditionFingerprintInput<'a>>,
    }

    Ok(serde_json::to_vec(&CanonicalCommit {
        domain: COMMIT_FINGERPRINT_DOMAIN,
        namespace_id: namespace_id.as_str(),
        actor_id: actor.as_str(),
        subject_id: subject_id.map(SubjectId::as_str),
        operations: operations
            .iter()
            .map(|operation| operation_fingerprint_input(operation, inline_content_ids))
            .collect::<Result<_, _>>()?,
        message,
        preconditions: preconditions
            .iter()
            .map(precondition_fingerprint_input)
            .collect(),
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::PutFileOptions;
    use crate::{
        ActorId, AttributeKey, AttributeValue, Checksum, ContentId, ContentRefKind, DisplayName,
    };

    #[test]
    fn canonical_bytes_and_digests_match_shared_vectors() {
        #[derive(Serialize, serde::Deserialize)]
        struct Vector {
            name: String,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            subject_id: Option<SubjectId>,
            operation: FilesystemOperation,
            #[serde(default)]
            preconditions: Vec<crate::CommitPrecondition>,
            #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
            inline_content_ids: BTreeSet<ContentId>,
            canonical_json: String,
            fingerprint: String,
        }
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/commit_fingerprints_v1.json");
        let mut vectors: Vec<Vector> = serde_json::from_str(
            &std::fs::read_to_string(&fixture_path).expect("read fingerprint vectors"),
        )
        .expect("fingerprint vectors");
        let update = std::env::var_os("UPDATE_GOLDEN").is_some();
        for vector in &mut vectors {
            let namespace = NamespaceId::parse("demo").expect("namespace");
            let operations = [vector.operation.clone()];
            let bytes = canonical_commit_bytes(
                &namespace,
                &test_actor(),
                vector.subject_id.as_ref(),
                None,
                &operations,
                &vector.preconditions,
                &vector.inline_content_ids,
            )
            .expect("canonical bytes");
            if update {
                vector.canonical_json = String::from_utf8(bytes.clone()).expect("canonical UTF-8");
                vector.fingerprint = fingerprint_bytes(&bytes).as_str().to_owned();
            }
            assert_eq!(
                bytes,
                vector.canonical_json.as_bytes(),
                "{} canonical bytes",
                vector.name
            );
            let fingerprint = semantic_commit_fingerprint(
                &namespace,
                &test_actor(),
                vector.subject_id.as_ref(),
                None,
                &operations,
                &vector.preconditions,
                &vector.inline_content_ids,
            )
            .expect("fingerprint");
            assert_eq!(
                fingerprint.as_str(),
                vector.fingerprint,
                "{} digest",
                vector.name
            );
        }
        if update {
            let json =
                serde_json::to_string_pretty(&vectors).expect("serialize fingerprint vectors");
            std::fs::write(fixture_path, format!("{json}\n")).expect("write fingerprint vectors");
        }
    }

    #[test]
    #[allow(
        clippy::panic,
        reason = "unexpected results need precise test diagnostics"
    )]
    fn inline_identity_rejects_other_checksum_algorithms() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let content_id = ContentId::generate();
        let mut content_ref =
            ContentRef::blob_v1(namespace_id.clone(), content_id.clone(), b"bytes");
        content_ref.checksum = Checksum::crc32c(b"bytes");
        let operation = FilesystemOperation::PutFileRevisionByInode {
            inode_id: InodeId(2),
            content_ref: Some(content_ref),
            inline_content: None,
            expected_revision_no: RevisionNo(1),
        };
        match semantic_commit_fingerprint(
            &namespace_id,
            &test_actor(),
            None,
            None,
            &[operation],
            &[],
            &BTreeSet::from([content_id.clone()]),
        ) {
            Err(SemanticFingerprintError::InlineChecksumAlgorithm {
                content_id: actual_content_id,
                actual_algorithm,
            }) => {
                assert_eq!(actual_content_id, content_id);
                assert_eq!(actual_algorithm, ChecksumAlgorithm::Crc32c);
            }
            other => panic!("expected InlineChecksumAlgorithm, got {other:?}"),
        }
    }

    fn test_actor() -> ActorId {
        ActorId::parse("test-actor").expect("valid test actor id")
    }

    fn attribute_key(value: &str) -> AttributeKey {
        AttributeKey::parse(value).expect("valid attribute key")
    }

    fn text(value: &str) -> AttributeValue {
        AttributeValue::parse(value).expect("valid attribute value")
    }

    fn fingerprint(operation: FilesystemOperation) -> String {
        semantic_commit_fingerprint(
            &NamespaceId::parse("demo").expect("valid namespace id"),
            &test_actor(),
            None,
            None,
            &[operation],
            &[],
            &BTreeSet::new(),
        )
        .expect("fingerprint")
        .as_str()
        .to_owned()
    }

    fn update_attributes(
        set: impl IntoIterator<Item = (&'static str, AttributeValue)>,
        remove: impl IntoIterator<Item = &'static str>,
        expected_inode_id: Option<InodeId>,
        expected_attributes_revision_no: Option<AttributeRevisionNo>,
    ) -> FilesystemOperation {
        FilesystemOperation::UpdateAttributes {
            path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            set: set
                .into_iter()
                .map(|(key, value)| (attribute_key(key), value))
                .collect(),
            remove: remove.into_iter().map(attribute_key).collect(),
            expected_inode_id,
            expected_attributes_revision_no,
        }
    }

    #[test]
    fn json_map_order_does_not_change_attribute_update_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let forward: FilesystemOperation = serde_json::from_str(
            r#"{"kind":"update_attributes","path":"/docs/report.txt",
                "set":{"a":"1","b":"2"}}"#,
        )
        .expect("forward operation");
        let reversed: FilesystemOperation = serde_json::from_str(
            r#"{"kind":"update_attributes","path":"/docs/report.txt",
                "set":{"b":"2","a":"1"}}"#,
        )
        .expect("reversed operation");

        assert_eq!(
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                None,
                &[forward],
                &[],
                &BTreeSet::new()
            )
            .expect("forward"),
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                None,
                &[reversed],
                &[],
                &BTreeSet::new()
            )
            .expect("reversed")
        );
    }

    #[test]
    fn remove_order_and_repeats_do_not_change_attribute_update_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let baseline = semantic_commit_fingerprint(
            &namespace_id,
            &test_actor(),
            None,
            None,
            &[update_attributes([], ["a", "b"], None, None)],
            &[],
            &BTreeSet::new(),
        )
        .expect("baseline");

        for spelling in [vec!["b", "a"], vec!["a", "b", "a"]] {
            assert_eq!(
                semantic_commit_fingerprint(
                    &namespace_id,
                    &test_actor(),
                    None,
                    None,
                    &[update_attributes([], spelling, None, None)],
                    &[],
                    &BTreeSet::new()
                )
                .expect("variant"),
                baseline
            );
        }
    }

    #[test]
    fn attribute_update_fingerprint_changes_with_every_request_field() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let baseline = semantic_commit_fingerprint(
            &namespace_id,
            &test_actor(),
            None,
            None,
            &[update_attributes(
                [("owner", text("ada"))],
                ["draft"],
                None,
                None,
            )],
            &[],
            &BTreeSet::new(),
        )
        .expect("baseline");

        for (label, variant) in [
            (
                "set value",
                update_attributes([("owner", text("grace"))], ["draft"], None, None),
            ),
            (
                "removed key",
                update_attributes([("owner", text("ada"))], ["final"], None, None),
            ),
            (
                "expected inode",
                update_attributes([("owner", text("ada"))], ["draft"], Some(InodeId(42)), None),
            ),
            (
                "expected attribute revision",
                update_attributes(
                    [("owner", text("ada"))],
                    ["draft"],
                    None,
                    Some(AttributeRevisionNo(0)),
                ),
            ),
        ] {
            assert_ne!(
                baseline,
                semantic_commit_fingerprint(
                    &namespace_id,
                    &test_actor(),
                    None,
                    None,
                    &[variant],
                    &[],
                    &BTreeSet::new()
                )
                .expect("variant fingerprint"),
                "a changed {label} must change the fingerprint"
            );
        }
    }

    #[test]
    fn access_update_fingerprint_changes_with_every_request_field() {
        let baseline = serde_json::json!({
            "kind": "update_access",
            "path": "/docs/secret",
            "boundary": true,
            "grants": {"prn_ada": ["read", "write"]},
            "expected_inode_id": "ino_9",
            "expected_access_revision_no": 2
        });
        let baseline_fingerprint =
            fingerprint(serde_json::from_value(baseline.clone()).expect("operation"));
        for (field, value) in [
            ("path", serde_json::json!("/docs/other")),
            ("boundary", serde_json::json!(false)),
            ("grants", serde_json::json!({"prn_ada": ["read"]})),
            ("expected_inode_id", serde_json::json!("ino_10")),
            ("expected_access_revision_no", serde_json::json!(3)),
        ] {
            let mut variant = baseline.clone();
            variant[field] = value;
            assert_ne!(
                baseline_fingerprint,
                fingerprint(serde_json::from_value(variant).expect("variant")),
                "a changed {field} must change the fingerprint"
            );
        }
    }

    #[test]
    fn binding_generation_changes_inode_mutation_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let operation = |expected_binding_generation: &str| FilesystemOperation::MoveByInode {
            inode_id: InodeId(42),
            expected_binding_generation: crate::BindingGeneration::parse(
                expected_binding_generation,
            )
            .expect("binding generation"),
            destination_parent_inode_id: InodeId(7),
            destination_display_name: DisplayName::parse("report.txt").expect("display name"),
            precondition: crate::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        };

        let fingerprint = |generation| {
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                None,
                &[operation(generation)],
                &[],
                &BTreeSet::new(),
            )
            .expect("fingerprint")
        };

        assert_ne!(fingerprint("aaaa"), fingerprint("bbbb"));
    }

    #[test]
    fn changed_actor_id_changes_the_fingerprint() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let operation = create_dir("/docs");
        let actor_x = ActorId::parse("x").expect("actor id");
        let actor_y = ActorId::parse("y").expect("actor id");

        let fingerprint = |actor: &ActorId| {
            semantic_commit_fingerprint(
                &namespace_id,
                actor,
                None,
                None,
                std::slice::from_ref(&operation),
                &[],
                &BTreeSet::new(),
            )
            .expect("fingerprint")
        };
        assert_ne!(fingerprint(&actor_x), fingerprint(&actor_y));
    }

    #[test]
    fn changed_subject_id_changes_the_fingerprint() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let operation = create_dir("/docs");
        let subject_x = SubjectId::parse("x").expect("subject id");
        let subject_y = SubjectId::parse("y").expect("subject id");
        let fingerprint = |subject_id| {
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                subject_id,
                None,
                std::slice::from_ref(&operation),
                &[],
                &BTreeSet::new(),
            )
            .expect("fingerprint")
        };
        assert_ne!(fingerprint(Some(&subject_x)), fingerprint(Some(&subject_y)));
        assert_ne!(fingerprint(None), fingerprint(Some(&subject_x)));
    }

    #[test]
    fn put_file_preconditions_change_the_fingerprint_deterministically() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"put bytes",
        );
        let operation = |expected_inode_id, expected_revision_no| FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            content_ref: Some(content_ref.clone()),
            inline_content: None,
            behavior: DestinationBehavior::Replace,
            expected_inode_id,
            expected_revision_no,
        };

        let without_preconditions = fingerprint(operation(None, None));
        let inode_only = fingerprint(operation(Some(InodeId(7)), None));
        let first_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))));
        let next_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(4))));

        assert_ne!(without_preconditions, inode_only);
        assert_ne!(inode_only, first_revision);
        assert_ne!(first_revision, next_revision);
        assert_eq!(
            first_revision,
            fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))))
        );
    }

    fn assert_destination_preconditions_change_fingerprint(
        operation: impl Fn(Option<InodeId>, Option<RevisionNo>) -> FilesystemOperation,
    ) {
        let without_preconditions = fingerprint(operation(None, None));
        let first_inode = fingerprint(operation(Some(InodeId(7)), None));
        let other_inode = fingerprint(operation(Some(InodeId(8)), None));
        let first_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))));
        let other_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(4))));

        assert_ne!(without_preconditions, first_inode);
        assert_ne!(first_inode, other_inode);
        assert_ne!(first_inode, first_revision);
        assert_ne!(first_revision, other_revision);
        assert_eq!(
            first_revision,
            fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))))
        );
    }

    #[test]
    fn move_and_copy_destination_preconditions_change_the_fingerprint_deterministically() {
        assert_destination_preconditions_change_fingerprint(|inode_id, revision_no| {
            FilesystemOperation::MovePath {
                source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
                destination_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
                precondition: crate::DestinationPrecondition {
                    behavior: DestinationBehavior::Replace,
                    expected_inode_id: inode_id,
                    expected_revision_no: revision_no,
                },
            }
        });
        assert_destination_preconditions_change_fingerprint(|inode_id, revision_no| {
            FilesystemOperation::CopyPath {
                source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
                destination_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
                precondition: crate::DestinationPrecondition {
                    behavior: DestinationBehavior::Replace,
                    expected_inode_id: inode_id,
                    expected_revision_no: revision_no,
                },
            }
        });
    }

    #[test]
    fn a_put_has_the_pinned_fingerprint_under_every_checksum_algorithm() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let content_id =
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id");
        let bytes = b"pinned put bytes";

        for content_ref in [
            ContentRef::blob_v1(
                crate::NamespaceId::parse("demo").expect("namespace id"),
                content_id.clone(),
                bytes,
            ),
            ContentRef {
                kind: ContentRefKind::BlobV1,
                owner_namespace_id: crate::NamespaceId::parse("demo").expect("namespace id"),
                content_id: content_id.clone(),
                size_bytes: bytes.len() as u64,
                checksum: Checksum::crc32c(bytes),
            },
            ContentRef {
                kind: ContentRefKind::BlobV1,
                owner_namespace_id: crate::NamespaceId::parse("demo").expect("namespace id"),
                content_id: content_id.clone(),
                size_bytes: bytes.len() as u64,
                checksum: Checksum::crc64nvme(bytes),
            },
        ] {
            assert_eq!(
                semantic_commit_fingerprint(
                    &namespace_id,
                    &test_actor(),
                    None,
                    None,
                    &[put("/docs/report.txt", content_ref)],
                    &[],
                    &BTreeSet::new()
                )
                .expect("retry fingerprint")
                .as_str(),
                "v1:sha256:713de4c58dac816a19e7fb4439074b8103d1bf377f08176027cf39eaa7dc3d2d"
            );
        }
    }

    fn create_dir(path: &str) -> FilesystemOperation {
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(path).expect("path"),
            parents: false,
        }
    }

    fn put(path: &str, content_ref: ContentRef) -> FilesystemOperation {
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse(path).expect("path"),
            content_ref: Some(content_ref),
            inline_content: None,
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        }
    }

    #[test]
    fn a_different_content_object_changes_mutation_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let bytes = b"identical bytes, two uploads";
        let first = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::generate(),
            bytes,
        );
        let second = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::generate(),
            bytes,
        );

        assert_ne!(
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                None,
                &[put("/docs/report.txt", first)],
                &[],
                &BTreeSet::new()
            )
            .expect("fingerprint"),
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                None,
                &[put("/docs/report.txt", second)],
                &[],
                &BTreeSet::new()
            )
            .expect("fingerprint")
        );
    }

    #[test]
    fn a_message_changes_mutation_identity() {
        // The annotation is part of what the caller asked for: replaying a
        // commit id with a different message must conflict, so the message
        // joins the preimage.
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let without = semantic_commit_fingerprint(
            &namespace_id,
            &test_actor(),
            None,
            None,
            &[create_dir("/docs")],
            &[],
            &BTreeSet::new(),
        )
        .expect("fingerprint");
        let with = semantic_commit_fingerprint(
            &namespace_id,
            &test_actor(),
            None,
            Some("import batch"),
            &[create_dir("/docs")],
            &[],
            &BTreeSet::new(),
        )
        .expect("fingerprint");

        assert_ne!(without, with);
    }

    #[test]
    fn operation_order_changes_mutation_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        assert_ne!(
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                None,
                &[create_dir("/a"), create_dir("/b")],
                &[],
                &BTreeSet::new()
            )
            .expect("forward fingerprint"),
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                None,
                &[create_dir("/b"), create_dir("/a")],
                &[],
                &BTreeSet::new()
            )
            .expect("reversed fingerprint")
        );
    }

    #[test]
    fn put_fingerprint_changes_with_every_request_field() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path = AbsolutePath::parse("/a.txt").expect("path");
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::generate(),
            b"hello",
        );
        let mut options = PutFileOptions::new(test_actor());
        options.behavior = DestinationBehavior::Replace;
        let fingerprint =
            |namespace_id: &NamespaceId, path: &AbsolutePath, options: &PutFileOptions| {
                semantic_commit_fingerprint(
                    namespace_id,
                    &options.commit.actor_id,
                    None,
                    options.commit.message.as_deref(),
                    &[FilesystemOperation::PutFile {
                        path: path.clone(),
                        content_ref: Some(content_ref.clone()),
                        inline_content: None,
                        behavior: options.behavior,
                        expected_inode_id: options.expected_inode_id,
                        expected_revision_no: options.expected_revision_no,
                    }],
                    &[],
                    &BTreeSet::new(),
                )
                .expect("retry fingerprint")
            };
        let baseline = fingerprint(&namespace_id, &path, &options);

        let mut changed_behavior = options.clone();
        changed_behavior.behavior = DestinationBehavior::NoReplace;
        let mut changed_inode = options.clone();
        changed_inode.expected_inode_id = Some(InodeId(2));
        let mut changed_revision = options.clone();
        changed_revision.expected_inode_id = Some(InodeId(2));
        changed_revision.expected_revision_no = Some(RevisionNo(2));
        let mut changed_message = options.clone();
        changed_message.commit.message = Some(String::new());

        for (label, fingerprint) in [
            (
                "path",
                fingerprint(
                    &namespace_id,
                    &AbsolutePath::parse("/b.txt").expect("path"),
                    &options,
                ),
            ),
            (
                "behavior",
                fingerprint(&namespace_id, &path, &changed_behavior),
            ),
            (
                "expected inode",
                fingerprint(&namespace_id, &path, &changed_inode),
            ),
            (
                "expected revision",
                fingerprint(&namespace_id, &path, &changed_revision),
            ),
            (
                "message",
                fingerprint(&namespace_id, &path, &changed_message),
            ),
            (
                "namespace",
                fingerprint(
                    &NamespaceId::parse("other").expect("valid namespace id"),
                    &path,
                    &options,
                ),
            ),
        ] {
            assert_ne!(
                baseline, fingerprint,
                "changed {label} must change identity"
            );
        }
    }
}
