//! Generates stable fingerprints for filesystem mutations (format spec,
//! "Commit identity fingerprints"). A fingerprint lets LoonFS determine
//! whether two requests that use the same commit ID describe the same
//! mutation.
//!
//! The runtime and HTTP client use the functions in this module so that they
//! apply the same identity rules. The runtime stores a fingerprint in the
//! commit receipt. A client can later recompute it when retrying a request.
//!
//! The commit ID is not part of the fingerprint input. The commit ID selects
//! a receipt, while the fingerprint describes the mutation stored in that
//! receipt.

use crate::options::PutFileOptions;
use crate::{
    AbsolutePath, ActorKind, ActorRef, AttributeRevisionNo, ChangeSeq, CommitId, ContentEvidence,
    ContentRef, DeleteDirectoryBehavior, DestinationBehavior, FilesystemOperation, InodeId,
    NamespaceId, RevisionNo,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::future::Future;
use thiserror::Error;

/// Domain separator included in every mutation fingerprint input.
const COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.commit.semantic.v2";

/// Format version and hash algorithm stored with each fingerprint.
///
/// Storing both values lets a later format use different encoding rules or a
/// different hash without changing existing fingerprints.
const FINGERPRINT_SCHEME: &str = "v2:sha256";

/// The semantic identity of one mutation request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, serde::Deserialize)]
pub struct CommitFingerprint(String);

impl CommitFingerprint {
    /// Returns the stored fingerprint string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Error returned when the canonical fingerprint input cannot be encoded.
///
/// The input contains validated types, so this error indicates an internal
/// encoding bug rather than invalid caller data.
#[derive(Debug, Error)]
#[error("failed to encode the commit fingerprint preimage: {0}")]
pub struct SemanticFingerprintError(#[from] serde_json::Error);

fn fingerprint_bytes(bytes: &[u8]) -> CommitFingerprint {
    let digest = Sha256::digest(bytes);
    CommitFingerprint(format!(
        "{FINGERPRINT_SCHEME}:{}",
        crate::hex::hex_encode_bytes(&digest)
    ))
}

/// Canonical preimage for one operation inside a mutation fingerprint.
///
/// The serde representation is durable contract (format spec, "Commit
/// identity fingerprints"): the same normalized request must fingerprint
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
    PutFileByInode {
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
        to_parent_inode_id: InodeId,
        to_display_name: &'a str,
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
    // delete guard must conflict instead of replaying the old receipt
    // without checking the new guard.
    DeletePath {
        path: &'a str,
        behavior: DeleteDirectoryBehavior,
        expected_inode_id: Option<InodeId>,
    },
    MovePath {
        from_path: &'a str,
        to_path: &'a str,
        behavior: DestinationBehavior,
        expected_destination_inode_id: Option<InodeId>,
        expected_destination_revision_no: Option<RevisionNo>,
    },
    CopyPath {
        from_path: &'a str,
        to_path: &'a str,
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
        path: Option<&'a str>,
    },
    // Both guards join the preimage for the same reason the delete guard
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
}

/// Canonical actor shape, matching the request and durable actor vocabulary.
#[derive(Serialize)]
struct ActorFingerprintInput<'a> {
    kind: ActorKind,
    id: &'a str,
}

/// Canonical preimage for the content a put attaches.
///
/// Identity is *which object*, so the id and its length are the whole of it.
/// The checksum is evidence about those bytes, pinned to the id by the
/// verification every write and read already performs, and it is left out
/// deliberately: a reference that named the same object with a differently
/// spelled checksum would otherwise read as a different mutation.
///
/// The consequence is worth stating plainly. A retry that re-runs the whole
/// operation, upload included, mints a new content object, so it is a
/// different request and a reused commit id conflicts. Retrying a commit
/// means sending the same `ContentRef` again — which replays — not uploading
/// the bytes again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ContentRefFingerprintInput<'a> {
    kind: &'a str,
    content_id: &'a str,
    size_bytes: u64,
}

fn content_ref_fingerprint_input(content_ref: &ContentRef) -> ContentRefFingerprintInput<'_> {
    ContentRefFingerprintInput {
        kind: content_ref.kind.as_str(),
        content_id: content_ref.content_id.as_str(),
        size_bytes: content_ref.size_bytes,
    }
}

/// Normalizes one operation into its durable semantic representation.
/// Attribute removals are a sorted set; content checksums are verification
/// evidence and excluded from identity.
fn operation_fingerprint_input(operation: &FilesystemOperation) -> OperationFingerprintInput<'_> {
    match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            OperationFingerprintInput::CreateDirectory {
                path: path.as_str(),
                parents: *parents,
            }
        }
        FilesystemOperation::PutFile {
            path,
            content_ref,
            behavior,
            expected_inode_id,
            expected_revision_no,
        } => OperationFingerprintInput::PutFile {
            path: path.as_str(),
            behavior: *behavior,
            content_ref: content_ref_fingerprint_input(content_ref),
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
        FilesystemOperation::PutFileByInode {
            parent_inode_id,
            display_name,
            content_ref,
        } => OperationFingerprintInput::PutFileByInode {
            parent_inode_id: *parent_inode_id,
            display_name: display_name.as_str(),
            content_ref: content_ref_fingerprint_input(content_ref),
        },
        FilesystemOperation::PutFileRevisionByInode {
            inode_id,
            content_ref,
            expected_revision_no,
        } => OperationFingerprintInput::PutFileRevisionByInode {
            inode_id: *inode_id,
            content_ref: content_ref_fingerprint_input(content_ref),
            expected_revision_no: *expected_revision_no,
        },
        FilesystemOperation::MoveByInode {
            inode_id,
            expected_binding_generation,
            to_parent_inode_id,
            to_display_name,
            guard,
        } => OperationFingerprintInput::MoveByInode {
            inode_id: *inode_id,
            expected_binding_generation: expected_binding_generation.as_str(),
            to_parent_inode_id: *to_parent_inode_id,
            to_display_name: to_display_name.as_str(),
            behavior: guard.behavior,
            expected_destination_inode_id: guard.expected_inode_id,
            expected_destination_revision_no: guard.expected_revision_no,
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
            from_path,
            to_path,
            guard,
        } => OperationFingerprintInput::MovePath {
            from_path: from_path.as_str(),
            to_path: to_path.as_str(),
            behavior: guard.behavior,
            expected_destination_inode_id: guard.expected_inode_id,
            expected_destination_revision_no: guard.expected_revision_no,
        },
        FilesystemOperation::CopyPath {
            from_path,
            to_path,
            guard,
        } => OperationFingerprintInput::CopyPath {
            from_path: from_path.as_str(),
            to_path: to_path.as_str(),
            behavior: guard.behavior,
            expected_destination_inode_id: guard.expected_inode_id,
            expected_destination_revision_no: guard.expected_revision_no,
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
            path,
        } => OperationFingerprintInput::Undelete {
            inode_id: *inode_id,
            deletion_seq: *deletion_seq,
            path: path.as_ref().map(AbsolutePath::as_str),
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
    }
}

/// Computes the semantic fingerprint used to validate a reused commit ID.
///
/// A single-operation helper and a one-item batch produce the same input and
/// therefore the same fingerprint.
pub fn semantic_commit_fingerprint(
    namespace_id: &NamespaceId,
    actor: &ActorRef,
    message: Option<&str>,
    operations: &[FilesystemOperation],
) -> Result<CommitFingerprint, SemanticFingerprintError> {
    Ok(fingerprint_bytes(&canonical_commit_bytes(
        namespace_id,
        actor,
        message,
        operations,
    )?))
}

fn canonical_commit_bytes(
    namespace_id: &NamespaceId,
    actor: &ActorRef,
    message: Option<&str>,
    operations: &[FilesystemOperation],
) -> Result<Vec<u8>, SemanticFingerprintError> {
    #[derive(Serialize)]
    struct CanonicalCommit<'a> {
        domain: &'static str,
        namespace_id: &'a str,
        actor: ActorFingerprintInput<'a>,
        operations: Vec<OperationFingerprintInput<'a>>,
        message: Option<&'a str>,
    }

    Ok(serde_json::to_vec(&CanonicalCommit {
        domain: COMMIT_FINGERPRINT_DOMAIN,
        namespace_id: namespace_id.as_str(),
        actor: ActorFingerprintInput {
            kind: actor.kind,
            id: actor.id.as_str(),
        },
        operations: operations.iter().map(operation_fingerprint_input).collect(),
        message,
    })?)
}

/// Computes the fingerprint for a retried single-file PUT using the content
/// reference from the original commit.
///
/// Retrying an upload creates a new content object, so its content ID differs
/// from the ID stored by the original commit. This function substitutes the
/// original content reference before computing the fingerprint. The caller
/// must separately verify that both content objects contain the same bytes.
pub fn put_retry_fingerprint(
    namespace_id: &NamespaceId,
    path: &AbsolutePath,
    options: &PutFileOptions,
    committed_content_ref: &ContentRef,
) -> Result<CommitFingerprint, SemanticFingerprintError> {
    let operation = FilesystemOperation::PutFile {
        path: path.clone(),
        content_ref: committed_content_ref.clone(),
        behavior: options.behavior,
        expected_inode_id: options.expected_inode_id,
        expected_revision_no: options.expected_revision_no,
    };
    semantic_commit_fingerprint(
        namespace_id,
        &options.commit.actor,
        options.commit.message.as_deref(),
        std::slice::from_ref(&operation),
    )
}

/// Receipt data needed to verify a PUT that reused a commit ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutRetryReceipt {
    /// Sequence number assigned to the original commit.
    pub committed_seq: ChangeSeq,
    /// Semantic fingerprint stored in the original commit receipt.
    pub committed_fingerprint: String,
}

/// Classification of an error encountered while verifying a retried PUT.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PutRetryErrorClassification {
    /// The commit ID was already used. The receipt is included when available.
    CommitIdReuseConflict(Option<PutRetryReceipt>),
    /// Retention removed the change record needed to verify the retry.
    RebootstrapRequired,
    /// Any error that does not have special handling during retry verification.
    Other,
}

/// Details of the retried PUT being compared with an existing receipt.
#[derive(Debug, Clone, Copy)]
pub struct PutRetryAttempt<'a> {
    /// Namespace targeted by the PUT.
    pub namespace_id: &'a NamespaceId,
    /// Absolute path targeted by the PUT.
    pub path: &'a AbsolutePath,
    /// Commit ID that was already used.
    pub commit_id: &'a CommitId,
    /// PUT options supplied by the caller.
    pub options: &'a crate::options::PutFileOptions,
    /// Checksum or byte evidence for the new upload.
    pub staged: ContentEvidence<'a>,
}

/// Checks whether a PUT rejected for commit-ID reuse is an exact retry of an
/// earlier successful PUT.
///
/// `read_change` receives the change-feed sequence immediately before the
/// sequence in the receipt. It must return a page containing at most the
/// expected change.
///
/// The function returns the original commit response only when both the
/// request fingerprint and the uploaded content match the original commit.
/// It returns the original conflict when the receipt or change record is
/// missing, the retained history is unavailable, or either comparison fails.
/// Other errors from `read_change` are returned unchanged.
pub async fn reconcile_put_commit_id_reuse<E, ReadChange, ReadChangeFuture, ClassifyError>(
    attempt: PutRetryAttempt<'_>,
    conflict: E,
    read_change: ReadChange,
    classify_error: ClassifyError,
) -> Result<crate::v0::CommitResponse, E>
where
    ReadChange: FnOnce(ChangeSeq) -> ReadChangeFuture,
    ReadChangeFuture: Future<Output = Result<crate::v0::ListChangesResponse, E>>,
    ClassifyError: Fn(&E) -> PutRetryErrorClassification,
{
    let PutRetryErrorClassification::CommitIdReuseConflict(Some(receipt)) =
        classify_error(&conflict)
    else {
        return Err(conflict);
    };
    let after_seq = ChangeSeq(receipt.committed_seq.0.saturating_sub(1));
    let page = match read_change(after_seq).await {
        Ok(page) => page,
        Err(error)
            if matches!(
                classify_error(&error),
                PutRetryErrorClassification::RebootstrapRequired
            ) =>
        {
            return Err(conflict);
        }
        Err(error) => return Err(error),
    };
    let Some(committed) = page.changes.into_iter().find(|change| {
        change.committed_seq == receipt.committed_seq && &change.commit_id == attempt.commit_id
    }) else {
        return Err(conflict);
    };
    let Some(content_ref) = sole_committed_content_ref(&committed) else {
        return Err(conflict);
    };
    let retried = put_retry_fingerprint(
        attempt.namespace_id,
        attempt.path,
        attempt.options,
        content_ref,
    );
    if retried.ok().as_ref().map(CommitFingerprint::as_str)
        != Some(receipt.committed_fingerprint.as_str())
        || !content_ref.matches_evidence(attempt.staged)
    {
        return Err(conflict);
    }
    Ok(crate::v0::CommitResponse::from_committed_change(
        attempt.namespace_id.clone(),
        committed,
    ))
}

/// Returns the content reference when a committed change wrote exactly one
/// file.
fn sole_committed_content_ref(change: &crate::v0::CommittedChange) -> Option<&ContentRef> {
    let mut content = change.events.iter().filter_map(|event| match event {
        crate::v0::FilesystemChange::FileCreated { content_ref, .. }
        | crate::v0::FilesystemChange::ContentChanged { content_ref, .. } => Some(content_ref),
        _ => None,
    });
    let only = content.next()?;
    content.next().is_none().then_some(only)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActorId, AttributeKey, AttributeValue, Checksum, ContentId, ContentRefKind, DisplayName,
    };

    #[test]
    fn canonical_bytes_and_digests_match_shared_vectors() {
        #[derive(serde::Deserialize)]
        struct Vector {
            name: String,
            operation: FilesystemOperation,
            canonical_json: String,
            fingerprint: String,
        }
        let vectors: Vec<Vector> =
            serde_json::from_str(include_str!("../tests/golden/commit_fingerprints_v2.json"))
                .expect("fingerprint vectors");
        for vector in vectors {
            let namespace = NamespaceId::parse("demo").expect("namespace");
            let operations = [vector.operation];
            let bytes = canonical_commit_bytes(&namespace, &test_actor(), None, &operations)
                .expect("canonical bytes");
            assert_eq!(
                bytes,
                vector.canonical_json.as_bytes(),
                "{} canonical bytes",
                vector.name
            );
            let fingerprint =
                semantic_commit_fingerprint(&namespace, &test_actor(), None, &operations)
                    .expect("fingerprint");
            assert_eq!(
                fingerprint.as_str(),
                vector.fingerprint,
                "{} digest",
                vector.name
            );
        }
    }

    fn test_actor() -> ActorRef {
        ActorRef::user(ActorId::parse("test-actor").expect("valid test actor id"))
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
            &[operation],
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
            semantic_commit_fingerprint(&namespace_id, &test_actor(), None, &[forward])
                .expect("forward"),
            semantic_commit_fingerprint(&namespace_id, &test_actor(), None, &[reversed])
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
            &[update_attributes([], ["a", "b"], None, None)],
        )
        .expect("baseline");

        for spelling in [vec!["b", "a"], vec!["a", "b", "a"]] {
            assert_eq!(
                semantic_commit_fingerprint(
                    &namespace_id,
                    &test_actor(),
                    None,
                    &[update_attributes([], spelling, None, None)]
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
            &[update_attributes(
                [("owner", text("ada"))],
                ["draft"],
                None,
                None,
            )],
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
                semantic_commit_fingerprint(&namespace_id, &test_actor(), None, &[variant])
                    .expect("variant fingerprint"),
                "a changed {label} must change the fingerprint"
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
            to_parent_inode_id: InodeId(7),
            to_display_name: DisplayName::parse("report.txt").expect("display name"),
            guard: crate::DestinationGuard {
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
                &[operation(generation)],
            )
            .expect("fingerprint")
        };

        assert_ne!(fingerprint("aaaa"), fingerprint("bbbb"));
    }

    #[test]
    fn actor_kind_and_id_are_distinct_canonical_identity_fields() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let operation = create_dir("/docs");
        let user_x = ActorRef::user(ActorId::parse("x").expect("actor id"));
        let user_y = ActorRef::user(ActorId::parse("y").expect("actor id"));
        let service_x = ActorRef::service(ActorId::parse("x").expect("actor id"));

        let fingerprint = |actor: &ActorRef| {
            semantic_commit_fingerprint(
                &namespace_id,
                actor,
                None,
                std::slice::from_ref(&operation),
            )
            .expect("fingerprint")
        };
        assert_ne!(fingerprint(&user_x), fingerprint(&user_y));
        assert_ne!(fingerprint(&user_x), fingerprint(&service_x));
    }

    #[test]
    fn put_file_guards_change_the_fingerprint_deterministically() {
        let content_ref = ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"guarded put bytes",
        );
        let operation = |expected_inode_id, expected_revision_no| FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/report.txt").expect("path"),
            content_ref: content_ref.clone(),
            behavior: DestinationBehavior::Replace,
            expected_inode_id,
            expected_revision_no,
        };

        let unguarded = fingerprint(operation(None, None));
        let inode_only = fingerprint(operation(Some(InodeId(7)), None));
        let first_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))));
        let next_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(4))));

        assert_ne!(unguarded, inode_only);
        assert_ne!(inode_only, first_revision);
        assert_ne!(first_revision, next_revision);
        assert_eq!(
            first_revision,
            fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))))
        );
    }

    fn assert_destination_guards_change_fingerprint(
        operation: impl Fn(Option<InodeId>, Option<RevisionNo>) -> FilesystemOperation,
    ) {
        let unguarded = fingerprint(operation(None, None));
        let first_inode = fingerprint(operation(Some(InodeId(7)), None));
        let other_inode = fingerprint(operation(Some(InodeId(8)), None));
        let first_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))));
        let other_revision = fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(4))));

        assert_ne!(unguarded, first_inode);
        assert_ne!(first_inode, other_inode);
        assert_ne!(first_inode, first_revision);
        assert_ne!(first_revision, other_revision);
        assert_eq!(
            first_revision,
            fingerprint(operation(Some(InodeId(7)), Some(RevisionNo(3))))
        );
    }

    #[test]
    fn move_and_copy_destination_guards_change_the_fingerprint_deterministically() {
        assert_destination_guards_change_fingerprint(|inode_id, revision_no| {
            FilesystemOperation::MovePath {
                from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
                to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
                guard: crate::DestinationGuard {
                    behavior: DestinationBehavior::Replace,
                    expected_inode_id: inode_id,
                    expected_revision_no: revision_no,
                },
            }
        });
        assert_destination_guards_change_fingerprint(|inode_id, revision_no| {
            FilesystemOperation::CopyPath {
                from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
                to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
                guard: crate::DestinationGuard {
                    behavior: DestinationBehavior::Replace,
                    expected_inode_id: inode_id,
                    expected_revision_no: revision_no,
                },
            }
        });
    }

    #[test]
    fn a_put_retry_reaches_the_pinned_fingerprint_under_every_checksum_algorithm() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let options = PutFileOptions::new(test_actor());
        let content_id =
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id");
        let bytes = b"pinned put bytes";

        for content_ref in [
            ContentRef::blob_v1(content_id.clone(), bytes),
            ContentRef {
                kind: ContentRefKind::BlobV1,
                content_id: content_id.clone(),
                size_bytes: bytes.len() as u64,
                checksum: Checksum::crc32c(bytes),
            },
            ContentRef {
                kind: ContentRefKind::BlobV1,
                content_id: content_id.clone(),
                size_bytes: bytes.len() as u64,
                checksum: Checksum::crc64nvme(bytes),
            },
        ] {
            assert_eq!(
                put_retry_fingerprint(
                    &namespace_id,
                    &AbsolutePath::parse("/docs/report.txt").expect("path"),
                    &options,
                    &content_ref,
                )
                .expect("retry fingerprint")
                .as_str(),
                "v2:sha256:f83a2787fca6165732d4c92faef300ed2f1527ac2804ecb0b4d2ccf6b0a6da83"
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
            content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        }
    }

    #[test]
    fn a_different_content_object_changes_mutation_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let bytes = b"identical bytes, two uploads";
        let first = ContentRef::blob_v1(ContentId::generate(), bytes);
        let second = ContentRef::blob_v1(ContentId::generate(), bytes);

        assert_ne!(
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                &[put("/docs/report.txt", first)]
            )
            .expect("fingerprint"),
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                &[put("/docs/report.txt", second)]
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
        let without =
            semantic_commit_fingerprint(&namespace_id, &test_actor(), None, &[create_dir("/docs")])
                .expect("fingerprint");
        let with = semantic_commit_fingerprint(
            &namespace_id,
            &test_actor(),
            Some("import batch"),
            &[create_dir("/docs")],
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
                &[create_dir("/a"), create_dir("/b")]
            )
            .expect("forward fingerprint"),
            semantic_commit_fingerprint(
                &namespace_id,
                &test_actor(),
                None,
                &[create_dir("/b"), create_dir("/a")]
            )
            .expect("reversed fingerprint")
        );
    }

    #[test]
    fn put_retry_fingerprint_matches_the_equivalent_single_operation_request() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path = AbsolutePath::parse("/docs/report.txt").expect("path");
        let mut options = PutFileOptions::new(test_actor());
        options.behavior = DestinationBehavior::Replace;
        options.expected_inode_id = Some(InodeId(42));
        options.expected_revision_no = Some(RevisionNo(4));
        options.commit.message = Some("import batch".to_owned());
        let content_ref = ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"pinned put bytes",
        );

        let by_hand = semantic_commit_fingerprint(
            &namespace_id,
            &test_actor(),
            Some("import batch"),
            &[FilesystemOperation::PutFile {
                path: path.clone(),
                content_ref: content_ref.clone(),
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(InodeId(42)),
                expected_revision_no: Some(RevisionNo(4)),
            }],
        )
        .expect("hand-built fingerprint");

        assert_eq!(
            put_retry_fingerprint(&namespace_id, &path, &options, &content_ref,)
                .expect("retry fingerprint"),
            by_hand
        );
    }

    #[test]
    fn put_retry_fingerprint_changes_with_every_request_field() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path = AbsolutePath::parse("/a.txt").expect("path");
        let content_ref = ContentRef::blob_v1(ContentId::generate(), b"hello");
        let mut options = PutFileOptions::new(test_actor());
        options.behavior = DestinationBehavior::Replace;
        let fingerprint = |namespace_id, path, options| {
            put_retry_fingerprint(namespace_id, path, options, &content_ref)
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

    #[test]
    fn put_retry_reconciliation_agrees_on_receipt_mismatch_and_unavailable_evidence() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        enum ReconciliationError {
            Conflict(PutRetryReceipt),
            EvidenceUnavailable,
        }

        fn classify(error: &ReconciliationError) -> PutRetryErrorClassification {
            match error {
                ReconciliationError::Conflict(receipt) => {
                    PutRetryErrorClassification::CommitIdReuseConflict(Some(receipt.clone()))
                }
                ReconciliationError::EvidenceUnavailable => {
                    PutRetryErrorClassification::RebootstrapRequired
                }
            }
        }

        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path = AbsolutePath::parse("/report.txt").expect("valid path");
        let commit_id = CommitId::parse("pinned-put").expect("valid commit id");
        let committed_seq = ChangeSeq(7);
        let bytes = b"stable bytes";
        let content_ref = ContentRef::blob_v1(ContentId::generate(), bytes);
        let mut options = crate::options::PutFileOptions::new(test_actor());
        options.commit.commit_id = Some(commit_id.clone());
        let receipt = PutRetryReceipt {
            committed_seq,
            committed_fingerprint: put_retry_fingerprint(
                &namespace_id,
                &path,
                &options,
                &content_ref,
            )
            .expect("fingerprint")
            .as_str()
            .to_owned(),
        };
        let page = crate::v0::ListChangesResponse {
            namespace_id: namespace_id.clone(),
            after_seq: ChangeSeq(6),
            through_seq: committed_seq,
            next_after_seq: None,
            changes: vec![crate::v0::CommittedChange {
                committed_seq,
                commit_id: commit_id.clone(),
                committed_by: test_actor(),
                committed_at_ms: 1,
                message: None,
                events: vec![crate::v0::FilesystemChange::FileCreated {
                    inode_id: InodeId(2),
                    parent_inode_id: InodeId(1),
                    display_name: DisplayName::parse("report.txt").expect("valid display name"),
                    binding_generation: crate::BindingGeneration::parse("abcdef")
                        .expect("binding generation"),
                    revision_no: RevisionNo(1),
                    content_ref,
                }],
            }],
        };

        let matching_attempt = PutRetryAttempt {
            namespace_id: &namespace_id,
            path: &path,
            commit_id: &commit_id,
            options: &options,
            staged: ContentEvidence::Bytes(bytes),
        };
        let reconciled = futures::executor::block_on(reconcile_put_commit_id_reuse(
            matching_attempt,
            ReconciliationError::Conflict(receipt.clone()),
            |after_seq| {
                assert_eq!(after_seq, ChangeSeq(6));
                std::future::ready(Ok(page.clone()))
            },
            classify,
        ))
        .expect("matching receipt and evidence reconcile");
        assert_eq!(reconciled.commit_id, commit_id);
        assert_eq!(reconciled.committed_seq, committed_seq);

        let mismatch = futures::executor::block_on(reconcile_put_commit_id_reuse(
            PutRetryAttempt {
                staged: ContentEvidence::Bytes(b"different bytes"),
                ..matching_attempt
            },
            ReconciliationError::Conflict(receipt.clone()),
            |_| std::future::ready(Ok(page.clone())),
            classify,
        ));
        assert_eq!(
            mismatch,
            Err(ReconciliationError::Conflict(receipt.clone()))
        );

        let unavailable = futures::executor::block_on(reconcile_put_commit_id_reuse(
            matching_attempt,
            ReconciliationError::Conflict(receipt.clone()),
            |_| std::future::ready(Err(ReconciliationError::EvidenceUnavailable)),
            classify,
        ));
        assert_eq!(unavailable, Err(ReconciliationError::Conflict(receipt)));
    }
}
