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

use crate::{
    AbsolutePath, AttributeRevisionNo, ChangeSeq, CommitId, ContentEvidence, ContentRef,
    DeleteDirectoryBehavior, DestinationBehavior, FilesystemOperation, InodeId, NamespaceId,
    RevisionNo,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::future::Future;
use thiserror::Error;

/// Domain separator included in every mutation fingerprint input.
const COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.commit.semantic.v0";

/// Scheme and hash algorithm included in every stored fingerprint.
///
/// `v0` identifies the canonical encoding rules, and `sha256` identifies the
/// hash algorithm. Including both values allows future versions to change
/// either rule without changing the meaning of existing fingerprints.
const FINGERPRINT_SCHEME: &str = "v0:sha256";

/// Error returned when the canonical fingerprint input cannot be encoded.
///
/// The input contains validated types, so this error indicates an internal
/// encoding bug rather than invalid caller data.
#[derive(Debug, Error)]
#[error("failed to encode the commit fingerprint preimage: {0}")]
pub struct SemanticFingerprintError(#[from] serde_json::Error);

/// Computes a stored fingerprint (`v0:sha256:<64 lowercase hex>`) from a
/// canonical input.
///
/// The compact JSON encoding is part of the durable format. The fixed-value
/// tests detect changes to that encoding.
fn fingerprint_digest<T>(preimage: &T) -> Result<String, SemanticFingerprintError>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(preimage)?;
    Ok(fingerprint_bytes(&bytes))
}

fn fingerprint_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(FINGERPRINT_SCHEME.len() + 1 + digest.len() * 2);
    value.push_str(FINGERPRINT_SCHEME);
    value.push(':');
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to a String should not fail");
    }
    value
}

/// Canonical preimage for one operation inside a mutation fingerprint.
///
/// The serde representation is durable contract (format spec, "Commit
/// identity fingerprints"): the same normalized request must fingerprint
/// identically across releases. A pinned-value test below fails if the
/// encoding drifts.
///
/// The variant names, the field names, and the field order below are all part
/// of that preimage under the [`COMMIT_FINGERPRINT_DOMAIN`] tag, and none of
/// them tracks the wire enum. They deliberately differ from it — `CreateDir`
/// against the wire's `CreateDirectory`, `absolute_path` against its `path`,
/// `behavior` ahead of `content_ref` in the put — because renaming a wire
/// field must not silently restate every already-published commit's identity.
/// [`operation_fingerprint_input`] is the one place the wire spelling is
/// translated into this one; nothing else may name these variants. Change any
/// of it and every stored fingerprint disagrees with its recomputed value,
/// which the pinned tests below exist to catch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OperationFingerprintInput<'a> {
    CreateDir {
        absolute_path: &'a str,
        parents: bool,
    },
    // The put guard joins the preimage for the same reason as the delete
    // guard below: a changed expected revision is a different logical
    // request and must conflict rather than replay a receipt.
    PutFile {
        absolute_path: &'a str,
        behavior: DestinationBehavior,
        content_ref: ContentRefFingerprintInput<'a>,
        expected_revision_no: Option<RevisionNo>,
    },
    // Identity covers the complete caller-visible logical request. A changed
    // delete guard must conflict instead of replaying the old receipt
    // without checking the new guard.
    DeletePath {
        absolute_path: &'a str,
        behavior: DeleteDirectoryBehavior,
        expected_inode_id: Option<InodeId>,
    },
    MovePath {
        from_path: &'a str,
        to_path: &'a str,
        behavior: DestinationBehavior,
    },
    CopyFilePath {
        from_path: &'a str,
        to_path: &'a str,
        behavior: DestinationBehavior,
    },
    RestoreRevision {
        absolute_path: &'a str,
        source_revision_no: RevisionNo,
    },
    Undelete {
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        // Preimage-additive: `Some` serializes as the bare string it always
        // was, so every stored undelete fingerprint is unchanged; `None`
        // serializes as `null`, a new distinct preimage for the in-place
        // form. Both shapes are pinned below.
        absolute_path: Option<&'a str>,
    },
    // Both guards join the preimage for the same reason the delete guard
    // does: a changed expectation is a different logical request. `set` is a
    // map, so it serializes key-ordered whatever order the caller sent; the
    // translation below sorts and deduplicates `remove` so two spellings of
    // one removal set reach the same preimage.
    UpdateAttrs {
        absolute_path: &'a str,
        set: BTreeMap<&'a str, &'a str>,
        remove: Vec<&'a str>,
        expected_inode_id: Option<InodeId>,
        expected_attributes_revision_no: Option<AttributeRevisionNo>,
    },
}

/// Canonical preimage for the content a put attaches.
///
/// Identity is *which object*, so the id and its length are the whole of it.
/// The checksums are evidence about those bytes, pinned to the id by the
/// verification every write and read already performs, and they are left out
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

/// Renames one wire operation into its durable preimage.
///
/// This is the whole of the wire-to-fingerprint translation. The left side
/// follows [`FilesystemOperation`] and may be renamed with it; the right side
/// is frozen (see [`OperationFingerprintInput`]).
fn operation_fingerprint_input(operation: &FilesystemOperation) -> OperationFingerprintInput<'_> {
    match operation {
        FilesystemOperation::CreateDirectory { path, parents } => {
            OperationFingerprintInput::CreateDir {
                absolute_path: path.as_str(),
                parents: *parents,
            }
        }
        FilesystemOperation::PutFile {
            path,
            content_ref,
            behavior,
            expected_revision_no,
        } => OperationFingerprintInput::PutFile {
            absolute_path: path.as_str(),
            behavior: *behavior,
            content_ref: content_ref_fingerprint_input(content_ref),
            expected_revision_no: *expected_revision_no,
        },
        FilesystemOperation::DeletePath {
            path,
            behavior,
            expected_inode_id,
        } => OperationFingerprintInput::DeletePath {
            absolute_path: path.as_str(),
            behavior: *behavior,
            expected_inode_id: *expected_inode_id,
        },
        FilesystemOperation::MovePath {
            from_path,
            to_path,
            behavior,
        } => OperationFingerprintInput::MovePath {
            from_path: from_path.as_str(),
            to_path: to_path.as_str(),
            behavior: *behavior,
        },
        FilesystemOperation::CopyPath {
            from_path,
            to_path,
            behavior,
        } => OperationFingerprintInput::CopyFilePath {
            from_path: from_path.as_str(),
            to_path: to_path.as_str(),
            behavior: *behavior,
        },
        FilesystemOperation::RestoreRevision {
            path,
            source_revision_no,
        } => OperationFingerprintInput::RestoreRevision {
            absolute_path: path.as_str(),
            source_revision_no: *source_revision_no,
        },
        FilesystemOperation::Undelete {
            inode_id,
            deleted_at_seq,
            path,
        } => OperationFingerprintInput::Undelete {
            inode_id: *inode_id,
            deleted_at_seq: *deleted_at_seq,
            absolute_path: path.as_ref().map(|path| path.as_str()),
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
            OperationFingerprintInput::UpdateAttrs {
                absolute_path: path.as_str(),
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
    message: Option<&str>,
    operations: &[FilesystemOperation],
) -> Result<String, SemanticFingerprintError> {
    #[derive(Serialize)]
    struct CanonicalCommit<'a> {
        domain: &'static str,
        namespace_id: &'a str,
        operations: Vec<OperationFingerprintInput<'a>>,
        message: Option<&'a str>,
    }

    fingerprint_digest(&CanonicalCommit {
        domain: COMMIT_FINGERPRINT_DOMAIN,
        namespace_id: namespace_id.as_str(),
        operations: operations.iter().map(operation_fingerprint_input).collect(),
        message,
    })
}

/// Computes the fingerprint for a retried single-file PUT using the content
/// reference from the original commit.
///
/// Retrying an upload creates a new content object, so its content ID differs
/// from the ID stored by the original commit. This function substitutes the
/// original content reference before computing the fingerprint. The path,
/// destination behavior, expected revision, message, and operation count must
/// still match. The caller must separately verify that both content objects
/// contain the same bytes.
pub fn put_retry_fingerprint(
    namespace_id: &NamespaceId,
    path: &AbsolutePath,
    behavior: DestinationBehavior,
    expected_revision_no: Option<RevisionNo>,
    message: Option<&str>,
    committed_content_ref: &ContentRef,
) -> Result<String, SemanticFingerprintError> {
    let operation = FilesystemOperation::PutFile {
        path: path.clone(),
        content_ref: committed_content_ref.clone(),
        behavior,
        expected_revision_no,
    };
    semantic_commit_fingerprint(namespace_id, message, std::slice::from_ref(&operation))
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
    ReadChangeFuture: Future<Output = Result<crate::v0::ChangesResponse, E>>,
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
        change.seq == receipt.committed_seq && &change.commit_id == attempt.commit_id
    }) else {
        return Err(conflict);
    };
    let Some(content_ref) = sole_committed_content_ref(&committed) else {
        return Err(conflict);
    };
    let retried = put_retry_fingerprint(
        attempt.namespace_id,
        attempt.path,
        attempt.options.behavior,
        attempt.options.expected_revision_no,
        attempt.options.message.as_deref(),
        content_ref,
    );
    if retried.ok().as_deref() != Some(receipt.committed_fingerprint.as_str())
        || !content_ref.matches_evidence(attempt.staged)
    {
        return Err(conflict);
    }
    Ok(crate::v0::CommitResponse {
        namespace_id: attempt.namespace_id.clone(),
        commit_id: committed.commit_id,
        committed_seq: committed.seq,
    })
}

/// Returns the content reference when a committed change wrote exactly one
/// file.
fn sole_committed_content_ref(change: &crate::v0::CommittedChange) -> Option<&ContentRef> {
    let mut content = change.events.iter().filter_map(|event| match event {
        crate::v0::FilesystemChange::Created { content_ref, .. } => content_ref.as_ref(),
        crate::v0::FilesystemChange::ContentChanged { content_ref, .. } => Some(content_ref),
        _ => None,
    });
    let only = content.next()?;
    content.next().is_none().then_some(only)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AttributeKey, AttributeValue, ContentId, ContentRefKind, StorageChecksum};

    fn attribute_key(value: &str) -> AttributeKey {
        AttributeKey::parse(value).expect("valid attribute key")
    }

    fn text(value: &str) -> AttributeValue {
        AttributeValue::parse(value).expect("valid attribute value")
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

    /// Pins the exact stored fingerprint for a guarded attribute update.
    ///
    /// The literal covers the frozen preimage: the variant name, the field
    /// order, the canonical attribute-value spelling, and both guards.
    #[test]
    fn update_attributes_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let fingerprint = semantic_commit_fingerprint(
            &namespace_id,
            None,
            &[update_attributes(
                [("owner", text("ada")), ("tags", text("a,b"))],
                ["draft"],
                Some(InodeId(42)),
                Some(AttributeRevisionNo(3)),
            )],
        )
        .expect("fingerprint");

        assert_eq!(
            fingerprint,
            "v0:sha256:e6c4c43ab20756a664d004adf22f223aef6467e45f737fb3d89fc1b6e211e5a2"
        );
    }

    /// The set is a map, so the order the caller wrote its keys in is not
    /// part of what was asked for.
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
            semantic_commit_fingerprint(&namespace_id, None, &[forward]).expect("forward"),
            semantic_commit_fingerprint(&namespace_id, None, &[reversed]).expect("reversed")
        );
    }

    /// Removing two keys asks for the same thing whichever order they are
    /// listed in, and asking twice for one removal asks for the same thing
    /// as asking once. Canonicalization inside the translation is what makes
    /// both true.
    #[test]
    fn remove_order_and_repeats_do_not_change_attribute_update_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let baseline = semantic_commit_fingerprint(
            &namespace_id,
            None,
            &[update_attributes([], ["a", "b"], None, None)],
        )
        .expect("baseline");

        for spelling in [vec!["b", "a"], vec!["a", "b", "a"]] {
            assert_eq!(
                semantic_commit_fingerprint(
                    &namespace_id,
                    None,
                    &[update_attributes([], spelling, None, None)]
                )
                .expect("variant"),
                baseline
            );
        }
    }

    /// Everything the update asks for is inside the value.
    #[test]
    fn attribute_update_fingerprint_changes_with_every_request_field() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let baseline = semantic_commit_fingerprint(
            &namespace_id,
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
                semantic_commit_fingerprint(&namespace_id, None, &[variant])
                    .expect("variant fingerprint"),
                "a changed {label} must change the fingerprint"
            );
        }
    }

    /// Pins the exact stored fingerprint for a fixed one-operation request.
    ///
    /// If this fails, the canonical preimage changed (format spec, "Commit
    /// identity fingerprints") and every persisted fingerprint would disagree
    /// with recomputed ones, breaking retry idempotency across versions. Do
    /// not update the literal without bumping the fingerprint scheme tag.
    #[test]
    fn commit_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let fingerprint = semantic_commit_fingerprint(&namespace_id, None, &[create_dir("/docs")])
            .expect("fingerprint");

        assert_eq!(
            fingerprint,
            "v0:sha256:85894f53a16c2c0be95afc39b245280101f3e2a414f044c87be8eb9f1980dbcd"
        );
    }

    /// Pins the exact stored fingerprint encoding for a guarded delete.
    #[test]
    fn guarded_delete_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let fingerprint = semantic_commit_fingerprint(
            &namespace_id,
            None,
            &[FilesystemOperation::DeletePath {
                path: AbsolutePath::parse("/docs").expect("path"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: Some(InodeId(42)),
            }],
        )
        .expect("fingerprint");

        assert_eq!(
            fingerprint,
            "v0:sha256:edc8e06bd0a651e9470198875ec44c8fcd7d9b95f162fe1d7ca46011c27e2818"
        );
    }

    /// Pins the exact stored fingerprint for an undelete with a destination
    /// path.
    ///
    /// This literal is what proves the in-place form was preimage-additive:
    /// the path became optional and this value did not move, because a
    /// present path serializes as the bare string it always was.
    #[test]
    fn undelete_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let fingerprint = semantic_commit_fingerprint(
            &namespace_id,
            None,
            &[FilesystemOperation::Undelete {
                inode_id: InodeId(42),
                deleted_at_seq: ChangeSeq(17),
                path: Some(AbsolutePath::parse("/docs/report.txt").expect("path")),
            }],
        )
        .expect("fingerprint");

        // The mechanism behind "did not move": a present option serializes
        // as the bare value, so wrapping the preimage field changed no
        // stored byte.
        assert_eq!(
            serde_json::to_value(Some("/docs/report.txt")).expect("serialize"),
            serde_json::to_value("/docs/report.txt").expect("serialize"),
        );
        assert_eq!(
            fingerprint,
            "v0:sha256:1f4fa76d65aa64903a7d44cead91600a97c0bac9ec3a01ac51f0cd1130eff3d6"
        );
    }

    /// Pins the exact stored fingerprint for an in-place undelete, whose
    /// absent path serializes as `null` — a distinct preimage from every
    /// pathed form.
    #[test]
    fn in_place_undelete_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let fingerprint = semantic_commit_fingerprint(
            &namespace_id,
            None,
            &[FilesystemOperation::Undelete {
                inode_id: InodeId(42),
                deleted_at_seq: ChangeSeq(17),
                path: None,
            }],
        )
        .expect("fingerprint");

        assert_eq!(
            fingerprint,
            "v0:sha256:4d7737cdc3888e3613dad0ec7d752e8daac089c8b528301cf0eba9307fa1cc4c"
        );
    }

    /// Pins the exact stored fingerprint for a put, which is the only
    /// operation whose preimage embeds a content reference.
    ///
    /// The literal covers the canonical content-ref form — kind, content id,
    /// size, and nothing else. Adding a checksum to that form, or reordering
    /// it, would change this value and silently break replay for every
    /// already-published put.
    #[test]
    fn put_file_fingerprint_value_is_pinned() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let fingerprint = semantic_commit_fingerprint(
            &namespace_id,
            None,
            &[FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/report.txt").expect("path"),
                content_ref: ContentRef::blob_v1(
                    ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
                    b"pinned put bytes",
                ),
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            }],
        )
        .expect("fingerprint");

        assert_eq!(
            fingerprint,
            "v0:sha256:3febc279ebb36c013f734095bebdba3c0a59bf8cbd82d205b53adbf00c112d59"
        );
    }

    /// The retry leg, over the pinned value above: whatever algorithm the
    /// original commit's reference landed with, a retry reading it back
    /// recomputes the same fingerprint.
    ///
    /// This is what lets a retry prove sameness in two independent steps —
    /// the fingerprint says the two requests are the same mutation, and the
    /// digest evidence says the two payloads are the same bytes. A checksum
    /// inside the preimage would collapse them into one weaker check and
    /// make a CRC-only commit unreplayable.
    #[test]
    fn a_put_retry_reaches_the_pinned_fingerprint_under_every_checksum_algorithm() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let content_id =
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id");
        let bytes = b"pinned put bytes";

        for content_ref in [
            ContentRef::blob_v1(content_id.clone(), bytes),
            ContentRef {
                kind: ContentRefKind::BlobV1,
                content_id: content_id.clone(),
                size_bytes: bytes.len() as u64,
                storage_checksum: StorageChecksum::crc32c(bytes),
                whole_file_sha256: None,
            },
            ContentRef {
                kind: ContentRefKind::BlobV1,
                content_id: content_id.clone(),
                size_bytes: bytes.len() as u64,
                storage_checksum: StorageChecksum::crc64nvme(bytes),
                whole_file_sha256: None,
            },
        ] {
            assert_eq!(
                put_retry_fingerprint(
                    &namespace_id,
                    &AbsolutePath::parse("/docs/report.txt").expect("path"),
                    DestinationBehavior::NoReplace,
                    None,
                    None,
                    &content_ref,
                )
                .expect("retry fingerprint"),
                "v0:sha256:3febc279ebb36c013f734095bebdba3c0a59bf8cbd82d205b53adbf00c112d59"
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
            expected_revision_no: None,
        }
    }

    /// Two references to the same object with different checksum evidence
    /// are the same mutation: identity is which object a put attaches, and
    /// the checksums are pinned to that object by verification elsewhere.
    #[test]
    fn checksum_evidence_is_outside_mutation_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let content_ref = ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"pinned put bytes",
        );
        let without_trusted_digest = ContentRef {
            whole_file_sha256: None,
            ..content_ref.clone()
        };

        assert_eq!(
            semantic_commit_fingerprint(
                &namespace_id,
                None,
                &[put("/docs/report.txt", content_ref)]
            )
            .expect("fingerprint"),
            semantic_commit_fingerprint(
                &namespace_id,
                None,
                &[put("/docs/report.txt", without_trusted_digest)]
            )
            .expect("fingerprint")
        );
    }

    /// A different content object is a different mutation, which is what
    /// makes a re-upload under a used commit id conflict instead of replay.
    #[test]
    fn a_different_content_object_changes_mutation_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let bytes = b"identical bytes, two uploads";
        let first = ContentRef::blob_v1(ContentId::generate(), bytes);
        let second = ContentRef::blob_v1(ContentId::generate(), bytes);

        assert_ne!(
            semantic_commit_fingerprint(&namespace_id, None, &[put("/docs/report.txt", first)])
                .expect("fingerprint"),
            semantic_commit_fingerprint(&namespace_id, None, &[put("/docs/report.txt", second)])
                .expect("fingerprint")
        );
    }

    #[test]
    fn a_message_changes_mutation_identity() {
        // The annotation is part of what the caller asked for: replaying a
        // commit id with a different message must conflict, so the message
        // joins the preimage.
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let without = semantic_commit_fingerprint(&namespace_id, None, &[create_dir("/docs")])
            .expect("fingerprint");
        let with = semantic_commit_fingerprint(
            &namespace_id,
            Some("import batch"),
            &[create_dir("/docs")],
        )
        .expect("fingerprint");

        assert_ne!(without, with);
    }

    #[test]
    fn commit_fingerprint_changes_when_logical_inputs_change() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let baseline = semantic_commit_fingerprint(&namespace_id, None, &[create_dir("/docs")])
            .expect("baseline");
        let changed = semantic_commit_fingerprint(&namespace_id, None, &[create_dir("/drafts")])
            .expect("changed");

        assert_ne!(baseline, changed);
    }

    /// Operation order is part of the request: reordering is a different
    /// logical mutation, so it must not replay the first one's receipt.
    #[test]
    fn operation_order_changes_mutation_identity() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        assert_ne!(
            semantic_commit_fingerprint(&namespace_id, None, &[create_dir("/a"), create_dir("/b")])
                .expect("forward fingerprint"),
            semantic_commit_fingerprint(&namespace_id, None, &[create_dir("/b"), create_dir("/a")])
                .expect("reversed fingerprint")
        );
    }

    /// The retry helper is not a second spelling of the preimage: it builds
    /// the same single-put request a caller would have sent and hands it to
    /// the same function.
    #[test]
    fn put_retry_fingerprint_matches_the_equivalent_single_operation_request() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path = AbsolutePath::parse("/docs/report.txt").expect("path");
        let content_ref = ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"pinned put bytes",
        );

        let by_hand = semantic_commit_fingerprint(
            &namespace_id,
            Some("import batch"),
            &[FilesystemOperation::PutFile {
                path: path.clone(),
                content_ref: content_ref.clone(),
                behavior: DestinationBehavior::Replace,
                expected_revision_no: Some(RevisionNo(4)),
            }],
        )
        .expect("hand-built fingerprint");

        assert_eq!(
            put_retry_fingerprint(
                &namespace_id,
                &path,
                DestinationBehavior::Replace,
                Some(RevisionNo(4)),
                Some("import batch"),
                &content_ref,
            )
            .expect("retry fingerprint"),
            by_hand
        );
    }

    /// Everything a put can ask for beyond its content is inside the value,
    /// which is what makes comparing the whole fingerprint a complete proof
    /// rather than a partial one.
    #[test]
    fn put_retry_fingerprint_changes_with_every_request_field() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path = AbsolutePath::parse("/a.txt").expect("path");
        let content_ref = ContentRef::blob_v1(ContentId::generate(), b"hello");
        let baseline = put_retry_fingerprint(
            &namespace_id,
            &path,
            DestinationBehavior::Replace,
            None,
            None,
            &content_ref,
        )
        .expect("baseline");

        for (label, variant) in [
            (
                "path",
                put_retry_fingerprint(
                    &namespace_id,
                    &AbsolutePath::parse("/b.txt").expect("path"),
                    DestinationBehavior::Replace,
                    None,
                    None,
                    &content_ref,
                ),
            ),
            (
                "behavior",
                put_retry_fingerprint(
                    &namespace_id,
                    &path,
                    DestinationBehavior::NoReplace,
                    None,
                    None,
                    &content_ref,
                ),
            ),
            (
                "expected revision",
                put_retry_fingerprint(
                    &namespace_id,
                    &path,
                    DestinationBehavior::Replace,
                    Some(RevisionNo(2)),
                    None,
                    &content_ref,
                ),
            ),
            (
                "message",
                put_retry_fingerprint(
                    &namespace_id,
                    &path,
                    DestinationBehavior::Replace,
                    None,
                    Some(""),
                    &content_ref,
                ),
            ),
            (
                "namespace",
                put_retry_fingerprint(
                    &NamespaceId::parse("other").expect("valid namespace id"),
                    &path,
                    DestinationBehavior::Replace,
                    None,
                    None,
                    &content_ref,
                ),
            ),
        ] {
            assert_ne!(
                baseline,
                variant.expect("variant fingerprint"),
                "a changed {label} must change the fingerprint"
            );
        }
    }

    /// Tests the shared retry logic without using the HTTP or embedded-runtime
    /// adapters.
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
        let options = crate::options::PutFileOptions {
            commit_id: Some(commit_id.clone()),
            ..crate::options::PutFileOptions::default()
        };
        let receipt = PutRetryReceipt {
            committed_seq,
            committed_fingerprint: put_retry_fingerprint(
                &namespace_id,
                &path,
                options.behavior,
                options.expected_revision_no,
                options.message.as_deref(),
                &content_ref,
            )
            .expect("fingerprint"),
        };
        let page = crate::v0::ChangesResponse {
            namespace_id: namespace_id.clone(),
            after_seq: ChangeSeq(6),
            through_seq: committed_seq,
            next_after_seq: None,
            changes: vec![crate::v0::CommittedChange {
                seq: committed_seq,
                commit_id: commit_id.clone(),
                committed_at_ms: 1,
                message: None,
                events: vec![crate::v0::FilesystemChange::ContentChanged {
                    inode_id: InodeId(2),
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
