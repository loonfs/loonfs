//! Commit identity fingerprints (format spec, "Commit identity
//! fingerprints"): a stable digest over a mutation's semantic content, used
//! to decide whether a reused commit id carries the same mutation or a
//! conflicting one.
//!
//! This lives beside [`FilesystemOperation`], the one operation language it
//! hashes, because every surface has to compute the same value from it: the
//! engine stamps it on a commit receipt, and a client reconciling a
//! reused-id conflict recomputes it to prove the retry is the same request.
//! One function is the authority; nobody re-derives the rules.
//!
//! The commit id is not in the preimage. The id is the key a mutation is
//! filed under; the fingerprint is what was filed. Comparing the two is the
//! whole of the reuse check.

use crate::{
    AbsolutePath, ChangeSeq, ContentRef, DeleteDirectoryBehavior, DestinationBehavior,
    FilesystemOperation, InodeId, NamespaceId, RevisionNo,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use thiserror::Error;

/// Domain separator for the one mutation fingerprint preimage.
const COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.commit.semantic.v0";

/// Scheme-and-algorithm tag carried by every stored fingerprint value.
///
/// `v0` names the canonicalization rules (domain string plus the frozen v0
/// preimage encoding; format spec, "Commit identity fingerprints") and
/// `sha256` the digest algorithm, so either can change later without
/// re-interpreting values already stored in WAL records and commit receipts.
const FINGERPRINT_SCHEME: &str = "v0:sha256";

/// A canonical preimage that could not be encoded.
///
/// The preimage is built here from validated types with no encoding failure
/// modes, so this reports a bug in the encoder rather than anything a caller
/// did.
#[derive(Debug, Error)]
#[error("failed to encode the commit fingerprint preimage: {0}")]
pub struct SemanticFingerprintError(#[from] serde_json::Error);

/// Computes a stored fingerprint value (`v0:sha256:<64 lowercase hex>`) from
/// a canonical preimage.
///
/// The preimage's compact JSON encoding is the durable contract: the
/// pinned-value tests below fail if it drifts.
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
    }
}

/// The semantic identity of one mutation request: what a reused commit id is
/// compared against.
///
/// A one-operation convenience call and a one-element batch are the same
/// request, so they reach this function with the same shape and fingerprint
/// identically; there is no separate single-operation form to keep in step.
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

/// The fingerprint the original request must have had, if this retry is the
/// same single put with only the content id renamed.
///
/// Rerunning a whole upload-then-commit sequence stages a fresh content
/// object, so the retry's own fingerprint can never match a landed one. This
/// substitutes the committed reference for the staged one and fingerprints
/// the request that results: everything else a put can ask for — the path,
/// the replacement behavior, the expected revision, the annotation, and that
/// the commit was this one operation and nothing more — has to agree for the
/// value to match. Whether the two content objects hold the same bytes is a
/// separate question, answered by digest evidence rather than by this.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentId;

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
}
