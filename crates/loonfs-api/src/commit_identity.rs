//! Generates stable fingerprints for filesystem mutations (format spec,
//! "Commit identity fingerprints"). A fingerprint lets LoonFS determine
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
    AbsolutePath, ActorKind, ActorRef, AttributeRevisionNo, ChangeSeq, ContentRef,
    DeleteDirectoryBehavior, DestinationBehavior, FilesystemOperation, InodeId, NamespaceId,
    RevisionNo,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::PutFileOptions;
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
    fn a_put_has_the_pinned_fingerprint_under_every_checksum_algorithm() {
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
                semantic_commit_fingerprint(
                    &namespace_id,
                    &test_actor(),
                    None,
                    &[put("/docs/report.txt", content_ref)],
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
    fn put_fingerprint_changes_with_every_request_field() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let path = AbsolutePath::parse("/a.txt").expect("path");
        let content_ref = ContentRef::blob_v1(ContentId::generate(), b"hello");
        let mut options = PutFileOptions::new(test_actor());
        options.behavior = DestinationBehavior::Replace;
        let fingerprint =
            |namespace_id: &NamespaceId, path: &AbsolutePath, options: &PutFileOptions| {
                semantic_commit_fingerprint(
                    namespace_id,
                    &options.commit.actor,
                    options.commit.message.as_deref(),
                    &[FilesystemOperation::PutFile {
                        path: path.clone(),
                        content_ref: content_ref.clone(),
                        behavior: options.behavior,
                        expected_inode_id: options.expected_inode_id,
                        expected_revision_no: options.expected_revision_no,
                    }],
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
