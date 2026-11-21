//! Unit tests for canonical commit fingerprint encoding.

use super::*;
use crate::options::PutFileOptions;
use crate::{AbsolutePath, ActorId, AttributeKey, AttributeValue, ContentId, DisplayName};

#[test]
fn canonical_bytes_and_digests_match_shared_vectors() {
    #[derive(Serialize, serde::Deserialize)]
    struct Vector {
        name: String,
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
        let identities = vector
            .inline_content_ids
            .iter()
            .map(|content_id| {
                (
                    content_id.clone(),
                    InlineContentIdentity::from_bytes(b"pinned put bytes"),
                )
            })
            .collect();
        let bytes = CommitFingerprintInput {
            namespace_id: &namespace,
            actor_id: &test_actor(),
            message: None,
            operations: &operations,
            preconditions: &vector.preconditions,
            inline_content_identities: &identities,
        }
        .canonical_bytes();
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
        let fingerprint = CommitFingerprintInput {
            namespace_id: &namespace,
            actor_id: &test_actor(),
            message: None,
            operations: &operations,
            preconditions: &vector.preconditions,
            inline_content_identities: &identities,
        }
        .fingerprint();
        assert_eq!(
            fingerprint.as_str(),
            vector.fingerprint,
            "{} digest",
            vector.name
        );
    }
    if update {
        let json = serde_json::to_string_pretty(&vectors).expect("serialize fingerprint vectors");
        std::fs::write(fixture_path, format!("{json}\n")).expect("write fingerprint vectors");
    }
}

/// Pins the example bytes and digest of Appendix B's merge section, and
/// checks that every member of the merge record changes the digest.
#[test]
fn a_merge_fingerprint_covers_the_merge_record_and_matches_the_spec_example() {
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let actor_id = ActorId::parse("usr_8f3c").expect("actor");
    let merge = MergeRecord {
        source_namespace_id: NamespaceId::parse("demo-fork").expect("namespace"),
        source_after_seq: CommitSeq(418),
        source_through_seq: CommitSeq(425),
        conflicts: Vec::new(),
    };
    let input = MergeFingerprintInput {
        namespace_id: &namespace_id,
        actor_id: &actor_id,
        merge: &merge,
        message: None,
    };
    assert_eq!(
        String::from_utf8(input.canonical_bytes()).expect("canonical UTF-8"),
        r#"{"domain":"loonfs.commit.merge.v1","namespace_id":"demo","actor_id":"usr_8f3c","merge":{"source_namespace_id":"demo-fork","source_after_seq":418,"source_through_seq":425},"message":null}"#
    );
    let fingerprint = input.fingerprint();
    assert_eq!(
        fingerprint.as_str(),
        "v1:sha256:004b16fd5b647a8003f24e3b558b5f93ff5e6bc836ca42322f93ddff1ffaa365"
    );
    let other_source = MergeRecord {
        source_namespace_id: NamespaceId::parse("demo-other").expect("namespace"),
        ..merge.clone()
    };
    let other_after = MergeRecord {
        source_after_seq: CommitSeq(419),
        ..merge.clone()
    };
    let other_through = MergeRecord {
        source_through_seq: CommitSeq(426),
        ..merge.clone()
    };
    for changed in [other_source, other_after, other_through] {
        let changed_input = MergeFingerprintInput {
            merge: &changed,
            ..input
        };
        assert_ne!(changed_input.fingerprint(), fingerprint, "{changed:?}");
    }
}

/// Checks that a replay commit's conflict report isn't part of its
/// identity: a record with conflicts has the fingerprint of the same
/// record without them.
#[test]
fn a_merge_fingerprint_leaves_out_the_conflict_report() {
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let actor_id = ActorId::parse("usr_8f3c").expect("actor");
    let merge = MergeRecord {
        source_namespace_id: NamespaceId::parse("demo-fork").expect("namespace"),
        source_after_seq: CommitSeq(418),
        source_through_seq: CommitSeq(425),
        conflicts: Vec::new(),
    };
    let reported = MergeRecord {
        conflicts: vec![crate::MergeConflict {
            inode_id: crate::ROOT_INODE_ID,
            kind: crate::MergeConflictKind::Content,
            base: None,
            ours: Some(crate::MergeConflictVersion {
                path: AbsolutePath::parse("/a.txt").expect("path"),
                revision: None,
            }),
            theirs: None,
        }],
        ..merge.clone()
    };
    let fingerprint = |merge| {
        MergeFingerprintInput {
            namespace_id: &namespace_id,
            actor_id: &actor_id,
            merge,
            message: None,
        }
        .fingerprint()
    };
    assert_eq!(fingerprint(&reported), fingerprint(&merge));
}

#[test]
fn inline_fingerprints_keep_the_request_byte_digest_for_both_store_algorithms() {
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let fingerprint = |operation, identities: &BTreeMap<ContentId, InlineContentIdentity>| {
        CommitFingerprintInput {
            namespace_id: &namespace_id,
            actor_id: &test_actor(),
            message: None,
            operations: &[operation],
            preconditions: &[],
            inline_content_identities: identities,
        }
        .fingerprint()
    };
    let inline = FilesystemOperation::PutFile {
        path: AbsolutePath::parse("/file").expect("path"),
        content: FileContent::Inline(b"hello".to_vec()),
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    };
    assert_eq!(
        fingerprint(inline, &BTreeMap::new()).as_str(),
        "v1:sha256:6a8f41b4a71d127a3b9b6b9a5175c66df1128522bfdc48ed7b8bc47be794a22c"
    );
    for algorithm in [
        crate::ChecksumAlgorithm::Crc64nvme,
        crate::ChecksumAlgorithm::Crc32c,
    ] {
        let content_id = ContentId::generate();
        let reference = ContentRef::blob_v1(content_id.clone(), b"hello", algorithm);
        let identities =
            BTreeMap::from([(content_id, InlineContentIdentity::from_bytes(b"hello"))]);
        assert_eq!(
            fingerprint(put("/file", reference), &identities).as_str(),
            "v1:sha256:6a8f41b4a71d127a3b9b6b9a5175c66df1128522bfdc48ed7b8bc47be794a22c"
        );
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
    operations_fingerprint(&[operation])
}

fn operations_fingerprint(operations: &[FilesystemOperation]) -> String {
    CommitFingerprintInput {
        namespace_id: &NamespaceId::parse("demo").expect("valid namespace id"),
        actor_id: &test_actor(),
        message: None,
        operations,
        preconditions: &[],
        inline_content_identities: &BTreeMap::new(),
    }
    .fingerprint()
    .as_str()
    .to_owned()
}

fn update_attributes(
    set: impl IntoIterator<Item = (&'static str, AttributeValue)>,
    remove: impl IntoIterator<Item = &'static str>,
    expected_inode_id: Option<InodeId>,
    expected_attributes_revision_no: Option<AttributesRevisionNo>,
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

    assert_eq!(fingerprint(forward), fingerprint(reversed));
}

#[test]
fn remove_order_and_repeats_do_not_change_attribute_update_identity() {
    let baseline = fingerprint(update_attributes([], ["a", "b"], None, None));

    for spelling in [vec!["b", "a"], vec!["a", "b", "a"]] {
        assert_eq!(
            fingerprint(update_attributes([], spelling, None, None)),
            baseline
        );
    }
}

#[test]
fn attribute_update_fingerprint_changes_with_every_request_field() {
    let baseline = fingerprint(update_attributes(
        [("owner", text("ada"))],
        ["draft"],
        None,
        None,
    ));

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
                Some(AttributesRevisionNo(0)),
            ),
        ),
    ] {
        assert_ne!(
            baseline,
            fingerprint(variant),
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
        "expected_inode_id": "ino_00000000000000000000000009",
        "expected_access_revision_no": 2
    });
    let baseline_fingerprint =
        fingerprint(serde_json::from_value(baseline.clone()).expect("operation"));
    for (field, value) in [
        ("path", serde_json::json!("/docs/other")),
        ("boundary", serde_json::json!(false)),
        ("grants", serde_json::json!({"prn_ada": ["read"]})),
        (
            "expected_inode_id",
            serde_json::json!("ino_0000000000000000000000000a"),
        ),
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
fn inode_forms_fingerprint_every_request_field() {
    for (baseline, variants) in [
        (
            serde_json::json!({
                "kind": "copy_by_inode",
                "inode_id": "ino_0000000000000000000000001a",
                "destination_parent_inode_id": "ino_00000000000000000000000007",
                "destination_display_name": "copy.txt",
                "behavior": "replace",
                "expected_destination_inode_id": "ino_00000000000000000000000009",
                "expected_destination_revision_no": 2
            }),
            vec![
                (
                    "inode_id",
                    serde_json::json!("ino_0000000000000000000000001b"),
                ),
                (
                    "destination_parent_inode_id",
                    serde_json::json!("ino_00000000000000000000000008"),
                ),
                ("destination_display_name", serde_json::json!("other.txt")),
                (
                    "expected_destination_inode_id",
                    serde_json::json!("ino_0000000000000000000000000a"),
                ),
                ("expected_destination_revision_no", serde_json::json!(3)),
            ],
        ),
        (
            serde_json::json!({
                "kind": "restore_revision_by_inode",
                "inode_id": "ino_0000000000000000000000001a",
                "source_revision_no": 3
            }),
            vec![
                (
                    "inode_id",
                    serde_json::json!("ino_0000000000000000000000001b"),
                ),
                ("source_revision_no", serde_json::json!(4)),
            ],
        ),
        (
            serde_json::json!({
                "kind": "restore_deleted",
                "inode_id": "ino_0000000000000000000000001a",
                "deletion_seq": 17,
                "destination_parent_inode_id": "ino_00000000000000000000000007",
                "destination_display_name": "report.txt"
            }),
            vec![
                (
                    "destination_parent_inode_id",
                    serde_json::json!("ino_00000000000000000000000008"),
                ),
                ("destination_display_name", serde_json::json!("other.txt")),
            ],
        ),
        (
            serde_json::json!({
                "kind": "restore_deleted",
                "inode_id": "ino_0000000000000000000000001a",
                "deletion_seq": 17,
                "destination_path": "/report.txt"
            }),
            vec![("destination_path", serde_json::json!("/other.txt"))],
        ),
        (
            serde_json::json!({
                "kind": "update_attributes_by_inode",
                "inode_id": "ino_0000000000000000000000001a",
                "set": {"owner": "ada"},
                "remove": ["draft"],
                "expected_attributes_revision_no": 3
            }),
            vec![
                (
                    "inode_id",
                    serde_json::json!("ino_0000000000000000000000001b"),
                ),
                ("set", serde_json::json!({"owner": "grace"})),
                ("remove", serde_json::json!(["final"])),
                ("expected_attributes_revision_no", serde_json::json!(4)),
            ],
        ),
        (
            serde_json::json!({
                "kind": "update_access_by_inode",
                "inode_id": "ino_00000000000000000000000009",
                "boundary": true,
                "grants": {"prn_ada": ["read", "write"]},
                "expected_access_revision_no": 2
            }),
            vec![
                (
                    "inode_id",
                    serde_json::json!("ino_0000000000000000000000000a"),
                ),
                ("boundary", serde_json::json!(false)),
                ("grants", serde_json::json!({"prn_ada": ["read"]})),
                ("expected_access_revision_no", serde_json::json!(3)),
            ],
        ),
    ] {
        let baseline_fingerprint =
            fingerprint(serde_json::from_value(baseline.clone()).expect("operation"));
        for (field, value) in variants {
            let mut variant = baseline.clone();
            variant[field] = value;
            assert_ne!(
                baseline_fingerprint,
                fingerprint(serde_json::from_value(variant).expect("variant")),
                "a changed {field} must change the {} fingerprint",
                baseline["kind"]
            );
        }
    }
}

#[test]
fn binding_version_changes_inode_mutation_identity() {
    let operation = |expected_binding_version: &str| FilesystemOperation::MoveByInode {
        inode_id: InodeId(42),
        expected_binding_version: crate::BindingVersion::parse(expected_binding_version)
            .expect("binding version"),
        destination_parent_inode_id: InodeId(7),
        destination_display_name: DisplayName::parse("report.txt").expect("display name"),
        precondition: crate::DestinationPrecondition {
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    };

    assert_ne!(
        fingerprint(operation("aaaa")),
        fingerprint(operation("bbbb"))
    );
}

#[test]
fn changed_actor_id_changes_the_fingerprint() {
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let operation = create_dir("/docs");
    let actor_x = ActorId::parse("x").expect("actor id");
    let actor_y = ActorId::parse("y").expect("actor id");

    let fingerprint = |actor: &ActorId| {
        CommitFingerprintInput {
            namespace_id: &namespace_id,
            actor_id: actor,
            message: None,
            operations: std::slice::from_ref(&operation),
            preconditions: &[],
            inline_content_identities: &BTreeMap::new(),
        }
        .fingerprint()
    };
    assert_ne!(fingerprint(&actor_x), fingerprint(&actor_y));
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

fn create_dir(path: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse(path).expect("path"),
        parents: false,
    }
}

fn put(path: &str, content_ref: ContentRef) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content: FileContent::Ref(content_ref),
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

#[test]
fn a_different_content_object_changes_mutation_identity() {
    let bytes = b"identical bytes, two uploads";
    let first = ContentRef::blob_v1(
        ContentId::generate(),
        bytes,
        crate::ChecksumAlgorithm::Crc64nvme,
    );
    let second = ContentRef::blob_v1(
        ContentId::generate(),
        bytes,
        crate::ChecksumAlgorithm::Crc64nvme,
    );

    assert_ne!(
        fingerprint(put("/docs/report.txt", first)),
        fingerprint(put("/docs/report.txt", second))
    );
}

#[test]
fn operation_order_changes_mutation_identity() {
    assert_ne!(
        operations_fingerprint(&[create_dir("/a"), create_dir("/b")]),
        operations_fingerprint(&[create_dir("/b"), create_dir("/a")])
    );
}

#[test]
fn put_fingerprint_changes_with_every_request_field() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let path = AbsolutePath::parse("/a.txt").expect("path");
    let content_ref = ContentRef::blob_v1(
        ContentId::generate(),
        b"hello",
        crate::ChecksumAlgorithm::Crc64nvme,
    );
    let options = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        expected_inode_id: Some(InodeId(2)),
        ..Default::default()
    };
    let fingerprint =
        |namespace_id: &NamespaceId, path: &AbsolutePath, options: &PutFileOptions| {
            CommitFingerprintInput {
                namespace_id,
                actor_id: &test_actor(),
                message: options.commit.message.as_deref(),
                operations: &[FilesystemOperation::PutFile {
                    path: path.clone(),
                    content: FileContent::Ref(content_ref.clone()),
                    behavior: options.behavior,
                    expected_inode_id: options.expected_inode_id,
                    expected_revision_no: options.expected_revision_no,
                }],
                preconditions: &[],
                inline_content_identities: &BTreeMap::new(),
            }
            .fingerprint()
        };
    let baseline = fingerprint(&namespace_id, &path, &options);

    let mut changed_behavior = options.clone();
    changed_behavior.behavior = DestinationBehavior::NoReplace;
    let mut changed_inode = options.clone();
    changed_inode.expected_inode_id = Some(InodeId(3));
    let mut changed_revision = options.clone();
    changed_revision.expected_revision_no = Some(RevisionNo(2));
    // Replaying a commit id with a different message must conflict, so the
    // message joins the preimage.
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
