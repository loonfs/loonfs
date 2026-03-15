use super::*;
use loon_types::{
    ChangeSeq, FenceToken, InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo,
    CONTENT_BLOCK_SIZE_BYTES,
};
use std::collections::BTreeMap;

fn seeded_metadata_state() -> ModelMetadataState {
    ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(7),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(5),
            },
            ModelInodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(12),
            },
            ModelInodeRecord {
                inode_id: InodeId(88),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(21),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(41),
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "docs".to_owned(),
                display_name: "docs".to_owned(),
                child_inode_id: InodeId(7),
                bind_seq: ChangeSeq(5),
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(7),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(88),
                bind_seq: ChangeSeq(21),
            },
        ],
        revisions: vec![
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(17),
                content_manifest_digest: "sha256:note-v1".to_owned(),
            },
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(2),
                committed_seq: ChangeSeq(41),
                content_manifest_digest: "sha256:note-v2".to_owned(),
            },
            ModelRevisionRecord {
                inode_id: InodeId(88),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(21),
                content_manifest_digest: "sha256:report-v1".to_owned(),
            },
        ],
        subtree_tombstones: vec![ModelSubtreeTombstoneRecord {
            root_inode_id: InodeId(7),
            tombstone_seq: ChangeSeq(40),
        }],
    }
}

#[test]
fn model_advances_seq() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    assert_eq!(ns.head_seq.0, 1);
}

#[test]
fn model_create_dir_advances_next_inode_id() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::CreateDir {
        inode_id: InodeId(7),
        writer_fence_token: FenceToken(0),
    })
    .expect("create dir should advance next inode id");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.next_inode_id, InodeId(8));
}

#[test]
fn model_create_file_advances_next_inode_id() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::CreateFile {
        inode_id: InodeId(7),
        writer_fence_token: FenceToken(0),
    })
    .expect("create file should advance next inode id");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.next_inode_id, InodeId(8));
}

#[test]
fn model_builds_uploaded_content_for_small_file() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");

    assert_eq!(uploaded.file_size_bytes, 16);
    assert_eq!(
        uploaded.file_digest_sha256,
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
    );
    assert_eq!(uploaded.manifest_envelope.payload.blocks.len(), 1);
    assert_eq!(
        uploaded.manifest_envelope.payload.blocks[0].plaintext_size_bytes,
        16
    );
}

#[test]
fn model_splits_content_at_fixed_block_boundary() {
    let mut bytes = vec![b'a'; CONTENT_BLOCK_SIZE_BYTES as usize];
    bytes.push(b'b');

    let uploaded =
        build_uploaded_content(NamespaceId::from("ns-1"), &bytes).expect("build uploaded content");

    assert_eq!(uploaded.manifest_envelope.payload.blocks.len(), 2);
    assert_eq!(
        uploaded.manifest_envelope.payload.blocks[0].plaintext_size_bytes,
        CONTENT_BLOCK_SIZE_BYTES
    );
    assert_eq!(
        uploaded.manifest_envelope.payload.blocks[1].plaintext_size_bytes,
        1
    );
}

#[test]
fn model_validates_uploaded_content_reference() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");
    let mut blocks = BTreeMap::new();
    blocks.insert(
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        b"hello from loon\n".to_vec(),
    );

    let validated = validate_uploaded_content_reference(
        &NamespaceId::from("ns-1"),
        &uploaded.content_manifest_digest,
        &uploaded.manifest_envelope,
        &blocks,
    )
    .expect("validate uploaded content reference");

    assert_eq!(validated.file_size_bytes, 16);
    assert_eq!(
        validated.file_digest_sha256,
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
    );
    assert_eq!(validated.block_count, 1);
}

#[test]
fn model_materializes_uploaded_content_reference() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");
    let mut blocks = BTreeMap::new();
    blocks.insert(
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        b"hello from loon\n".to_vec(),
    );

    let materialized = materialize_uploaded_content_reference(
        &NamespaceId::from("ns-1"),
        &uploaded.content_manifest_digest,
        &uploaded.manifest_envelope,
        &blocks,
    )
    .expect("materialize uploaded content reference");

    assert_eq!(materialized.file_size_bytes, 16);
    assert_eq!(
        materialized.file_digest_sha256,
        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
    );
    assert_eq!(materialized.bytes, b"hello from loon\n");
}

#[test]
fn model_validates_local_only_upload_record() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let resolved = validate_local_only_upload_record(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        &upload,
    )
    .expect("validate local-only upload");

    assert_eq!(
        resolved,
        "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
    );
}

#[test]
fn model_validates_inode_upload_record() {
    let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let resolved = validate_inode_upload_record(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        &upload,
    )
    .expect("validate inode upload");

    assert_eq!(
        resolved,
        "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
    );
}

#[test]
fn model_reuses_matching_local_only_upload_record() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_local_only_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        Some(&upload),
    )
    .expect("decide upload action");

    assert_eq!(
        decision,
        ModelLocalOnlyUploadDecision::ReuseExisting {
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        }
    );
}

#[test]
fn model_reuses_matching_inode_upload_record() {
    let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_inode_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        Some(&upload),
    )
    .expect("decide inode upload action");

    assert_eq!(
        decision,
        ModelInodeUploadDecision::ReuseExisting {
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        }
    );
}

#[test]
fn model_reuploads_when_existing_local_only_upload_is_stale() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_local_only_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:edited-after-upload"),
        Some(&upload),
    )
    .expect("stale upload should trigger reupload");

    assert_eq!(decision, ModelLocalOnlyUploadDecision::UploadFresh);
}

#[test]
fn model_reuploads_when_existing_inode_upload_is_stale() {
    let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let decision = decide_inode_upload_action(
        &NamespaceId::from("ns-1"),
        Some("sha256:edited-after-upload"),
        Some(&upload),
    )
    .expect("stale inode upload should trigger reupload");

    assert_eq!(decision, ModelInodeUploadDecision::UploadFresh);
}

#[test]
fn model_allocates_client_request_ids_monotonically() {
    assert_eq!(
        allocate_client_request_id(1),
        "client-req-00000000000000000001"
    );
    assert_eq!(
        allocate_client_request_id(2),
        "client-req-00000000000000000002"
    );
}

#[test]
fn model_reuses_existing_client_request_id_for_retry() {
    let (request_id, allocated_new) =
        reuse_or_allocate_client_request_id(Some("client-req-00000000000000000007"), 8);

    assert_eq!(request_id, "client-req-00000000000000000007");
    assert!(!allocated_new);
}

#[test]
fn model_selects_next_local_only_action_deterministically() {
    let selected = select_next_local_only_action(&[
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000003".to_owned(),
            created_at_ms: 1_700_000_300_000,
        },
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        },
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
            created_at_ms: 1_700_000_200_000,
        },
    ])
    .expect("one action should be selected");

    assert_eq!(
        selected,
        ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        }
    );
}

#[test]
fn model_selects_next_client_action_preferring_local_only_on_tie() {
    let selected = select_next_client_action(
        Some(&ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        }),
        Some(&ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            created_at_ms: 1_700_000_200_000,
        }),
        None,
    )
    .expect("one action should be selected");

    assert_eq!(
        selected,
        ModelScheduledClientAction::LocalOnlyCreate(ModelPlannedLocalOnlyAction {
            client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn model_selects_executable_inode_action_before_deferred_inode_action() {
    let selected = select_next_client_action(
        None,
        Some(&ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            created_at_ms: 1_700_000_205_000,
        }),
        Some(&ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            created_at_ms: 1_700_000_200_000,
        }),
    )
    .expect("one action should be selected");

    assert_eq!(
        selected,
        ModelScheduledClientAction::PlannedInodeAction(ModelPlannedInodeAction {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            created_at_ms: 1_700_000_205_000,
        })
    );
}

#[test]
fn model_selects_unique_local_only_bind_candidate_for_remote_observation() {
    let selected = select_local_only_observation_bind_candidate(
        &[
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
            },
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:different".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "other.txt".to_owned(),
                exists_on_disk: true,
            },
        ],
        &ModelObservedRemoteInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(42),
            revision_no: RevisionNo(1),
            content_digest: Some(
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            ),
            content_manifest_digest: Some(
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            ),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            is_deleted: false,
        },
    )
    .expect("select unique bind candidate");

    assert_eq!(selected, Some("tmp:ns-1:00000000000000000001".to_owned()));
}

#[test]
fn model_rejects_ambiguous_local_only_bind_candidate_for_remote_observation() {
    let error = select_local_only_observation_bind_candidate(
        &[
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
            },
            ModelLocalOnlyObservationCandidate {
                client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
                namespace_id: NamespaceId::from("ns-1"),
                inode_kind: InodeKind::File,
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
            },
        ],
        &ModelObservedRemoteInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(42),
            revision_no: RevisionNo(1),
            content_digest: Some(
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            ),
            content_manifest_digest: Some(
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            ),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            is_deleted: false,
        },
    )
    .expect_err("ambiguous local-only bind should be rejected");

    assert_eq!(
        error,
        ModelRemoteObservationSelectionError::AmbiguousLocalOnlyBind { matches: 2 }
    );
}

#[test]
fn model_detects_bound_local_match_for_remote_observation() {
    let matches = bound_local_matches_remote_observation(
        &InodeKind::File,
        Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
        Some(InodeId(2)),
        "report.txt",
        true,
        &ModelObservedRemoteInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(42),
            revision_no: RevisionNo(18),
            content_digest: Some(
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            ),
            content_manifest_digest: Some(
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            ),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        },
    );

    assert!(matches);
    assert!(remote_observation_is_stale(
        Some(ChangeSeq(42)),
        ChangeSeq(42)
    ));
    assert!(!remote_observation_is_stale(
        Some(ChangeSeq(41)),
        ChangeSeq(42)
    ));
}

#[test]
fn model_supports_remote_only_discovery_from_authoritative_observation() {
    let observed = ModelObservedRemoteInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(601),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(42),
        revision_no: RevisionNo(1),
        content_digest: Some(
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        ),
        content_manifest_digest: Some(
            "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        ),
        parent_inode_id: Some(InodeId(2)),
        display_name: "welcome.txt".to_owned(),
        is_deleted: false,
    };

    assert!(remote_only_discovery_supported(&observed));

    let observed_dir = ModelObservedRemoteInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(701),
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(52),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: Some(InodeId(2)),
        display_name: "incoming".to_owned(),
        is_deleted: false,
    };

    assert!(remote_only_discovery_supported(&observed_dir));
}

#[test]
fn model_detects_remote_only_placeholder_match_for_materialization() {
    let observed = ModelObservedRemoteInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(601),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(42),
        revision_no: RevisionNo(1),
        content_digest: Some(
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
        ),
        content_manifest_digest: Some(
            "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6".to_owned(),
        ),
        parent_inode_id: Some(InodeId(2)),
        display_name: "welcome.txt".to_owned(),
        is_deleted: false,
    };

    assert!(remote_only_placeholder_matches_remote_observation(
        &InodeKind::File,
        Some(InodeId(2)),
        "welcome.txt",
        false,
        false,
        &observed,
    ));
    assert!(!remote_only_placeholder_matches_remote_observation(
        &InodeKind::File,
        Some(InodeId(2)),
        "welcome.txt",
        true,
        false,
        &observed,
    ));
}

#[test]
fn model_child_name_absent_rejects_existing_bound_name() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_child_name_absent(InodeId(2), "note.txt", ChangeSeq(41))
        .expect_err("existing child name should collide");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::ChildNameCollision {
            parent_inode_id: InodeId(2),
            name_key: "note.txt".to_owned(),
            child_inode_id: InodeId(42),
        }
    );
}

#[test]
fn model_inode_revision_is_rejects_stale_revision() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_inode_revision_is(InodeId(42), RevisionNo(1), ChangeSeq(41))
        .expect_err("stale base revision should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::InodeRevisionMismatch {
            inode_id: InodeId(42),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    );
}

#[test]
fn model_inode_is_directory_rejects_file_inode() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_inode_is_directory(InodeId(42), ChangeSeq(41))
        .expect_err("file inode should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::InodeNotDirectory {
            inode_id: InodeId(42),
            actual_kind: InodeKind::File,
        }
    );
}

#[test]
fn model_ancestors_not_subtree_deleted_rejects_covered_inode() {
    let metadata = seeded_metadata_state();

    let error = metadata
        .ensure_ancestors_not_subtree_deleted(InodeId(88), ChangeSeq(41))
        .expect_err("covered descendant should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::AncestorCoveredBySubtreeTombstone {
            inode_id: InodeId(88),
            root_inode_id: InodeId(7),
            tombstone_seq: ChangeSeq(40),
        }
    );
}

#[test]
fn model_distinguishes_raw_and_visible_metadata_queries() {
    let metadata = seeded_metadata_state();

    assert_eq!(
        metadata.inode_at_seq(InodeId(7), ChangeSeq(41)),
        Some(ModelInodeRecord {
            inode_id: InodeId(7),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(5),
        })
    );
    assert_eq!(metadata.visible_inode(InodeId(7), ChangeSeq(41)), None);
    assert_eq!(
        metadata.bound_child_at_seq(InodeId(2), "docs", ChangeSeq(41)),
        Some(ModelDirentryRecord {
            parent_inode_id: InodeId(2),
            name_key: "docs".to_owned(),
            display_name: "docs".to_owned(),
            child_inode_id: InodeId(7),
            bind_seq: ChangeSeq(5),
        })
    );
    assert_eq!(
        metadata.visible_child(InodeId(2), "docs", ChangeSeq(41)),
        None
    );
    assert_eq!(
        metadata.current_revision_head(InodeId(42), ChangeSeq(41)),
        Some(ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(2),
            committed_seq: ChangeSeq(41),
            content_manifest_digest: "sha256:note-v2".to_owned(),
        })
    );
}

#[test]
fn model_apply_create_dir_appends_inode_and_direntry_rows() {
    let applied = ModelMetadataState::default()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::CreateDir {
                inode_id: InodeId(501),
                parent_inode_id: InodeId(2),
                display_name: "drafts".to_owned(),
            }],
        )
        .expect("apply create_dir metadata");

    assert_eq!(
        applied.metadata_state.inodes,
        vec![ModelInodeRecord {
            inode_id: InodeId(501),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(42),
        }]
    );
    assert_eq!(
        applied.metadata_state.direntries,
        vec![ModelDirentryRecord {
            parent_inode_id: InodeId(2),
            name_key: "drafts".to_owned(),
            display_name: "drafts".to_owned(),
            child_inode_id: InodeId(501),
            bind_seq: ChangeSeq(42),
        }]
    );
    assert!(applied
        .checked_invariants
        .contains(&"create_dir_writes_inode_and_direntry_rows".to_owned()));
}

#[test]
fn model_apply_create_file_appends_initial_revision_row() {
    let applied = ModelMetadataState::default()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::CreateFile {
                inode_id: InodeId(501),
                parent_inode_id: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:note-v1".to_owned(),
            }],
        )
        .expect("apply create_file metadata");

    assert_eq!(
        applied.metadata_state.inodes,
        vec![ModelInodeRecord {
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(42),
        }]
    );
    assert_eq!(
        applied.metadata_state.direntries,
        vec![ModelDirentryRecord {
            parent_inode_id: InodeId(2),
            name_key: "note.txt".to_owned(),
            display_name: "note.txt".to_owned(),
            child_inode_id: InodeId(501),
            bind_seq: ChangeSeq(42),
        }]
    );
    assert_eq!(
        applied.metadata_state.revisions,
        vec![ModelRevisionRecord {
            inode_id: InodeId(501),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(42),
            content_manifest_digest: "sha256:note-v1".to_owned(),
        }]
    );
    assert!(applied
        .checked_invariants
        .contains(&"create_file_writes_inode_direntry_and_initial_revision".to_owned()));
}

#[test]
fn model_apply_replace_file_appends_next_revision_row() {
    let applied = seeded_metadata_state()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::ReplaceFile {
                inode_id: InodeId(42),
                base_revision_no: RevisionNo(2),
                content_manifest_digest: "sha256:note-v3".to_owned(),
            }],
        )
        .expect("apply replace_file metadata");

    assert_eq!(
        applied
            .metadata_state
            .latest_revision_head_at_seq(InodeId(42), ChangeSeq(42))
            .expect("revision head after replace"),
        ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(3),
            committed_seq: ChangeSeq(42),
            content_manifest_digest: "sha256:note-v3".to_owned(),
        }
    );
    assert!(applied
        .checked_invariants
        .contains(&"replace_file_appends_new_revision_head".to_owned()));
}

#[test]
fn model_apply_restore_revision_appends_new_head_from_historical_content() {
    let applied = seeded_metadata_state()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::RestoreRevision {
                inode_id: InodeId(42),
                base_revision_no: RevisionNo(2),
                restore_from_revision_no: RevisionNo(1),
            }],
        )
        .expect("apply restore_revision metadata");

    assert_eq!(
        applied
            .metadata_state
            .latest_revision_head_at_seq(InodeId(42), ChangeSeq(42))
            .expect("revision head after restore"),
        ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(3),
            committed_seq: ChangeSeq(42),
            content_manifest_digest: "sha256:note-v1".to_owned(),
        }
    );
    assert!(applied
        .checked_invariants
        .contains(&"restore_creates_new_revision_head".to_owned()));
}

#[test]
fn model_apply_rename_appends_new_binding_and_hides_old_visible_name() {
    let applied = seeded_metadata_state()
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::Rename {
                inode_id: InodeId(42),
                new_parent_inode_id: InodeId(2),
                new_display_name: "renamed.txt".to_owned(),
            }],
        )
        .expect("apply rename metadata");

    assert_eq!(
        applied
            .metadata_state
            .visible_child(InodeId(2), "note.txt", ChangeSeq(42)),
        None
    );
    assert_eq!(
        applied
            .metadata_state
            .visible_child(InodeId(2), "renamed.txt", ChangeSeq(42))
            .expect("renamed visible child")
            .child_inode_id,
        InodeId(42)
    );
    assert!(applied
        .checked_invariants
        .contains(&"rename_appends_new_direntry_binding".to_owned()));
}

#[test]
fn model_apply_delete_subtree_appends_tombstone_row_and_hides_descendants() {
    let applied = ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(7),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(5),
            },
            ModelInodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(17),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "docs".to_owned(),
                display_name: "docs".to_owned(),
                child_inode_id: InodeId(7),
                bind_seq: ChangeSeq(5),
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(7),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(17),
            },
        ],
        revisions: vec![ModelRevisionRecord {
            inode_id: InodeId(42),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(17),
            content_manifest_digest: "sha256:report-v1".to_owned(),
        }],
        subtree_tombstones: Vec::new(),
    }
    .apply_committed_mutations(
        ChangeSeq(42),
        &[ModelMetadataMutation::DeleteSubtree {
            root_inode_id: InodeId(7),
        }],
    )
    .expect("apply delete_subtree metadata");

    assert_eq!(
        applied.metadata_state.subtree_tombstones,
        vec![ModelSubtreeTombstoneRecord {
            root_inode_id: InodeId(7),
            tombstone_seq: ChangeSeq(42),
        }]
    );
    assert_eq!(
        applied
            .metadata_state
            .visible_inode(InodeId(7), ChangeSeq(42)),
        None
    );
    assert_eq!(
        applied
            .metadata_state
            .visible_inode(InodeId(42), ChangeSeq(42)),
        None
    );
    assert!(applied
        .checked_invariants
        .contains(&"delete_subtree_writes_tombstone_row".to_owned()));
}

#[test]
fn model_restore_source_must_be_historical() {
    let error = seeded_metadata_state()
        .ensure_restore_source_revision_exists(
            InodeId(42),
            RevisionNo(2),
            RevisionNo(2),
            ChangeSeq(41),
        )
        .expect_err("current head cannot be restore source");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::SourceRevisionNotHistorical {
            inode_id: InodeId(42),
            base_revision_no: RevisionNo(2),
            restore_from_revision: RevisionNo(2),
        }
    );
}

#[test]
fn model_rejects_directory_rename_cycle() {
    let metadata_state = ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(7),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(5),
            },
            ModelInodeRecord {
                inode_id: InodeId(9),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(8),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "docs".to_owned(),
                display_name: "docs".to_owned(),
                child_inode_id: InodeId(7),
                bind_seq: ChangeSeq(5),
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(7),
                name_key: "archive".to_owned(),
                display_name: "archive".to_owned(),
                child_inode_id: InodeId(9),
                bind_seq: ChangeSeq(8),
            },
        ],
        revisions: Vec::new(),
        subtree_tombstones: Vec::new(),
    };

    let error = metadata_state
        .ensure_rename_does_not_cycle(InodeId(7), InodeId(9), ChangeSeq(52))
        .expect_err("directory cycle should be rejected");

    assert_eq!(
        error,
        ModelMetadataPreconditionError::RenameWouldCycle {
            inode_id: InodeId(7),
            new_parent_inode_id: InodeId(9),
        }
    );
}

#[test]
fn model_visible_child_prefers_latest_slot_binding_when_name_is_reused() {
    let metadata_state = ModelMetadataState {
        inodes: vec![
            ModelInodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            ModelInodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(10),
            },
            ModelInodeRecord {
                inode_id: InodeId(77),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(30),
            },
        ],
        direntries: vec![
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(10),
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "archive.txt".to_owned(),
                display_name: "archive.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(20),
            },
            ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(77),
                bind_seq: ChangeSeq(30),
            },
        ],
        revisions: vec![
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(10),
                content_manifest_digest: "sha256:note-v1".to_owned(),
            },
            ModelRevisionRecord {
                inode_id: InodeId(77),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(30),
                content_manifest_digest: "sha256:note-v2".to_owned(),
            },
        ],
        subtree_tombstones: Vec::new(),
    };

    assert_eq!(
        metadata_state
            .visible_child(InodeId(2), "note.txt", ChangeSeq(30))
            .expect("latest visible note.txt binding")
            .child_inode_id,
        InodeId(77)
    );
    let old_child_binding = metadata_state
        .current_parent_binding_for_child(InodeId(42), ChangeSeq(30))
        .expect("latest binding for renamed-away inode");
    assert_eq!(old_child_binding.parent_inode_id, InodeId(2));
    assert_eq!(old_child_binding.name_key, "archive.txt");
}

#[test]
fn model_rejects_local_only_upload_record_when_digest_mismatches() {
    let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

    let error = validate_local_only_upload_record(
        &NamespaceId::from("ns-1"),
        Some("sha256:different"),
        &upload,
    )
    .expect_err("mismatched digest should be rejected");

    assert_eq!(
        error,
        ModelLocalOnlyUploadValidationError::FileDigestMismatch {
            expected: "sha256:different".to_owned(),
            actual: "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                .to_owned(),
        }
    );
}

#[test]
fn model_rejects_uploaded_content_reference_when_block_is_missing() {
    let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
        .expect("build uploaded content");
    let error = validate_uploaded_content_reference(
        &NamespaceId::from("ns-1"),
        &uploaded.content_manifest_digest,
        &uploaded.manifest_envelope,
        &BTreeMap::new(),
    )
    .expect_err("missing block should be rejected");

    assert_eq!(
        error,
        ModelContentValidationError::MissingBlock {
            digest: "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                .to_owned(),
        }
    );
}

#[test]
fn model_rejects_stale_writer_after_fence_rotation() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::RotateFence {
        new_fence_token: FenceToken(9),
    })
    .expect("fence rotation should succeed");

    let error = ns
        .apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(8),
        })
        .expect_err("stale writer should be rejected");

    assert_eq!(
        error,
        ModelError::StaleWriterFenceToken {
            expected: FenceToken(9),
            actual: FenceToken(8),
        }
    );
}

#[test]
fn model_accepts_active_commit_attempt() {
    let ns = ModelNamespace {
        namespace_id: NamespaceId::from("ns-1"),
        head_seq: ChangeSeq(41),
        active_fence_token: FenceToken(8),
        next_inode_id: InodeId(501),
        snapshot_hint_seq: Some(ChangeSeq(40)),
        retention_floor_seq: ChangeSeq(40),
        metadata_state: ModelMetadataState::default(),
    };
    let lease = LeaseState {
        namespace_id: NamespaceId::from("ns-1"),
        holder_id: "writer-a".to_owned(),
        fence_token: FenceToken(8),
        lease_expires_at_ms: 1_000,
    };
    let request = ModelCommitValidationRequest {
        namespace_id: NamespaceId::from("ns-1"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(8),
        planned_head_seq: ChangeSeq(41),
    };

    let outcome = ns
        .validate_commit_attempt(&request, &lease, 999)
        .expect("active writer should validate");

    assert_eq!(
        outcome,
        ModelCommitValidationOutcome {
            next_seq: ChangeSeq(42),
        }
    );
}

#[test]
fn model_stale_commit_attempt_hits_planned_head_seq_mismatch_after_handover_publish() {
    let ns = ModelNamespace {
        namespace_id: NamespaceId::from("ns-1"),
        head_seq: ChangeSeq(42),
        active_fence_token: FenceToken(9),
        next_inode_id: InodeId(504),
        snapshot_hint_seq: Some(ChangeSeq(40)),
        retention_floor_seq: ChangeSeq(40),
        metadata_state: ModelMetadataState::default(),
    };
    let lease = LeaseState {
        namespace_id: NamespaceId::from("ns-1"),
        holder_id: "writer-b".to_owned(),
        fence_token: FenceToken(9),
        lease_expires_at_ms: 2_000,
    };
    let request = ModelCommitValidationRequest {
        namespace_id: NamespaceId::from("ns-1"),
        writer_id: "writer-a".to_owned(),
        writer_fence_token: FenceToken(8),
        planned_head_seq: ChangeSeq(41),
    };

    let error = ns
        .validate_commit_attempt(&request, &lease, 1_500)
        .expect_err("stale writer should be rejected");

    assert_eq!(
        error,
        ModelCommitValidationError::PlannedHeadSeqMismatch {
            expected: ChangeSeq(42),
            actual: ChangeSeq(41),
        }
    );
}

#[test]
fn model_prepares_next_wal_commit_seq_for_active_writer() {
    let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let wal = ns
        .prepare_wal_commit("req-20260311-0001", FenceToken(0))
        .expect("active writer should prepare WAL");

    assert_eq!(wal.seq, ChangeSeq(1));
    assert_eq!(wal.base_head_seq, ChangeSeq(0));
    assert_eq!(wal.commit_id, "req-20260311-0001");
}

#[test]
fn model_replays_contiguous_wal_commit() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let wal = ns
        .prepare_wal_commit("req-20260311-0001", FenceToken(0))
        .expect("active writer should prepare WAL");

    ns.replay_wal_commit(&wal)
        .expect("contiguous WAL should replay");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.active_fence_token, FenceToken(0));
}

#[test]
fn model_rejects_non_contiguous_wal_commit() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let wal = ModelWalCommit {
        namespace_id: NamespaceId::from("ns-1"),
        seq: ChangeSeq(2),
        base_head_seq: ChangeSeq(0),
        commit_id: "req-20260311-0001".to_owned(),
        writer_fence_token: FenceToken(0),
        ops: Vec::new(),
    };

    let error = ns
        .replay_wal_commit(&wal)
        .expect_err("gap should be rejected");

    assert_eq!(
        error,
        ModelError::NonContiguousSeq {
            expected: ChangeSeq(1),
            actual: ChangeSeq(2),
        }
    );
}

#[test]
fn model_restores_from_verified_checkpoint() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::RotateFence {
        new_fence_token: FenceToken(9),
    })
    .expect("fence rotation should succeed");
    ns.apply(ModelAction::CreateDir {
        inode_id: InodeId(41),
        writer_fence_token: FenceToken(9),
    })
    .expect("active writer should advance seq");

    let checkpoint = ns.checkpoint();
    let available_segment_keys = checkpoint
        .tables
        .iter()
        .flat_map(|table| {
            table
                .segments
                .iter()
                .map(|segment| segment.object_key.clone())
        })
        .collect::<Vec<_>>();
    let restored = ModelNamespace::restore_from_checkpoint(&checkpoint, &available_segment_keys)
        .expect("checkpoint restore");

    assert_eq!(restored.namespace_id, NamespaceId::from("ns-1"));
    assert_eq!(restored.head_seq, ChangeSeq(1));
    assert_eq!(restored.active_fence_token, FenceToken(9));
    assert_eq!(restored.next_inode_id, InodeId(42));
    assert_eq!(restored.snapshot_hint_seq, Some(ChangeSeq(1)));
    assert_eq!(restored.retention_floor_seq, ChangeSeq(0));
}

#[test]
fn model_publishes_verified_checkpoint_into_head_summary() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::RotateFence {
        new_fence_token: FenceToken(9),
    })
    .expect("fence rotation should succeed");
    ns.apply(ModelAction::CreateDir {
        inode_id: InodeId(41),
        writer_fence_token: FenceToken(9),
    })
    .expect("active writer should advance seq");

    let checkpoint = ns.checkpoint();
    ns.publish_checkpoint(
        &checkpoint,
        &available_segment_keys(&checkpoint),
        Some(ChangeSeq(1)),
        Some(&sample_publish_authorizers(ChangeSeq(1))),
    )
    .expect("checkpoint publication should succeed");

    assert_eq!(ns.head_seq, ChangeSeq(1));
    assert_eq!(ns.active_fence_token, FenceToken(9));
    assert_eq!(ns.next_inode_id, InodeId(42));
    assert_eq!(ns.snapshot_hint_seq, Some(ChangeSeq(1)));
    assert_eq!(ns.retention_floor_seq, ChangeSeq(1));
}

#[test]
fn model_progress_publish_is_monotonic() {
    let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let current = ModelProgressObject {
        namespace_id: NamespaceId::from("ns-1"),
        work_class: "BuildSnapshot".to_owned(),
        through_seq: ChangeSeq(42),
    };

    let published = ns
        .publish_progress(Some(&current), "BuildSnapshot", ChangeSeq(41))
        .expect("stale progress publish should no-op");

    assert_eq!(published, current);
}

#[test]
fn model_repair_enqueues_snapshot_job_when_progress_lags() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let progress = ModelProgressObject {
        namespace_id: NamespaceId::from("ns-1"),
        work_class: "BuildSnapshot".to_owned(),
        through_seq: ChangeSeq(0),
    };
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![],
    };

    let outcome = ns
        .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
        .expect("repair should enqueue missing snapshot job");

    assert_eq!(
        outcome,
        ModelQueueRepairOutcome::Enqueued {
            through_seq: ChangeSeq(1),
        }
    );
    assert_eq!(queue.jobs.len(), 1);
    assert_eq!(queue.jobs[0].dedupe_key, "BuildSnapshot:ns-1");
    assert_eq!(queue.jobs[0].payload.through_seq, ChangeSeq(1));
}

#[test]
fn model_repair_attaches_follow_up_for_claimed_snapshot_job() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq again");
    let progress = ModelProgressObject {
        namespace_id: NamespaceId::from("ns-1"),
        work_class: "BuildSnapshot".to_owned(),
        through_seq: ChangeSeq(0),
    };
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![ModelQueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: "BuildSnapshot:ns-1".to_owned(),
            state: ModelQueueJobState::Claimed,
            payload: ModelQueueSeqPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(1),
            },
            follow_up: None,
            claim: Some(ModelQueueClaim {
                worker_id: "worker-a".to_owned(),
                claim_token: "claim-a".to_owned(),
                heartbeat_at_ms: 0,
                timeout_at_ms: 10_000,
            }),
            attempts: 1,
        }],
    };

    let outcome = ns
        .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
        .expect("repair should attach follow-up to claimed job");

    assert_eq!(
        outcome,
        ModelQueueRepairOutcome::AttachedFollowUp {
            through_seq: ChangeSeq(2),
        }
    );
    assert_eq!(
        queue.jobs[0].follow_up,
        Some(ModelQueueSeqPayload {
            namespace_id: NamespaceId::from("ns-1"),
            through_seq: ChangeSeq(2),
        })
    );
}

#[test]
fn model_broker_lease_takeover_fences_old_generation() {
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![],
    };

    assert_eq!(
        queue
            .renew_broker_lease("broker-a", 0, 10_000)
            .expect("first lease should be acquired"),
        ModelBrokerLeaseOutcome::Acquired { epoch: 1 }
    );
    assert_eq!(
        queue
            .renew_broker_lease("broker-b", 30_000, 10_000)
            .expect("expired lease should be takeable"),
        ModelBrokerLeaseOutcome::TakenOver { epoch: 2 }
    );
    assert_eq!(
        ensure_active_broker_lease(&queue, "broker-a", 1, 30_001)
            .expect_err("old broker generation should be fenced"),
        ModelError::BrokerLeaseMismatch {
            expected_broker_id: "broker-b".to_owned(),
            expected_epoch: 2,
            actual_broker_id: "broker-a".to_owned(),
            actual_epoch: 1,
        }
    );
}

#[test]
fn model_claim_timeout_then_steal_rejects_stale_complete() {
    let mut queue = ModelQueueShard {
        work_class: ModelQueueWorkClass::BuildSnapshot,
        shard_id: 17,
        broker: None,
        jobs: vec![ModelQueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: "BuildSnapshot:ns-1".to_owned(),
            state: ModelQueueJobState::Ready,
            payload: ModelQueueSeqPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(420),
            },
            follow_up: None,
            claim: None,
            attempts: 0,
        }],
    };
    queue
        .renew_broker_lease("broker-a", 0, 10_000)
        .expect("broker-a should acquire lease");
    assert_eq!(
        queue
            .claim_job("broker-a", 1, "worker-a", "claim-a", "job-1", 0, 10_000)
            .expect("worker-a should claim job"),
        ModelJobClaimOutcome::Claimed {
            claim_token: "claim-a".to_owned(),
        }
    );

    queue
        .renew_broker_lease("broker-b", 30_000, 10_000)
        .expect("broker-b should take over after expiry");
    assert_eq!(
        queue
            .claim_job("broker-b", 2, "worker-b", "claim-b", "job-1", 30_000, 10_000,)
            .expect("worker-b should steal expired job"),
        ModelJobClaimOutcome::Stolen {
            claim_token: "claim-b".to_owned(),
        }
    );

    assert_eq!(
        queue
            .complete_job("broker-b", 2, "job-1", "claim-a", 30_001)
            .expect_err("stale claim token should be rejected"),
        ModelError::ClaimTokenMismatch {
            expected: "claim-b".to_owned(),
            actual: "claim-a".to_owned(),
        }
    );
    assert_eq!(
        queue
            .complete_job("broker-b", 2, "job-1", "claim-b", 30_001)
            .expect("fresh claim should complete"),
        ModelJobCompleteOutcome::Removed
    );
    assert!(queue.jobs.is_empty());
}

#[test]
fn model_rejects_retention_floor_without_authorizers() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let checkpoint = ns.checkpoint();

    let error = ns
        .publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(1)),
            None,
        )
        .expect_err("missing authorizers should fail");

    assert_eq!(
        error,
        ModelError::MissingRetentionAuthorizers {
            requested: ChangeSeq(1),
        }
    );
}

#[test]
fn model_rejects_retention_floor_when_required_progress_lags() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let checkpoint = ns.checkpoint();
    let authorizers = sample_publish_authorizers(ChangeSeq(0));

    let error = ns
        .publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(1)),
            Some(&authorizers),
        )
        .expect_err("lagging required progress should fail");

    assert_eq!(
        error,
        ModelError::RequiredProgressLag {
            work_class: "BuildListingIndex".to_owned(),
            requested: ChangeSeq(1),
            available: ChangeSeq(0),
        }
    );
}

#[test]
fn model_rejects_retention_floor_above_checkpoint() {
    let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    ns.apply(ModelAction::BumpSeq {
        writer_fence_token: FenceToken(0),
    })
    .expect("active writer should advance seq");
    let checkpoint = ns.checkpoint();

    let error = ns
        .publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(2)),
            Some(&sample_publish_authorizers(ChangeSeq(2))),
        )
        .expect_err("retention floor beyond checkpoint should fail");

    assert_eq!(
        error,
        ModelError::RetentionFloorBeyondCheckpoint {
            checkpoint_seq: ChangeSeq(1),
            requested: ChangeSeq(2),
        }
    );
}

#[test]
fn model_checkpoint_includes_one_empty_segment_per_family() {
    let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
    let checkpoint = ns.checkpoint();

    assert_eq!(checkpoint.tables.len(), 4);
    assert!(checkpoint
        .tables
        .iter()
        .all(|table| table.segments.len() == 1));
    assert!(checkpoint
        .tables
        .iter()
        .all(|table| table.segments[0].segment_index == 0));
    assert!(checkpoint
        .tables
        .iter()
        .all(|table| table.segments[0].row_count == 0));
    assert!(checkpoint
        .tables
        .iter()
        .all(|table| table.segments[0].object_key.contains("/tables/")));
}

#[test]
fn model_rejects_restore_when_checkpoint_segment_is_missing() {
    let checkpoint = ModelNamespace::new(NamespaceId::from("ns-1")).checkpoint();
    let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
        .expect_err("missing checkpoint segment should fail");

    assert_eq!(
        error,
        ModelError::MissingCheckpointSegment {
            object_key:
                "namespaces/ns-1/snapshots/00000000000000000000/tables/inodes-00000.sst.zst"
                    .to_owned(),
        }
    );
}

#[test]
fn model_rejects_unverified_checkpoint() {
    let checkpoint = ModelCheckpoint {
        namespace_id: NamespaceId::from("ns-1"),
        checkpoint_seq: ChangeSeq(40),
        active_fence_token: FenceToken(8),
        next_inode_id: InodeId(501),
        retention_floor_seq: ChangeSeq(40),
        verified: false,
        tables: vec![],
    };

    let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
        .expect_err("unverified checkpoint should fail");

    assert_eq!(
        error,
        ModelError::UnverifiedCheckpoint {
            checkpoint_seq: ChangeSeq(40),
        }
    );
}

fn available_segment_keys(checkpoint: &ModelCheckpoint) -> Vec<String> {
    checkpoint
        .tables
        .iter()
        .flat_map(|table| {
            table
                .segments
                .iter()
                .map(|segment| segment.object_key.clone())
        })
        .collect()
}

fn sample_publish_authorizers(through_seq: ChangeSeq) -> ModelCheckpointPublishAuthorizers {
    ModelCheckpointPublishAuthorizers {
        required_progress: vec![ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildListingIndex".to_owned(),
            through_seq,
        }],
        retention_policy: ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "RetentionPolicy".to_owned(),
            through_seq,
        },
    }
}
