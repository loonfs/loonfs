//! WAL size estimates checked against planned requests at the API limits.

use super::super::engine::CommitCandidate;
use super::super::CommitPlan;
use super::*;
use crate::limits::{MAX_COMMIT_MESSAGE_BYTES, MAX_COMMIT_OPERATIONS};
use crate::metadata::{MetadataState, MetadataView};
use crate::namespace::state::NamespaceReadState;
use crate::path::write::PublishPlanningSession;
use crate::segment::VerifiedMetadataSegments;
use crate::storage::inline_content::InlineContent;
use crate::storage::tail_content::ContentWrites;
use crate::wal::prepare_wal_object;
use loonfs_test_support::ids::{attribute_key, attribute_text};
use loonfs_types::format::manifest::{DeltaPosition, RevisionSummary};
use loonfs_types::format::wal::{
    MergeRecord, WalDelta, MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_BYTES,
    MAX_WAL_OBJECT_INLINE_CONTENT_BYTES, WAL_OBJECT_OVERHEAD_BYTES,
};
use loonfs_types::ROOT_INODE_ID;
use loonfs_types::{
    ActorId, AttributeKey, Attributes, AttributesRevisionNo, ChangeSeq, Checksum, CommitId,
    ContentId, ContentRef, ContentRefKind, DestinationBehavior, DestinationPrecondition,
    DisplayName, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo, WalNo, WriterEpoch,
    MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES, MAX_PUBLIC_INTEGER,
};
use std::collections::BTreeMap;

fn full_attributes() -> Attributes {
    let mut entries = BTreeMap::from([(attribute_key("x"), attribute_text("a"))]);
    let mut remaining =
        MAX_ATTRIBUTES_TOTAL_BYTES - 2 - (MAX_ATTRIBUTE_ENTRIES - 1) * MAX_ATTRIBUTE_KEY_BYTES;
    for index in 1..MAX_ATTRIBUTE_ENTRIES {
        let length = remaining.min(MAX_ATTRIBUTE_VALUE_BYTES);
        remaining -= length;
        entries.insert(
            AttributeKey::parse(format!("{index:0128}")).expect("key"),
            attribute_text(&"v".repeat(length)),
        );
    }
    let attributes = Attributes::new(entries).expect("full attributes");
    assert_eq!(attributes.logical_bytes(), MAX_ATTRIBUTES_TOTAL_BYTES);
    assert_eq!(attributes.len(), MAX_ATTRIBUTE_ENTRIES);
    attributes
}

#[tokio::test]
async fn maximum_requests_encode_within_the_admitted_estimate() {
    let directory = tempfile::tempdir().expect("store directory");
    let store =
        loonfs_objectstore::local_fs_store::LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("n".repeat(MAX_ID_BYTES)).expect("namespace");
    let actor = ActorId::parse("a".repeat(256)).expect("actor");
    let mut head =
        NamespaceReadState::initial(namespace_id.clone(), 0, loonfs_test_support::test_actor());
    head.seq = ChangeSeq(MAX_PUBLIC_INTEGER - 1);
    head.wal_no = WalNo(MAX_PUBLIC_INTEGER - 1);
    head.writer_epoch = WriterEpoch(MAX_PUBLIC_INTEGER);
    let content_ref = ContentRef {
        kind: ContentRefKind::BlobV1,
        content_id: ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content id"),
        size_bytes: u64::MAX,
        checksum: Checksum::crc64nvme(b"content"),
    };
    let log_ref = ContentRef::blob_v1(
        ContentId::parse("con_7yvjx9gxjm688fxq5tk1v58cgg").expect("content id"),
        b"log",
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let mut state = MetadataState::default();
    state.apply_committed_wal_deltas_mut(
        head.seq,
        &CommitId::parse("seed").expect("commit"),
        &actor,
        0,
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: ROOT_INODE_ID,
                inode_kind: InodeKind::Directory,
            },
            WalDelta::CreateInode {
                delta_index: 1,
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
            },
            WalDelta::BindDirentry {
                delta_index: 2,
                parent_inode_id: ROOT_INODE_ID,
                name_key: NameKey::parse("source").expect("key"),
                display_name: DisplayName::parse("source").expect("name"),
                child_inode_id: InodeId(2),
                child_kind: loonfs_types::InodeKind::File,
                child_created_by: actor.clone(),
                child_created_at_ms: 0,
                revision: Some(RevisionSummary {
                    revision_no: RevisionNo(MAX_PUBLIC_INTEGER - 1),
                    size_bytes: content_ref.size_bytes,
                    revision_committed_by: actor.clone(),
                    revision_committed_at_ms: 0,
                }),
            },
            WalDelta::AppendFileRevision {
                delta_index: 3,
                inode_id: InodeId(2),
                parent_inode_id: ROOT_INODE_ID,
                name_key: NameKey::parse("source").expect("key"),
                display_name: DisplayName::parse("source").expect("name"),
                bind_position: DeltaPosition {
                    seq: head.seq,
                    delta_index: 2,
                },
                child_created_by: actor.clone(),
                child_created_at_ms: 0,
                revision_no: RevisionNo(MAX_PUBLIC_INTEGER - 1),
                content_ref: content_ref.clone(),
                layout: Some(loonfs_types::ContentLayout {
                    creator_namespace_id: namespace_id.clone(),
                    extents: (0..16)
                        .map(|index| loonfs_types::ContentExtent {
                            content_id: content_ref.content_id.clone(),
                            object: loonfs_types::ExtentObject::Whole,
                            offset: 0,
                            length: if index == 0 { u64::MAX - 15 } else { 1 },
                        })
                        .collect(),
                }),
                append_offset: None,
            },
            WalDelta::AppendAttributesRevision {
                delta_index: 4,
                inode_id: InodeId(2),
                attributes_revision_no: AttributesRevisionNo(
                    MAX_PUBLIC_INTEGER - MAX_COMMIT_OPERATIONS as u64,
                ),
                attributes: full_attributes(),
            },
            WalDelta::CreateInode {
                delta_index: 5,
                inode_id: InodeId(3),
                inode_kind: InodeKind::File,
            },
            WalDelta::BindDirentry {
                delta_index: 6,
                parent_inode_id: ROOT_INODE_ID,
                name_key: NameKey::parse("log").expect("key"),
                display_name: DisplayName::parse("log").expect("name"),
                child_inode_id: InodeId(3),
                child_kind: loonfs_types::InodeKind::File,
                child_created_by: actor.clone(),
                child_created_at_ms: 0,
                revision: Some(RevisionSummary {
                    revision_no: RevisionNo(MAX_PUBLIC_INTEGER - MAX_COMMIT_OPERATIONS as u64),
                    size_bytes: log_ref.size_bytes,
                    revision_committed_by: actor.clone(),
                    revision_committed_at_ms: 0,
                }),
            },
            WalDelta::AppendFileRevision {
                delta_index: 7,
                inode_id: InodeId(3),
                parent_inode_id: ROOT_INODE_ID,
                name_key: NameKey::parse("log").expect("key"),
                display_name: DisplayName::parse("log").expect("name"),
                bind_position: DeltaPosition {
                    seq: head.seq,
                    delta_index: 6,
                },
                child_created_by: actor.clone(),
                child_created_at_ms: 0,
                revision_no: RevisionNo(MAX_PUBLIC_INTEGER - MAX_COMMIT_OPERATIONS as u64),
                content_ref: log_ref.clone(),
                layout: Some(loonfs_types::ContentLayout {
                    creator_namespace_id: namespace_id.clone(),
                    extents: vec![loonfs_types::ContentExtent {
                        content_id: log_ref.content_id.clone(),
                        object: loonfs_types::ExtentObject::Whole,
                        offset: 0,
                        length: log_ref.size_bytes,
                    }],
                }),
                append_offset: None,
            },
        ],
    );
    for kind in ["attributes", "copy", "merge", "put", "inline", "append"] {
        let inline = InlineContent::new(
            ContentId::generate(),
            bytes::Bytes::from(vec![
                0;
                loonfs_types::format::wal::MAX_WAL_INLINE_CONTENT_BYTES
            ]),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let operation_count = match kind {
            "put" | "inline" => 1,
            "append" => MAX_WAL_OBJECT_INLINE_CONTENT_BYTES / MAX_WAL_INLINE_CONTENT_BYTES,
            _ => MAX_COMMIT_OPERATIONS,
        };
        let request = CommitIntent {
            commit_id: CommitId::parse("c".repeat(MAX_ID_BYTES)).expect("commit"),
            actor_id: actor.clone(),
            subject: None,
            message: Some("m".repeat(MAX_COMMIT_MESSAGE_BYTES)),
            preconditions: Vec::new(),
            operations: (0..operation_count)
                .map(|index| match kind {
                    "attributes" => FilesystemOperation::UpdateAttributes {
                        path: AbsolutePath::parse("/source").expect("path"),
                        set: BTreeMap::from([(
                            attribute_key("x"),
                            attribute_text(if index % 2 == 0 { "b" } else { "a" }),
                        )]),
                        remove: Vec::new(),
                        expected_inode_id: None,
                        expected_attributes_revision_no: None,
                    },
                    "copy" | "merge" => FilesystemOperation::CopyPath {
                        source_path: AbsolutePath::parse("/source").expect("path"),
                        destination_path: AbsolutePath::parse(format!("/{index:0255}"))
                            .expect("path"),
                        precondition: DestinationPrecondition::default(),
                    },
                    "append" => FilesystemOperation::AppendFile {
                        path: AbsolutePath::parse("/log").expect("path"),
                        inline_content: vec![0; MAX_WAL_INLINE_CONTENT_BYTES],
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                    _ => FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!(
                            "/{}{index:0255}",
                            "d/".repeat(loonfs_types::MAX_PATH_DEPTH - 1)
                        ))
                        .expect("path"),
                        content: loonfs_types::FileContent::Ref {
                            content_ref: if kind == "inline" {
                                inline.content_ref().clone()
                            } else {
                                content_ref.clone()
                            },
                            source_namespace_id: None,
                        },
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                })
                .collect(),
        };
        let baseline = CommitCandidate::new(request.clone());
        let baseline_bound = baseline.wal_record_bytes_upper_bound();
        let candidate = if kind == "merge" {
            baseline.with_merge(
                MergeRecord {
                    source_namespace_id: NamespaceId::parse("s".repeat(MAX_ID_BYTES))
                        .expect("namespace"),
                    source_after_seq: ChangeSeq(MAX_PUBLIC_INTEGER - 1),
                    source_through_seq: ChangeSeq(MAX_PUBLIC_INTEGER),
                },
                ChangeSeq(MAX_PUBLIC_INTEGER - 1),
                (0..operation_count)
                    .map(|index| {
                        let id = InodeId::generate(ChangeSeq(1), std::num::NonZeroU32::MIN);
                        (index, id)
                    })
                    .collect(),
            )
        } else if kind == "inline" {
            let candidate =
                CommitCandidate::with_inline_content(request, Vec::new(), vec![inline.clone()]);
            assert!(
                candidate.estimated_retained_bytes()
                    >= baseline.estimated_retained_bytes() + inline.bytes().len()
            );
            assert!(
                candidate.wal_record_bytes_upper_bound()
                    >= baseline.wal_record_bytes_upper_bound() + inline.bytes().len()
            );
            candidate
        } else {
            baseline
        };
        candidate.validate_request_limits().expect("request limits");
        let estimate = candidate.wal_record_bytes_upper_bound() + WAL_OBJECT_OVERHEAD_BYTES;
        assert!(estimate <= MAX_WAL_OBJECT_BYTES, "{kind}");
        let tail = crate::wal::ProjectedWalTail::default();
        let base = crate::wal::ProjectedWalTail::from_rows(state.clone());
        let content_writes = crate::test_support::content_writes();
        let manifest = VerifiedMetadataSegments::initial(&store);
        let session = PublishPlanningSession::new(
            &head,
            &tail,
            MetadataView::in_memory(&manifest, &base, None, head.seq),
            ContentWrites::new(&store, &content_writes, &head),
            u64::MAX,
        );
        let plan = session
            .prepare_commit(
                &candidate,
                crate::authorize::Authorizer::Unrestricted,
                candidate.semantic_identity(&namespace_id),
            )
            .await
            .expect("plan");
        let estimate = candidate.planned_wal_record_bytes(&plan.deltas, &plan.appended)
            + WAL_OBJECT_OVERHEAD_BYTES;
        let without_merge = plan.merge.is_some().then(|| CommitPlan {
            merge: None,
            ..plan.clone()
        });
        let encoded = prepare_wal_object(
            namespace_id.clone(),
            head.writer_epoch,
            &head,
            head.basis_floor_seq,
            &[plan],
        )
        .expect("encode");
        assert!(
            encoded.document_bytes() <= estimate,
            "{kind}: {} > {estimate}",
            encoded.document_bytes()
        );
        if let Some(without_merge) = without_merge {
            let merge_bytes = encoded.document_bytes()
                - prepare_wal_object(
                    namespace_id.clone(),
                    head.writer_epoch,
                    &head,
                    head.basis_floor_seq,
                    &[without_merge],
                )
                .expect("encode without the merge record")
                .document_bytes();
            assert!(
                candidate.wal_record_bytes_upper_bound() >= baseline_bound + merge_bytes,
                "{} < {baseline_bound} + {merge_bytes}",
                candidate.wal_record_bytes_upper_bound()
            );
        }
    }
}

/// Checks that a merge's bound includes the layout that its revision delta
/// records for each content reference, from the merge's proofs, so the bound
/// is the merge's planned WAL bytes.
#[test]
fn a_merges_bound_includes_the_layouts_of_its_proofs() {
    let main = NamespaceId::parse("main").expect("namespace");
    let fork = NamespaceId::parse("main-fork").expect("namespace");
    let content_ref = ContentRef::blob_v1(
        ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content id"),
        b"merged",
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let layout = loonfs_types::ContentLayout {
        creator_namespace_id: fork.clone(),
        extents: (0..3)
            .map(|offset| loonfs_types::ContentExtent {
                content_id: content_ref.content_id.clone(),
                object: loonfs_types::ExtentObject::Whole,
                offset,
                length: 1,
            })
            .collect(),
    };
    let intent = crate::path::write::CommitIntent::single(
        CommitId::parse("merge").expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateFileByInode {
            parent_inode_id: ROOT_INODE_ID,
            display_name: DisplayName::parse("merged.txt").expect("name"),
            content: loonfs_types::FileContent::Ref {
                content_ref: content_ref.clone(),
                source_namespace_id: None,
            },
        },
    );
    let merge = |proofs| {
        CommitCandidate::prepared(intent.clone(), proofs).with_merge(
            MergeRecord {
                source_namespace_id: fork.clone(),
                source_after_seq: ChangeSeq(3),
                source_through_seq: ChangeSeq(7),
            },
            ChangeSeq(3),
            BTreeMap::from([(
                0,
                InodeId::generate(ChangeSeq(4), std::num::NonZeroU32::MIN),
            )]),
        )
    };
    let proof = crate::storage::content_admission::PreparedContent::for_family_member(
        main,
        content_ref,
        layout,
        u64::MAX,
    );
    assert_eq!(
        merge(vec![proof]).wal_record_bytes_upper_bound(),
        merge(Vec::new()).wal_record_bytes_upper_bound() + layout_bytes(3)
    );
}
