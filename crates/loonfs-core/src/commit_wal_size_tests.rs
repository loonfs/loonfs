//! WAL size estimates checked against planned requests at the API limits.

use super::*;
use crate::commit::materialize_commit;
use crate::commit_engine::CommitCandidate;
use crate::limits::{MAX_COMMIT_MESSAGE_BYTES, MAX_COMMIT_OPERATIONS};
use crate::metadata::{InMemoryMetadataView, MetadataState};
use crate::namespace::state::NamespaceReadState;
use crate::path::write::PublishPlanningSession;
use crate::storage::inline_content::InlineContent;
use crate::wal::prepare_wal_segment;
use loonfs_api::wire::wal::{WalDelta, MAX_WAL_SEGMENT_BYTES, WAL_SEGMENT_OVERHEAD_BYTES};
use loonfs_api::{
    ActorId, AttributeKey, AttributeRevisionNo, Attributes, ChangeSeq, Checksum, CommitId,
    ContentId, ContentRef, ContentRefKind, ContentStoreId, DestinationBehavior,
    DestinationPrecondition, DisplayName, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo,
    WalNo, WriterEpoch, MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES, MAX_PUBLIC_INTEGER,
};
use loonfs_test_support::ids::{attribute_key, attribute_text};
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
    let namespace_id = NamespaceId::parse("n".repeat(MAX_ID_BYTES)).expect("namespace");
    let actor = ActorId::parse("a".repeat(256)).expect("actor");
    let mut head = NamespaceReadState::initial(
        namespace_id.clone(),
        ContentStoreId::generate(),
        0,
        loonfs_test_support::test_actor(),
    );
    head.seq = ChangeSeq(MAX_PUBLIC_INTEGER - 1);
    head.wal_no = WalNo(MAX_PUBLIC_INTEGER - 1);
    head.next_inode_id = InodeId(MAX_PUBLIC_INTEGER - 1_000_000);
    head.writer_epoch = WriterEpoch(MAX_PUBLIC_INTEGER);
    let content_ref = ContentRef {
        kind: ContentRefKind::BlobV1,
        owner_namespace_id: namespace_id.clone(),
        content_id: ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
        size_bytes: u64::MAX,
        checksum: Checksum::sha256(b"content"),
    };
    let mut state = MetadataState::default();
    state.apply_committed_wal_deltas_mut(
        head.seq,
        &CommitId::parse("seed").expect("commit"),
        &actor,
        0,
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
            },
            WalDelta::CreateInode {
                delta_index: 1,
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
            },
            WalDelta::BindDirentry {
                delta_index: 2,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("source").expect("key"),
                display_name: DisplayName::parse("source").expect("name"),
                child_inode_id: InodeId(2),
            },
            WalDelta::AppendFileRevision {
                delta_index: 3,
                inode_id: InodeId(2),
                revision_no: RevisionNo(MAX_PUBLIC_INTEGER - 1),
                content_ref: content_ref.clone(),
            },
            WalDelta::AppendAttributesRevision {
                delta_index: 4,
                inode_id: InodeId(2),
                attributes_revision_no: AttributeRevisionNo(
                    MAX_PUBLIC_INTEGER - MAX_COMMIT_OPERATIONS as u64,
                ),
                attributes: full_attributes(),
            },
        ],
    );
    for kind in ["attributes", "copy", "put", "inline"] {
        let inline = InlineContent::new(
            namespace_id.clone(),
            ContentId::generate(),
            bytes::Bytes::from(vec![0; loonfs_api::wire::wal::MAX_WAL_INLINE_CONTENT_BYTES]),
        );
        let operation_count = if kind == "put" || kind == "inline" {
            1
        } else {
            MAX_COMMIT_OPERATIONS
        };
        let request = CommitRequest {
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
                    "copy" => FilesystemOperation::CopyPath {
                        source_path: AbsolutePath::parse("/source").expect("path"),
                        destination_path: AbsolutePath::parse(format!("/{index:0255}"))
                            .expect("path"),
                        precondition: DestinationPrecondition::default(),
                    },
                    _ => FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!(
                            "/{}{index:0255}",
                            "d/".repeat(loonfs_api::MAX_PATH_DEPTH - 1)
                        ))
                        .expect("path"),
                        content_ref: Some(if kind == "inline" {
                            inline.content_ref().clone()
                        } else {
                            content_ref.clone()
                        }),
                        inline_content: None,
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                })
                .collect(),
        };
        let baseline = CommitCandidate::new(request.clone());
        let candidate = if kind == "inline" {
            let candidate =
                CommitCandidate::with_inline_content(request, Vec::new(), vec![inline.clone()]);
            assert!(
                candidate.estimated_retained_bytes().expect("weight")
                    >= baseline
                        .estimated_retained_bytes()
                        .expect("baseline weight")
                        + inline.bytes().len()
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
        let estimate = candidate.wal_record_bytes_upper_bound() + WAL_SEGMENT_OVERHEAD_BYTES;
        assert!(estimate <= MAX_WAL_SEGMENT_BYTES, "{kind}");
        let mut session = PublishPlanningSession::new(&head);
        let mut allocation = session.begin_candidate();
        let plan = session
            .prepare_commit(
                &candidate,
                candidate
                    .semantic_identity(&namespace_id)
                    .expect("fingerprint"),
                InMemoryMetadataView::in_memory(&state, None, head.seq),
                u64::MAX,
                &mut allocation,
            )
            .await
            .expect("plan");
        let next_inode_id = session.commit_candidate(allocation).expect("allocation");
        let materialized = materialize_commit(
            plan.finish(next_inode_id),
            u64::MAX,
            candidate.inline_content(),
        );
        let encoded = prepare_wal_segment(
            namespace_id.clone(),
            head.writer_epoch,
            &head,
            &[materialized],
        )
        .expect("encode");
        assert!(
            encoded.document_len() <= estimate,
            "{kind}: {} > {estimate}",
            encoded.document_len()
        );
    }
}
