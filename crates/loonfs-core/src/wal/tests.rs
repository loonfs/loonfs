//! Behavior tests for WAL segment preparation and validated chain loading.

use super::*;
use crate::commit::{
    materialize_commit, wal_payload_from_materialized_commit, CommitPlan, CommitRequest,
    MaterializedCommit, PreparedCommit, ValidatedOp,
};
use bytes::Bytes;
use loonfs_api::v0::CommitOp;
use loonfs_api::wire::control::WalSegmentPointer;
use loonfs_api::wire::wal::{encode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, CommitId, InodeId, NameKey, NamespaceId, WalSegmentId, WriterEpoch};
use loonfs_objectstore::keys::wal_segment;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use std::borrow::Cow;
use tempfile::tempdir;

#[tokio::test]
async fn build_wal_record_payload_matches_segment_record_payload() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let request = CommitRequest {
        namespace_id: namespace_id.clone(),
        commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
        }],
        preconditions: Vec::new(),
        message: Some("create docs".to_owned()),
    };
    let plan = CommitPlan {
        namespace_id: namespace_id.clone(),
        commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
        apply_after_seq: ChangeSeq(0),
        assigned_seq: ChangeSeq(1),
        validated_ops: vec![ValidatedOp::CreateDir {
            op_index: 0,
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            name_key: NameKey::parse("docs").expect("valid name key"),
            child_inode_id: InodeId(2),
            create_inode_delta_index: 0,
            bind_delta_index: 1,
        }],
        resulting_next_inode_id: InodeId(3),
        checked_invariants: Vec::new(),
    };
    let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
    let record = materialize_commit(prepared, 4_200);

    let segment = prepare_wal_segment(
        namespace_id,
        WriterEpoch(1),
        None,
        std::slice::from_ref(&record),
        "test-writer",
    )
    .expect("prepare wal segment");
    let payload = wal_payload_from_materialized_commit(&record).expect("build commit payload");

    assert_eq!(payload, segment.envelope.payload.records[0]);
}

#[tokio::test]
async fn prepared_wal_segments_use_unique_segment_ids_and_object_keys() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let record = materialized_create_directory(
        &namespace_id,
        "c_wal_unique",
        "docs",
        ChangeSeq(0),
        ChangeSeq(1),
    );

    let first = prepare_wal_segment(
        namespace_id.clone(),
        WriterEpoch(1),
        None,
        std::slice::from_ref(&record),
        "test-writer",
    )
    .expect("prepare first wal segment");
    let second = prepare_wal_segment(
        namespace_id,
        WriterEpoch(1),
        None,
        std::slice::from_ref(&record),
        "test-writer",
    )
    .expect("prepare second wal segment");

    assert_ne!(first.segment_id, second.segment_id);
    assert_ne!(first.object_key, second.object_key);
    WalSegmentId::parse(first.segment_id.as_str()).expect("first segment id shape");
    WalSegmentId::parse(second.segment_id.as_str()).expect("second segment id shape");
}

#[test]
fn wal_segment_namespace_mismatch_names_record_and_segment_values() {
    let record_namespace = NamespaceId::parse("record").expect("valid namespace id");
    let segment_namespace = NamespaceId::parse("segment").expect("valid namespace id");
    let record = materialized_create_directory(
        &record_namespace,
        "c_wal_namespace_mismatch",
        "docs",
        ChangeSeq(0),
        ChangeSeq(1),
    );

    let error = prepare_wal_segment(
        segment_namespace,
        WriterEpoch(1),
        None,
        &[record],
        "test-writer",
    )
    .expect_err("namespace mismatch should fail");

    assert_eq!(
        error.to_string(),
        "WAL segment namespace mismatch: record `record`, segment `segment`"
    );
}

#[tokio::test]
async fn validated_wal_chain_loads_visible_segments_in_ascending_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let segment = write_create_directory_segment(
        &store,
        &namespace_id,
        None,
        "c_wal_chain_a",
        "alpha",
        ChangeSeq(0),
        ChangeSeq(1),
    )
    .await;

    let chain = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(1),
            visible_tip: Some(segment.envelope.pointer(segment.object_key.clone())),
            stop_after_seq: None,
            recent_segments: &[],
        },
    )
    .await
    .expect("load valid chain");

    assert_eq!(chain.segments().len(), 1);
    assert_eq!(chain.segments()[0].records()[0].seq, ChangeSeq(1));
}

#[test]
fn canonical_replay_advances_head_and_applies_metadata_rows() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let commit = materialized_create_directory(
        &namespace_id,
        "c_wal_replay",
        "docs",
        ChangeSeq(0),
        ChangeSeq(1),
    );
    let segment = prepare_wal_segment(
        namespace_id.clone(),
        WriterEpoch(1),
        None,
        std::slice::from_ref(&commit),
        "test-writer",
    )
    .expect("prepare wal segment");
    let mut base_head = loonfs_api::wire::control::HeadState::initial(namespace_id.clone());
    base_head.writer_epoch = WriterEpoch(1);
    let record = segment
        .envelope
        .payload
        .records
        .first()
        .expect("wal record");

    let replayed = replay::replay_wal_records(
        &base_head,
        &crate::metadata::MetadataState::default(),
        Some(WriterEpoch(1)),
        [DecodedWalRecord {
            namespace_id: &namespace_id,
            seq: record.seq,
            writer_epoch: segment.envelope.payload.writer_epoch,
            commit_id: &record.commit_id,
            committed_at_ms: record.committed_at_ms,
            semantic_commit_fingerprint: &record.semantic_commit_fingerprint,
            message: record.message.as_deref(),
            deltas: Cow::Borrowed(&record.deltas),
        }],
    )
    .expect("replay wal records");

    assert_eq!(replayed.resulting_head.seq, ChangeSeq(1));
    assert_eq!(replayed.resulting_head.head_commit_id, record.commit_id);
    assert_eq!(replayed.resulting_head.next_inode_id, InodeId(3));
    assert!(replayed
        .resulting_metadata_state
        .inode_at_head(InodeId(2))
        .is_some());
    assert!(replayed
        .resulting_metadata_state
        .find_commit_receipt(&record.commit_id)
        .is_some());
}

#[test]
fn canonical_replay_rejects_writer_epoch_above_expected_bound() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let commit = materialized_create_directory(
        &namespace_id,
        "c_wal_replay_epoch",
        "docs",
        ChangeSeq(0),
        ChangeSeq(1),
    );
    let segment = prepare_wal_segment(
        namespace_id.clone(),
        WriterEpoch(2),
        None,
        std::slice::from_ref(&commit),
        "test-writer",
    )
    .expect("prepare wal segment");
    let mut base_head = loonfs_api::wire::control::HeadState::initial(namespace_id.clone());
    base_head.writer_epoch = WriterEpoch(1);
    let record = segment
        .envelope
        .payload
        .records
        .first()
        .expect("wal record");

    let error = replay::replay_wal_records(
        &base_head,
        &crate::metadata::MetadataState::default(),
        Some(WriterEpoch(1)),
        [DecodedWalRecord {
            namespace_id: &namespace_id,
            seq: record.seq,
            writer_epoch: segment.envelope.payload.writer_epoch,
            commit_id: &record.commit_id,
            committed_at_ms: record.committed_at_ms,
            semantic_commit_fingerprint: &record.semantic_commit_fingerprint,
            message: record.message.as_deref(),
            deltas: Cow::Borrowed(&record.deltas),
        }],
    )
    .expect_err("future writer epoch should fail");

    assert_eq!(
        error,
        WalReplayError::WriterEpochMismatch {
            expected_max: WriterEpoch(1),
            actual: WriterEpoch(2),
        }
    );
}

#[tokio::test]
async fn validated_wal_chain_can_load_cursor_suffix_without_full_base() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let first = write_create_directory_segment(
        &store,
        &namespace_id,
        None,
        "c_wal_suffix_a",
        "alpha",
        ChangeSeq(0),
        ChangeSeq(1),
    )
    .await;
    let second = write_create_directory_segment(
        &store,
        &namespace_id,
        Some(first.envelope.pointer(first.object_key.clone())),
        "c_wal_suffix_b",
        "beta",
        ChangeSeq(1),
        ChangeSeq(2),
    )
    .await;

    let chain = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(2),
            visible_tip: Some(second.envelope.pointer(second.object_key.clone())),
            stop_after_seq: Some(ChangeSeq(1)),
            recent_segments: &[],
        },
    )
    .await
    .expect("load suffix chain");

    assert_eq!(chain.segments().len(), 1);
    assert_eq!(chain.segments()[0].records()[0].seq, ChangeSeq(2));
}

#[tokio::test]
async fn validated_wal_chain_reports_missing_previous_link_truthfully() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let segment = write_create_directory_segment(
        &store,
        &namespace_id,
        None,
        "c_wal_broken_link",
        "docs",
        ChangeSeq(1),
        ChangeSeq(2),
    )
    .await;

    let error = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(2),
            visible_tip: Some(segment.envelope.pointer(segment.object_key.clone())),
            stop_after_seq: None,
            recent_segments: &[],
        },
    )
    .await
    .expect_err("missing previous segment link should fail");

    assert_eq!(
        error.to_string(),
        format!(
            "wal replay validation failed: WAL segment `{}` is missing its previous visible segment link before seq `0`",
            segment.object_key
        )
    );
}

#[tokio::test]
async fn validated_wal_chain_rejects_corrupt_visible_segments() {
    assert_wal_chain_corruption_rejected(|object_key, _envelope, pointer| {
        *object_key = wal_segment("other", "seg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        pointer.object_key = object_key.clone();
    })
    .await;
    assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
        envelope.payload.namespace_id = NamespaceId::parse("other").expect("valid namespace id");
        rewrap_envelope(envelope);
        *object_key = wal_segment(
            envelope.payload.namespace_id.as_str(),
            envelope.payload.segment_id.as_str(),
        );
        *pointer = envelope.pointer(object_key.clone());
    })
    .await;
    assert_wal_chain_corruption_rejected(|_object_key, envelope, _pointer| {
        envelope.payload.segment_id =
            WalSegmentId::parse("00000000000000000001-bbbbbbbbbbbbbbbb").expect("valid segment id");
        rewrap_envelope(envelope);
    })
    .await;
    assert_wal_chain_corruption_rejected(|_object_key, _envelope, pointer| {
        pointer.payload_checksum = "sha256:not-the-payload".to_owned();
    })
    .await;
    assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
        envelope.payload.records.clear();
        rewrap_envelope(envelope);
        *pointer = envelope.pointer(object_key.clone());
    })
    .await;
    assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
        envelope.payload.end_seq = ChangeSeq(2);
        rewrap_envelope(envelope);
        *object_key = wal_segment(
            envelope.payload.namespace_id.as_str(),
            envelope.payload.segment_id.as_str(),
        );
        *pointer = envelope.pointer(object_key.clone());
    })
    .await;
    assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
        let mut skipped = envelope.payload.records[0].clone();
        skipped.seq = ChangeSeq(3);
        envelope.payload.records.push(skipped);
        envelope.payload.end_seq = ChangeSeq(3);
        rewrap_envelope(envelope);
        *object_key = wal_segment(
            envelope.payload.namespace_id.as_str(),
            envelope.payload.segment_id.as_str(),
        );
        *pointer = envelope.pointer(object_key.clone());
    })
    .await;
    assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
        envelope.payload.base_head_seq = ChangeSeq(1);
        envelope.payload.start_seq = ChangeSeq(2);
        envelope.payload.end_seq = ChangeSeq(2);
        envelope.payload.records[0].seq = ChangeSeq(2);
        rewrap_envelope(envelope);
        *object_key = wal_segment(
            envelope.payload.namespace_id.as_str(),
            envelope.payload.segment_id.as_str(),
        );
        *pointer = envelope.pointer(object_key.clone());
    })
    .await;
}

fn materialized_create_directory(
    namespace_id: &NamespaceId,
    commit_id: &str,
    display_name: &str,
    apply_after_seq: ChangeSeq,
    assigned_seq: ChangeSeq,
) -> MaterializedCommit {
    let request = CommitRequest {
        namespace_id: namespace_id.clone(),
        commit_id: CommitId::parse(commit_id).expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse(display_name).expect("valid display name"),
        }],
        preconditions: Vec::new(),
        message: None,
    };
    let plan = CommitPlan {
        namespace_id: namespace_id.clone(),
        commit_id: CommitId::parse(commit_id).expect("valid commit id"),
        apply_after_seq,
        assigned_seq,
        validated_ops: vec![ValidatedOp::CreateDir {
            op_index: 0,
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse(display_name).expect("valid display name"),
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(
                loonfs_api::NamePolicy::default(),
                display_name,
            ))
            .expect("derived name key"),
            child_inode_id: InodeId(2),
            create_inode_delta_index: 0,
            bind_delta_index: 1,
        }],
        resulting_next_inode_id: InodeId(3),
        checked_invariants: Vec::new(),
    };
    let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
    materialize_commit(prepared, 4_200)
}

async fn assert_wal_chain_corruption_rejected(
    corrupt: impl FnOnce(&mut String, &mut WalSegmentEnvelope, &mut WalSegmentPointer),
) {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let segment = prepare_wal_segment(
        namespace_id.clone(),
        WriterEpoch(1),
        None,
        &[materialized_create_directory(
            &namespace_id,
            "c_wal_corrupt",
            "docs",
            ChangeSeq(0),
            ChangeSeq(1),
        )],
        "test-writer",
    )
    .expect("prepare wal segment");
    let mut object_key = segment.object_key;
    let mut envelope = segment.envelope;
    let mut pointer = envelope.pointer(object_key.clone());

    corrupt(&mut object_key, &mut envelope, &mut pointer);

    let encoded = encode_wal_segment_envelope_zstd(&envelope).expect("encode corrupted envelope");
    store
        .put_if_absent(&object_key, Bytes::from(encoded))
        .await
        .expect("write corrupted wal segment");

    load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: pointer.end_seq,
            visible_tip: Some(pointer),
            stop_after_seq: None,
            recent_segments: &[],
        },
    )
    .await
    .expect_err("corrupted WAL chain should be rejected");
}

#[tokio::test]
async fn chain_load_with_recent_segment_hints_matches_the_unhinted_chain() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let first = write_create_directory_segment(
        &store,
        &namespace_id,
        None,
        "c_wal_hint_a",
        "alpha",
        ChangeSeq(0),
        ChangeSeq(1),
    )
    .await;
    let second = write_create_directory_segment(
        &store,
        &namespace_id,
        Some(first.envelope.pointer(first.object_key.clone())),
        "c_wal_hint_b",
        "beta",
        ChangeSeq(1),
        ChangeSeq(2),
    )
    .await;
    let unhinted = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(2),
            visible_tip: Some(second.envelope.pointer(second.object_key.clone())),
            stop_after_seq: None,
            recent_segments: &[],
        },
    )
    .await
    .expect("unhinted chain");

    // Accurate hints (newest first, tip included) prefetch the gap.
    let accurate = [
        second.envelope.pointer(second.object_key.clone()),
        first.envelope.pointer(first.object_key.clone()),
    ];
    let hinted = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(2),
            visible_tip: Some(second.envelope.pointer(second.object_key.clone())),
            stop_after_seq: None,
            recent_segments: &accurate,
        },
    )
    .await
    .expect("hinted chain");
    assert_eq!(hinted.segments(), unhinted.segments());

    // Garbage hints (missing objects, lying seq ranges) cost fallback
    // fetches, never correctness: chain links stay the authority.
    let mut lying_tip = second.envelope.pointer(second.object_key.clone());
    lying_tip.end_seq = ChangeSeq(999);
    let missing_segment_id =
        WalSegmentId::parse("00000000000000000001-00000000deadbeef").expect("valid segment id");
    let garbage = [
        WalSegmentPointer {
            object_key: wal_segment(namespace_id.as_str(), missing_segment_id.as_str()),
            segment_id: missing_segment_id,
            start_seq: ChangeSeq(1),
            end_seq: ChangeSeq(1),
            payload_checksum: "sha256:absent".to_owned(),
        },
        lying_tip,
    ];
    let survived = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(2),
            visible_tip: Some(second.envelope.pointer(second.object_key.clone())),
            stop_after_seq: None,
            recent_segments: &garbage,
        },
    )
    .await
    .expect("chain despite garbage hints");
    assert_eq!(survived.segments(), unhinted.segments());
}

async fn write_create_directory_segment(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    prev_visible_segment: Option<WalSegmentPointer>,
    commit_id: &str,
    display_name: &str,
    apply_after_seq: ChangeSeq,
    assigned_seq: ChangeSeq,
) -> PreparedWalSegment {
    let segment = prepare_wal_segment(
        namespace_id.clone(),
        WriterEpoch(1),
        prev_visible_segment,
        &[materialized_create_directory(
            namespace_id,
            commit_id,
            display_name,
            apply_after_seq,
            assigned_seq,
        )],
        "test-writer",
    )
    .expect("prepare wal segment");
    store
        .put_if_absent(
            &segment.object_key,
            Bytes::copy_from_slice(&segment.encoded_bytes),
        )
        .await
        .expect("write wal segment");
    segment
}

fn rewrap_envelope(envelope: &mut WalSegmentEnvelope) {
    *envelope =
        WalSegmentEnvelope::from_payload(envelope.writer_version.clone(), envelope.payload.clone())
            .expect("rewrap wal envelope");
}
