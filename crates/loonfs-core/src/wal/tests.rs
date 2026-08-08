//! Behavior tests for WAL segment preparation and validated chain loading.

#![allow(clippy::panic)]

use super::reader::RECENT_SEGMENT_PREFETCH_CONCURRENCY;
use super::*;
use crate::commit::{
    materialize_commit, wal_payload_from_materialized_commit, CommitFingerprint, CommitIr,
    CommitOp, CommitPlan, MaterializedCommit, PlannedOp, PreparedCommit, ValidatedOp,
};
use bytes::Bytes;
use loonfs_api::wire::control::WalSegmentPointer;
use loonfs_api::wire::wal::{encode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, CommitId, InodeId, NameKey, NamespaceId, WalSegmentId, WriterEpoch};
use loonfs_objectstore::keys::{wal_segment, wal_segment_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{
    ConcurrencyWatchStore, CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
    RecordingStore,
};
use std::borrow::Cow;
use tempfile::tempdir;

fn test_fingerprint() -> CommitFingerprint {
    CommitFingerprint::new_unchecked("v0:sha256:test".to_owned())
}

#[tokio::test]
async fn build_wal_record_payload_matches_segment_record_payload() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let request = CommitIr {
        namespace_id: namespace_id.clone(),
        commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
        writer_epoch: WriterEpoch(1),
        ops: vec![PlannedOp::unchecked(CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
        })],
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
    };
    let prepared = PreparedCommit::new(request, plan, test_fingerprint()).expect("prepare commit");
    let record = materialize_commit(prepared, 4_200);

    let segment = prepare_wal_segment(
        namespace_id,
        WriterEpoch(1),
        None,
        std::slice::from_ref(&record),
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
    )
    .expect("prepare first wal segment");
    let second = prepare_wal_segment(
        namespace_id,
        WriterEpoch(1),
        None,
        std::slice::from_ref(&record),
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

    let error = prepare_wal_segment(segment_namespace, WriterEpoch(1), None, &[record])
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
    )
    .expect("prepare wal segment");
    let mut base_head = loonfs_api::wire::control::HeadState::initial(
        namespace_id.clone(),
        loonfs_api::ContentStoreId::generate(),
    );
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
    )
    .expect("prepare wal segment");
    let mut base_head = loonfs_api::wire::control::HeadState::initial(
        namespace_id.clone(),
        loonfs_api::ContentStoreId::generate(),
    );
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
    let request = CommitIr {
        namespace_id: namespace_id.clone(),
        commit_id: CommitId::parse(commit_id).expect("valid commit id"),
        writer_epoch: WriterEpoch(1),
        ops: vec![PlannedOp::unchecked(CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse(display_name).expect("valid display name"),
        })],
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
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(display_name))
                .expect("derived name key"),
            child_inode_id: InodeId(2),
            create_inode_delta_index: 0,
            bind_delta_index: 1,
        }],
        resulting_next_inode_id: InodeId(3),
    };
    let prepared = PreparedCommit::new(request, plan, test_fingerprint()).expect("prepare commit");
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

/// Writes a chain of `length` linked segments and returns their pointers,
/// oldest first.
async fn write_linked_chain(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    length: u64,
) -> Vec<WalSegmentPointer> {
    let mut pointers: Vec<WalSegmentPointer> = Vec::new();
    for index in 0..length {
        let segment = write_create_directory_segment(
            store,
            namespace_id,
            pointers.last().cloned(),
            &format!("c_wal_bounded_{index:02}"),
            &format!("dir-{index:02}"),
            ChangeSeq(index),
            ChangeSeq(index + 1),
        )
        .await;
        pointers.push(segment.envelope.pointer(segment.object_key.clone()));
    }
    pointers
}

/// A caller that meters its own reads caps the load, and the loader stops
/// once it has read that many segment bodies. It reports how many it read
/// so the caller can charge for exactly those.
#[tokio::test]
async fn a_bounded_chain_load_stops_at_its_fetch_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let pointers = write_linked_chain(store.inner(), &namespace_id, 5).await;
    let tip = pointers.last().expect("a chain was written").clone();

    // No hints, so every body comes from the serial walk down the links.
    store.reset();
    let bounded = load_wal_chain_within(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(5),
            visible_tip: Some(tip.clone()),
            stop_after_seq: None,
            recent_segments: &[],
        },
        2,
    )
    .await
    .expect("bounded chain load");
    match bounded {
        WalChainLoad::Complete { .. } => panic!("a five-segment chain does not fit in two fetches"),
        WalChainLoad::LimitReached { requests_issued } => assert_eq!(requests_issued, 2),
    }
    assert_eq!(
        store.count(OperationClass::Get),
        2,
        "the walk reads no more bodies than the limit allows"
    );

    // A hint list longer than the limit is cut down to what the limit
    // covers rather than refused. The prefetch takes the newest hints,
    // which is where the walk starts, and the walk stops once the limit is
    // spent — reporting what it spent, not zero.
    let mut newest_first = pointers.clone();
    newest_first.reverse();
    store.reset();
    let hinted = load_wal_chain_within(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(5),
            visible_tip: Some(tip.clone()),
            stop_after_seq: None,
            recent_segments: &newest_first,
        },
        2,
    )
    .await
    .expect("bounded hinted load");
    match hinted {
        WalChainLoad::Complete { .. } => panic!("five hints do not fit in two fetches"),
        WalChainLoad::LimitReached { requests_issued } => assert_eq!(requests_issued, 2),
    }
    assert_eq!(
        store.count(OperationClass::Get),
        2,
        "the prefetch issued the requests the limit allows, not none and not five"
    );
}

/// A limit that covers the chain exactly does not truncate it. The load
/// completes and matches what an unbounded load returns.
#[tokio::test]
async fn a_limit_that_covers_the_chain_loads_all_of_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let pointers = write_linked_chain(store.inner(), &namespace_id, 5).await;
    let tip = pointers.last().expect("a chain was written").clone();
    let request = |recent: &'static [WalSegmentPointer]| WalChainLoadRequest {
        namespace_id: &namespace_id,
        chain_base_seq: ChangeSeq(0),
        head_seq: ChangeSeq(5),
        visible_tip: Some(tip.clone()),
        stop_after_seq: None,
        recent_segments: recent,
    };
    let unbounded = load_validated_wal_chain(&store, request(&[]))
        .await
        .expect("unbounded chain");

    store.reset();
    let bounded = load_wal_chain_within(&store, request(&[]), 5)
        .await
        .expect("bounded chain load");
    match bounded {
        WalChainLoad::Complete {
            chain,
            requests_issued,
        } => {
            assert_eq!(chain.segments(), unbounded.segments());
            assert_eq!(requests_issued, 5);
        }
        WalChainLoad::LimitReached { requests_issued } => {
            panic!("a five-segment chain fits in five fetches, not {requests_issued}")
        }
    }
    assert_eq!(store.count(OperationClass::Get), 5);
}

/// A prefetch request that fails is a request the store answered, so it
/// counts like any other. The walk fetches that segment itself and the
/// chain still loads. The number the load reports is the number of requests
/// it issued: one failed prefetch and one walk fetch for each of the three
/// segments, which is what the counting wrapper saw.
#[tokio::test]
async fn a_failed_prefetch_costs_its_own_request_and_the_walk_loads_the_chain() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let pointers = write_linked_chain(&inner, &namespace_id, 3).await;
    let tip = pointers.last().expect("a chain was written").clone();
    let mut newest_first = pointers.clone();
    newest_first.reverse();

    // The prefetch completes before the walk fetches anything, so arming
    // one failure per hint fails every prefetch request and leaves the
    // walk's own fetches to succeed.
    let failing = FailStore::new(
        inner,
        KeyPredicate::prefix(wal_segment_prefix(namespace_id.as_str())),
        OperationClass::Get,
        InjectedError::Transport("the store is flaky".to_owned()),
    );
    failing.fail_next(newest_first.len());
    let store = CountingStore::new(failing, KeyPredicate::any());

    let loaded = load_wal_chain_within(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(3),
            visible_tip: Some(tip),
            stop_after_seq: None,
            recent_segments: &newest_first,
        },
        16,
    )
    .await
    .expect("the walk fetches what the prefetch could not deliver");

    let issued = store.count(OperationClass::Get);
    assert_eq!(
        issued, 6,
        "three failed prefetch requests and three walk fetches"
    );
    match loaded {
        WalChainLoad::Complete {
            chain,
            requests_issued,
        } => {
            assert_eq!(chain.segments().len(), 3);
            assert_eq!(
                requests_issued, issued,
                "the load reports the requests it issued"
            );
        }
        WalChainLoad::LimitReached { requests_issued } => {
            panic!("three segments fit in sixteen requests, not {requests_issued}")
        }
    }
}

/// The limit bounds every request the load issues, prefetch included. A
/// store that fails every prefetch cannot make the load exceed it: the walk
/// stops once the failures have spent the limit.
#[tokio::test]
async fn failed_prefetch_requests_count_against_the_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let pointers = write_linked_chain(&inner, &namespace_id, 3).await;
    let tip = pointers.last().expect("a chain was written").clone();
    let mut newest_first = pointers.clone();
    newest_first.reverse();

    let failing = FailStore::new(
        inner,
        KeyPredicate::prefix(wal_segment_prefix(namespace_id.as_str())),
        OperationClass::Get,
        InjectedError::Transport("the store is flaky".to_owned()),
    );
    failing.fail_all();
    let store = CountingStore::new(failing, KeyPredicate::any());

    let loaded = load_wal_chain_within(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(3),
            visible_tip: Some(tip),
            stop_after_seq: None,
            recent_segments: &newest_first,
        },
        3,
    )
    .await
    .expect("a load whose limit the failures spent");

    match loaded {
        WalChainLoad::Complete { .. } => panic!("no body was ever delivered"),
        WalChainLoad::LimitReached { requests_issued } => assert_eq!(requests_issued, 3),
    }
    assert_eq!(
        store.count(OperationClass::Get),
        3,
        "the limit bounds prefetch requests exactly as it bounds walk fetches"
    );
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
        WalSegmentEnvelope::from_payload(envelope.payload.clone()).expect("rewrap wal envelope");
}

fn prepared_create_directory_segment(
    namespace_id: &NamespaceId,
    prev_visible_segment: Option<WalSegmentPointer>,
    commit_id: &str,
    display_name: &str,
    apply_after_seq: ChangeSeq,
    assigned_seq: ChangeSeq,
) -> PreparedWalSegment {
    prepare_wal_segment(
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
    )
    .expect("prepare wal segment")
}

/// Prepares a linked chain of `len` single-commit segments covering seqs
/// `1..=len`, without writing any of them to a store.
fn prepared_unstored_chain(namespace_id: &NamespaceId, len: u64) -> Vec<PreparedWalSegment> {
    let mut chain: Vec<PreparedWalSegment> = Vec::new();
    for seq in 1..=len {
        let prev = chain
            .last()
            .map(|segment| segment.envelope.pointer(segment.object_key.clone()));
        chain.push(prepared_create_directory_segment(
            namespace_id,
            prev,
            &format!("c_count_{seq}"),
            &format!("dir-{seq}"),
            ChangeSeq(seq - 1),
            ChangeSeq(seq),
        ));
    }
    chain
}

fn newest_first_pointers(chain: &[PreparedWalSegment]) -> Vec<WalSegmentPointer> {
    chain
        .iter()
        .rev()
        .map(|segment| segment.envelope.pointer(segment.object_key.clone()))
        .collect()
}

async fn store_chain<S: ObjectStore + ?Sized>(store: &S, chain: &[PreparedWalSegment]) {
    for segment in chain {
        store
            .put_if_absent(
                &segment.object_key,
                Bytes::copy_from_slice(&segment.encoded_bytes),
            )
            .await
            .expect("write wal segment");
    }
}

fn tail_count_request<'a>(
    namespace_id: &'a NamespaceId,
    chain_base_seq: ChangeSeq,
    head_seq: ChangeSeq,
    hints: &'a [WalSegmentPointer],
) -> WalChainLoadRequest<'a> {
    WalChainLoadRequest {
        namespace_id,
        chain_base_seq,
        head_seq,
        visible_tip: hints.first().cloned(),
        stop_after_seq: None,
        recent_segments: hints,
    }
}

/// The count takes no store at all, so "reads no bodies" is the signature
/// rather than an assertion — and it holds at the longest tail a landed
/// publish can leave behind, which is what the head is sized to describe.
#[test]
fn tail_count_from_contiguous_hints_covers_the_longest_legal_tail() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let boundary = crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS;
    let chain = prepared_unstored_chain(&namespace_id, boundary);
    let hints = newest_first_pointers(&chain);

    let count = count_visible_wal_tail_segments(tail_count_request(
        &namespace_id,
        ChangeSeq(0),
        ChangeSeq(boundary),
        &hints,
    ))
    .expect("count from pointers");
    assert_eq!(count, boundary);

    // A manifest boundary inside the hinted window is honored: only the
    // segments above it are tail.
    let count = count_visible_wal_tail_segments(tail_count_request(
        &namespace_id,
        ChangeSeq(boundary - 1),
        ChangeSeq(boundary),
        &hints,
    ))
    .expect("count above interior boundary");
    assert_eq!(count, 1);

    // An empty tail costs nothing and consults nothing.
    let count = count_visible_wal_tail_segments(tail_count_request(
        &namespace_id,
        ChangeSeq(boundary),
        ChangeSeq(boundary),
        &hints,
    ))
    .expect("count of empty tail");
    assert_eq!(count, 0);
}

/// A head that does not name its whole tail is corrupted. The count says so
/// instead of walking chain links one round trip at a time on an inspection
/// call.
#[test]
fn tail_count_rejects_a_head_that_does_not_describe_its_tail() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let chain = prepared_unstored_chain(&namespace_id, 4);
    let pointers = newest_first_pointers(&chain);
    let head_seq = ChangeSeq(4);

    // A run that stops short of the basis boundary.
    let error = count_visible_wal_tail_segments(tail_count_request(
        &namespace_id,
        ChangeSeq(0),
        head_seq,
        &pointers[..2],
    ))
    .expect_err("a short run must not be walked");
    assert_eq!(
        error,
        WalChainLoadError::TailNotDescribedByHead {
            boundary_seq: ChangeSeq(0),
            described_segments: 2,
            described_from_seq: ChangeSeq(3),
        }
    );

    // A run with a hole in it: nothing below the hole is described.
    let discontiguous = [
        pointers[0].clone(),
        pointers[2].clone(),
        pointers[3].clone(),
    ];
    let error = count_visible_wal_tail_segments(tail_count_request(
        &namespace_id,
        ChangeSeq(0),
        head_seq,
        &discontiguous,
    ))
    .expect_err("a discontiguous run must not be walked");
    assert!(matches!(
        error,
        WalChainLoadError::TailNotDescribedByHead {
            described_segments: 1,
            ..
        }
    ));

    // A run whose newest entry is not the tip carries no authority at all.
    let error = count_visible_wal_tail_segments(WalChainLoadRequest {
        namespace_id: &namespace_id,
        chain_base_seq: ChangeSeq(0),
        head_seq,
        visible_tip: Some(pointers[0].clone()),
        stop_after_seq: None,
        recent_segments: &pointers[1..],
    })
    .expect_err("a run that does not start at the tip must not be walked");
    assert!(matches!(
        error,
        WalChainLoadError::TailNotDescribedByHead {
            described_segments: 1,
            ..
        }
    ));
}

/// The two readers diverge on purpose: replay walks chain links for history
/// the head no longer names, and the count refuses to.
#[tokio::test]
async fn tail_count_and_chain_load_agree_where_the_head_describes_the_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let chain = prepared_unstored_chain(&namespace_id, 3);
    store_chain(&store, &chain).await;
    let pointers = newest_first_pointers(&chain);

    let request = tail_count_request(&namespace_id, ChangeSeq(0), ChangeSeq(3), &pointers);
    let count = count_visible_wal_tail_segments(request.clone()).expect("count tail");
    let loaded = load_validated_wal_chain(&store, request)
        .await
        .expect("load chain");
    assert_eq!(count as usize, loaded.segments().len());
    assert_eq!(count, 3);

    let unhinted = WalChainLoadRequest {
        recent_segments: &[],
        ..tail_count_request(&namespace_id, ChangeSeq(0), ChangeSeq(3), &pointers)
    };
    count_visible_wal_tail_segments(unhinted.clone())
        .expect_err("an unhinted head describes no tail to count");
    let loaded = load_validated_wal_chain(&store, unhinted)
        .await
        .expect("replay still walks the links");
    assert_eq!(loaded.segments().len(), 3);
}

/// The change feed reads history below the accelerator window, so the chain
/// loader keeps its predecessor walk.
#[tokio::test]
async fn chain_load_walks_predecessor_links_past_the_hinted_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let chain = prepared_unstored_chain(&namespace_id, 3);
    store_chain(&store, &chain).await;
    let pointers = newest_first_pointers(&chain);
    let short_window = &pointers[..2];

    let loaded = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(3),
            visible_tip: Some(pointers[0].clone()),
            stop_after_seq: None,
            recent_segments: short_window,
        },
    )
    .await
    .expect("load past the hinted window");

    assert_eq!(loaded.segments().len(), 3);
    assert_eq!(loaded.segments()[0].records()[0].seq, ChangeSeq(1));
}

/// A boundary-length replay is one wave shape: every segment arrives from
/// the hints, none is fetched twice, and the width is the prefetch bound.
#[tokio::test]
async fn boundary_length_replay_fetches_every_segment_once_in_bounded_waves() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = ConcurrencyWatchStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_segment_prefix(namespace_id.as_str())),
    );
    let segments =
        usize::try_from(crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS).expect("a segment count");
    assert!(
        segments > RECENT_SEGMENT_PREFETCH_CONCURRENCY,
        "a tail inside one wave would not pin the wave's width"
    );
    let chain = prepared_unstored_chain(&namespace_id, crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS);
    store_chain(&store, &chain).await;
    let hints = newest_first_pointers(&chain);

    let loaded = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS),
            visible_tip: Some(hints[0].clone()),
            stop_after_seq: None,
            recent_segments: &hints,
        },
    )
    .await
    .expect("load the whole legal tail");

    assert_eq!(loaded.segments().len(), segments);
    let reads = store.reads();
    assert_eq!(
        reads.total, segments,
        "every segment came from the prefetch; the walk re-fetched none"
    );
    assert_eq!(reads.peak_in_flight, RECENT_SEGMENT_PREFETCH_CONCURRENCY);
}

/// The prefetch fetches only the segments the replay gap intersects.
#[tokio::test]
async fn prefetch_fetches_only_the_segments_the_gap_intersects() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_segment_prefix(namespace_id.as_str())),
    );
    let chain = prepared_unstored_chain(&namespace_id, 5);
    store_chain(&store, &chain).await;
    let hints = newest_first_pointers(&chain);
    store.reset();

    let loaded = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(5),
            visible_tip: Some(hints[0].clone()),
            stop_after_seq: Some(ChangeSeq(3)),
            recent_segments: &hints,
        },
    )
    .await
    .expect("load the newest two segments");

    assert_eq!(loaded.segments().len(), 2);
    let mut fetched = store.take_get_keys();
    fetched.sort();
    let mut expected = vec![chain[3].object_key.clone(), chain[4].object_key.clone()];
    expected.sort();
    assert_eq!(fetched, expected);
}

/// Prefetching moves where the bytes come from, not what is checked: every
/// chain validation runs in the sequential pass either way.
#[tokio::test]
async fn a_corrupt_segment_inside_the_hinted_set_is_still_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let chain = prepared_unstored_chain(&namespace_id, 3);
    store_chain(&store, &chain).await;
    // Rewrite the middle segment's body at its own key: it decodes, but its
    // payload no longer matches the checksum its successor's chain link
    // recorded.
    let mut tampered = chain[1].envelope.clone();
    tampered.payload.records[0].committed_at_ms += 1;
    rewrap_envelope(&mut tampered);
    store
        .put_overwrite(
            &chain[1].object_key,
            Bytes::from(encode_wal_segment_envelope_zstd(&tampered).expect("encode tampered")),
        )
        .await
        .expect("overwrite middle wal segment");
    let hints = newest_first_pointers(&chain);

    let hinted = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(3),
            visible_tip: Some(hints[0].clone()),
            stop_after_seq: None,
            recent_segments: &hints,
        },
    )
    .await
    .expect_err("a prefetched corrupt segment must be rejected");
    let serial = load_validated_wal_chain(
        &store,
        WalChainLoadRequest {
            namespace_id: &namespace_id,
            chain_base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(3),
            visible_tip: Some(hints[0].clone()),
            stop_after_seq: None,
            recent_segments: &[],
        },
    )
    .await
    .expect_err("the serial path rejects it too");

    assert_eq!(
        hinted,
        WalChainLoadError::PointerMismatch {
            object_key: chain[1].object_key.clone(),
        }
    );
    assert_eq!(hinted, serial);
}
