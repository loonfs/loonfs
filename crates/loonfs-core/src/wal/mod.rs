mod frame;
mod reader;
mod replay;
mod writer;

pub(crate) use self::frame::{
    PreparedWalSegment, ReplayedWalTail, ValidatedWalChain, ValidatedWalSegment, WalBuildError,
    WalChainLoadError, WalChainLoadRequest, WalReplayError,
};
pub(crate) use self::reader::load_validated_wal_chain;
pub(crate) use self::replay::replay_validated_wal_tail_with_metadata;
pub(crate) use self::writer::prepare_wal_segment;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{
        materialize_commit, wal_payload_from_materialized_commit, CommitOp, CommitPlan,
        CommitRequest, MaterializedCommit, PreparedCommit, ValidatedOp,
    };
    use bytes::Bytes;
    use loonfs_api::wire::control::WalSegmentPointer;
    use loonfs_api::wire::wal::{encode_wal_segment_envelope_zstd, WalSegmentEnvelope};
    use loonfs_api::{
        validate_wal_segment_id, ChangeSeq, CommitId, FenceToken, InodeId, NamespaceId,
    };
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::wal_segment;
    use loonfs_objectstore::ObjectStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn build_wal_record_payload_matches_segment_record_payload() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
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
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
                name_key: "docs".to_owned(),
                child_inode: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            }],
            resulting_next_inode_id: InodeId(3),
            checked_invariants: Vec::new(),
        };
        let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
        let record = materialize_commit(prepared);

        let segment = prepare_wal_segment(
            namespace_id,
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
        let record = materialized_create_dir(
            &namespace_id,
            "c_wal_unique",
            "docs",
            ChangeSeq(0),
            ChangeSeq(1),
        );

        let first = prepare_wal_segment(
            namespace_id.clone(),
            None,
            std::slice::from_ref(&record),
            "test-writer",
        )
        .expect("prepare first wal segment");
        let second = prepare_wal_segment(
            namespace_id,
            None,
            std::slice::from_ref(&record),
            "test-writer",
        )
        .expect("prepare second wal segment");

        assert_ne!(first.segment_id, second.segment_id);
        assert_ne!(first.object_key, second.object_key);
        validate_wal_segment_id(&first.segment_id).expect("first segment id shape");
        validate_wal_segment_id(&second.segment_id).expect("second segment id shape");
    }

    #[tokio::test]
    async fn validated_wal_chain_loads_visible_segments_in_ascending_order() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let segment = write_create_dir_segment(
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
            },
        )
        .await
        .expect("load valid chain");

        assert_eq!(chain.segments().len(), 1);
        assert_eq!(chain.segments()[0].records()[0].seq, ChangeSeq(1));
    }

    #[tokio::test]
    async fn validated_wal_chain_can_load_cursor_suffix_without_full_base() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let first = write_create_dir_segment(
            &store,
            &namespace_id,
            None,
            "c_wal_suffix_a",
            "alpha",
            ChangeSeq(0),
            ChangeSeq(1),
        )
        .await;
        let second = write_create_dir_segment(
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
            },
        )
        .await
        .expect("load suffix chain");

        assert_eq!(chain.segments().len(), 1);
        assert_eq!(chain.segments()[0].records()[0].seq, ChangeSeq(2));
    }

    #[tokio::test]
    async fn validated_wal_chain_rejects_corrupt_visible_segments() {
        assert_wal_chain_corruption_rejected(|object_key, _envelope, pointer| {
            *object_key = wal_segment("other", "seg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            pointer.object_key = object_key.clone();
        })
        .await;
        assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
            envelope.payload.namespace_id =
                NamespaceId::parse("other").expect("valid namespace id");
            rewrap_envelope(envelope);
            *object_key = wal_segment(
                envelope.payload.namespace_id.as_str(),
                &envelope.payload.segment_id,
            );
            *pointer = envelope.pointer(object_key.clone());
        })
        .await;
        assert_wal_chain_corruption_rejected(|_object_key, envelope, _pointer| {
            envelope.payload.segment_id = "seg_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
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
                &envelope.payload.segment_id,
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
                &envelope.payload.segment_id,
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
                &envelope.payload.segment_id,
            );
            *pointer = envelope.pointer(object_key.clone());
        })
        .await;
    }

    fn materialized_create_dir(
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
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode: InodeId(1),
                display_name: display_name.to_owned(),
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
                parent_inode: InodeId(1),
                display_name: display_name.to_owned(),
                name_key: loonfs_api::name_key_for_display_name(
                    loonfs_api::NamePolicy::default(),
                    display_name,
                ),
                child_inode: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            }],
            resulting_next_inode_id: InodeId(3),
            checked_invariants: Vec::new(),
        };
        let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
        materialize_commit(prepared)
    }

    async fn assert_wal_chain_corruption_rejected(
        corrupt: impl FnOnce(&mut String, &mut WalSegmentEnvelope, &mut WalSegmentPointer),
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let segment = prepare_wal_segment(
            namespace_id.clone(),
            None,
            &[materialized_create_dir(
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

        let encoded =
            encode_wal_segment_envelope_zstd(&envelope).expect("encode corrupted envelope");
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
            },
        )
        .await
        .expect_err("corrupted WAL chain should be rejected");
    }

    async fn write_create_dir_segment(
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
            prev_visible_segment,
            &[materialized_create_dir(
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
        *envelope = WalSegmentEnvelope::from_payload(
            envelope.writer_version.clone(),
            envelope.payload.clone(),
        )
        .expect("rewrap wal envelope");
    }
}
