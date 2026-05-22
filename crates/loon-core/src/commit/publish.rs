use super::{push_unique_invariant, CommitHeadPublishError, CommitPlan, PreparedCommitHeadPublish};
use crate::wal::PreparedWalSegment;
use loon_api::{ChangeSeq, ControlObjectKind, HeadState, HeadStateEnvelope};
use loon_objectstore::keys::namespace_head;
use loon_objectstore::ObjectStoreError;
use loon_objectstore::{ObjectMetadata, ObjectStore};

pub fn prepare_commit_head_publish(
    current_head: &HeadState,
    plan: &CommitPlan,
    wal: &PreparedWalSegment,
    writer_version: &str,
) -> Result<PreparedCommitHeadPublish, CommitHeadPublishError> {
    if writer_version.trim().is_empty() {
        return Err(CommitHeadPublishError::EmptyWriterVersion);
    }

    if current_head.namespace_id != plan.namespace_id {
        return Err(CommitHeadPublishError::NamespaceMismatch {
            head: current_head.namespace_id.clone(),
            plan: plan.namespace_id.clone(),
        });
    }

    let wal_payload = &wal.envelope.payload;
    if wal_payload.namespace_id != current_head.namespace_id {
        return Err(CommitHeadPublishError::WalSegmentNamespaceMismatch {
            head: current_head.namespace_id.clone(),
            wal: wal_payload.namespace_id.clone(),
        });
    }

    if wal_payload.records.is_empty() {
        return Err(CommitHeadPublishError::EmptyWalSegment);
    }

    if wal_payload.base_head_seq != current_head.seq {
        return Err(CommitHeadPublishError::WalSegmentBaseHeadSeqMismatch {
            expected: current_head.seq,
            actual: wal_payload.base_head_seq,
        });
    }

    let expected_start_seq = ChangeSeq(
        current_head
            .seq
            .0
            .checked_add(1)
            .ok_or(CommitHeadPublishError::SeqOverflow)?,
    );
    if wal_payload.start_seq != expected_start_seq {
        return Err(CommitHeadPublishError::WalSegmentStartSeqMismatch {
            expected: expected_start_seq,
            actual: wal_payload.start_seq,
        });
    }

    if wal_payload.end_seq != plan.assigned_seq {
        return Err(CommitHeadPublishError::WalSegmentEndSeqMismatch {
            expected: plan.assigned_seq,
            actual: wal_payload.end_seq,
        });
    }

    let resulting_head = HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: plan.assigned_seq,
        active_fence_token: current_head.active_fence_token,
        next_inode_id: plan.resulting_next_inode_id,
        name_policy: current_head.name_policy,
        checkpoint_hint_seq: current_head.checkpoint_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
        visible_wal_tip: Some(wal.envelope.pointer(wal.object_key.clone())),
    };
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        writer_version,
        resulting_head.clone(),
    )
    .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;
    let encoded_bytes = serde_json::to_vec(&envelope)
        .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;

    let mut checked_invariants = plan.checked_invariants.clone();
    push_unique_invariant(&mut checked_invariants, "head_publish_requires_durable_wal");

    Ok(PreparedCommitHeadPublish {
        object_key: namespace_head(current_head.namespace_id.as_str()),
        resulting_head,
        envelope,
        encoded_bytes,
        checked_invariants,
    })
}

pub fn publish_commit_head<S: ObjectStore + ?Sized>(
    store: &S,
    expected_head_etag: &str,
    prepared: &PreparedCommitHeadPublish,
) -> Result<ObjectMetadata, CommitHeadPublishError> {
    if expected_head_etag.trim().is_empty() {
        return Err(CommitHeadPublishError::EmptyExpectedHeadEtag);
    }

    store
        .compare_and_swap(
            &prepared.object_key,
            expected_head_etag,
            &prepared.encoded_bytes,
        )
        .map_err(map_object_store_error)
}

fn map_object_store_error(err: ObjectStoreError) -> CommitHeadPublishError {
    match err {
        ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict => {
            CommitHeadPublishError::StaleHead
        }
        other => CommitHeadPublishError::Store(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_api::{
        CommitId, FenceToken, InodeId, NamespaceId, WalCommitPayload, WalSegmentEnvelope,
        WalSegmentPayload,
    };

    fn head(namespace_id: NamespaceId, seq: ChangeSeq) -> HeadState {
        HeadState {
            namespace_id,
            seq,
            active_fence_token: FenceToken(1),
            next_inode_id: InodeId(10),
            name_policy: loon_api::NamePolicy::default(),
            checkpoint_hint_seq: Some(ChangeSeq(0)),
            retention_floor_seq: ChangeSeq(0),
            visible_wal_tip: None,
        }
    }

    fn plan(namespace_id: NamespaceId, assigned_seq: ChangeSeq) -> CommitPlan {
        CommitPlan {
            namespace_id,
            commit_id: CommitId::from("publish-plan"),
            apply_after_seq: ChangeSeq(assigned_seq.0.saturating_sub(1)),
            assigned_seq,
            allocated_inode_ids: Vec::new(),
            resolved_restore_content_refs: Vec::new(),
            resolved_source_bindings: Vec::new(),
            resulting_next_inode_id: InodeId(10),
            metadata_preconditions: Vec::new(),
            checked_invariants: Vec::new(),
        }
    }

    fn wal_segment(
        namespace_id: NamespaceId,
        base_head_seq: ChangeSeq,
        start_seq: ChangeSeq,
        end_seq: ChangeSeq,
        record_count: usize,
    ) -> PreparedWalSegment {
        let records = (0..record_count)
            .map(|index| {
                let offset = u64::try_from(index).expect("test index");
                let seq = ChangeSeq(start_seq.0 + offset);
                WalCommitPayload {
                    namespace_id: namespace_id.clone(),
                    seq,
                    apply_after_seq: ChangeSeq(seq.0.saturating_sub(1)),
                    commit_id: CommitId::from(format!("publish-record-{index}")),
                    semantic_commit_fingerprint_sha256: format!("fingerprint-{index}"),
                    writer_id: "writer-a".to_owned(),
                    writer_fence_token: FenceToken(1),
                    message: None,
                    annotations: None,
                    ops: Vec::new(),
                    preconditions: Vec::new(),
                    results: Vec::new(),
                }
            })
            .collect();
        let segment_id = "seg_publish_test".to_owned();
        let payload = WalSegmentPayload {
            namespace_id: namespace_id.clone(),
            segment_id: segment_id.clone(),
            prev_visible_segment: None,
            base_head_seq,
            start_seq,
            end_seq,
            records,
        };
        let envelope = WalSegmentEnvelope::from_payload("test", payload).expect("wal envelope");
        PreparedWalSegment {
            object_key: format!(
                "namespaces/{}/wal/{}-{}-{segment_id}.cbor.zst",
                namespace_id.as_str(),
                start_seq.0,
                end_seq.0
            ),
            segment_id,
            envelope,
            encoded_bytes: Vec::new(),
            checked_invariants: Vec::new(),
        }
    }

    #[test]
    fn head_publish_accepts_segment_connecting_current_head_to_plan() {
        let namespace_id = NamespaceId::from("demo");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(7), ChangeSeq(8), ChangeSeq(9), 2);

        let prepared = prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer")
            .expect("prepare head publish");

        assert_eq!(prepared.resulting_head.seq, ChangeSeq(9));
        assert_eq!(
            prepared.resulting_head.visible_wal_tip,
            Some(wal.envelope.pointer(wal.object_key.clone()))
        );
    }

    #[test]
    fn head_publish_rejects_segment_base_after_current_head() {
        let namespace_id = NamespaceId::from("demo");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(8), ChangeSeq(9), ChangeSeq(9), 1);

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer"),
            Err(CommitHeadPublishError::WalSegmentBaseHeadSeqMismatch {
                expected: ChangeSeq(7),
                actual: ChangeSeq(8),
            })
        ));
    }

    #[test]
    fn head_publish_rejects_segment_base_before_current_head() {
        let namespace_id = NamespaceId::from("demo");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(6), ChangeSeq(7), ChangeSeq(9), 3);

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer"),
            Err(CommitHeadPublishError::WalSegmentBaseHeadSeqMismatch {
                expected: ChangeSeq(7),
                actual: ChangeSeq(6),
            })
        ));
    }

    #[test]
    fn head_publish_rejects_empty_segment() {
        let namespace_id = NamespaceId::from("demo");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(7), ChangeSeq(8), ChangeSeq(9), 0);

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer"),
            Err(CommitHeadPublishError::EmptyWalSegment)
        ));
    }

    #[test]
    fn head_publish_rejects_segment_start_that_skips_current_head() {
        let namespace_id = NamespaceId::from("demo");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(7), ChangeSeq(9), ChangeSeq(9), 1);

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer"),
            Err(CommitHeadPublishError::WalSegmentStartSeqMismatch {
                expected: ChangeSeq(8),
                actual: ChangeSeq(9),
            })
        ));
    }

    #[test]
    fn head_publish_rejects_segment_end_that_differs_from_plan() {
        let namespace_id = NamespaceId::from("demo");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(7), ChangeSeq(8), ChangeSeq(10), 3);

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer"),
            Err(CommitHeadPublishError::WalSegmentEndSeqMismatch {
                expected: ChangeSeq(9),
                actual: ChangeSeq(10),
            })
        ));
    }

    #[test]
    fn head_publish_rejects_segment_namespace_mismatch() {
        let current_head = head(NamespaceId::from("demo"), ChangeSeq(7));
        let plan = plan(NamespaceId::from("demo"), ChangeSeq(9));
        let wal = wal_segment(
            NamespaceId::from("other"),
            ChangeSeq(7),
            ChangeSeq(8),
            ChangeSeq(9),
            2,
        );

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer"),
            Err(CommitHeadPublishError::WalSegmentNamespaceMismatch { head, wal })
                if head == NamespaceId::from("demo") && wal == NamespaceId::from("other")
        ));
    }
}
