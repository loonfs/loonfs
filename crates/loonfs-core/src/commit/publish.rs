use super::{push_unique_invariant, CommitHeadPublishError, CommitPlan};
use crate::invariants::InvariantId;
use crate::wal::PreparedWalSegment;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope, WalSegmentPointer,
};
use loonfs_api::ChangeSeq;
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::ObjectStoreError;
use loonfs_objectstore::{ObjectMetadata, ObjectStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommitHeadPublish {
    pub object_key: String,
    pub resulting_head: HeadState,
    pub envelope: HeadStateEnvelope,
    pub encoded_bytes: Vec<u8>,
    pub checked_invariants: Vec<InvariantId>,
}

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
    if wal_payload.writer_epoch != current_head.writer_epoch {
        return Err(CommitHeadPublishError::WalSegmentWriterEpochMismatch {
            expected: current_head.writer_epoch,
            actual: wal_payload.writer_epoch,
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

    let new_tip = wal.envelope.pointer(wal.object_key.clone());
    let resulting_head = HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: plan.assigned_seq,
        head_commit_id: plan.commit_id.clone(),
        writer_epoch: current_head.writer_epoch,
        writer: current_head.writer.clone(),
        next_inode_id: plan.resulting_next_inode_id,
        name_policy: current_head.name_policy,
        current_manifest_id: current_head.current_manifest_id,
        latest_checkpoint_id: current_head.latest_checkpoint_id.clone(),
        retention_floor_seq: current_head.retention_floor_seq,
        recent_segments: next_recent_segments(current_head, new_tip.clone()),
        visible_wal_tip: Some(new_tip),
        state: current_head.state,
    };
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::WalHead,
        writer_version,
        resulting_head.clone(),
    )
    .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;
    let encoded_bytes = encode_control_object(&envelope)
        .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;

    let mut checked_invariants = plan.checked_invariants.clone();
    push_unique_invariant(
        &mut checked_invariants,
        InvariantId::HeadPublishRequiresDurableWal,
    );

    Ok(PreparedCommitHeadPublish {
        object_key: wal_head(current_head.namespace_id.as_str()),
        resulting_head,
        envelope,
        encoded_bytes,
        checked_invariants,
    })
}

/// How many segment pointers the head carries as a replay accelerator.
///
/// Newest first, tip included. The bound keeps the head one small object at
/// any commit rate; readers needing older history walk the chain links,
/// which remain the only authority.
const RECENT_SEGMENTS_LIMIT: usize = 32;

fn next_recent_segments(
    current_head: &HeadState,
    new_tip: WalSegmentPointer,
) -> Vec<WalSegmentPointer> {
    let mut recent = Vec::with_capacity(RECENT_SEGMENTS_LIMIT);
    recent.push(new_tip);
    if current_head.recent_segments.is_empty() {
        // Heads published before the accelerator existed carry only the tip
        // pointer; seed from it so the hint list stays gap-free.
        recent.extend(current_head.visible_wal_tip.iter().cloned());
    } else {
        recent.extend(current_head.recent_segments.iter().cloned());
    }
    recent.truncate(RECENT_SEGMENTS_LIMIT);
    recent
}

pub async fn publish_commit_head<S: ObjectStore + ?Sized>(
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
            Bytes::copy_from_slice(&prepared.encoded_bytes),
        )
        .await
        .map_err(map_object_store_error)
}

fn map_object_store_error(err: ObjectStoreError) -> CommitHeadPublishError {
    match err {
        ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict => {
            CommitHeadPublishError::StaleHead
        }
        // A transport failure after the CAS was sent leaves the outcome
        // unobserved: the head may already reference the new segment.
        ObjectStoreError::Transport(message) => CommitHeadPublishError::OutcomeUnknown(message),
        other => CommitHeadPublishError::Store(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::keys::wal_segment as wal_segment_key;

    #[test]
    fn head_cas_transport_failure_maps_to_unknown_outcome_not_failure() {
        assert_eq!(
            map_object_store_error(ObjectStoreError::Transport("timeout".to_owned())),
            CommitHeadPublishError::OutcomeUnknown("timeout".to_owned())
        );
        assert_eq!(
            map_object_store_error(ObjectStoreError::PreconditionFailed),
            CommitHeadPublishError::StaleHead
        );
        assert!(matches!(
            map_object_store_error(ObjectStoreError::NotFound),
            CommitHeadPublishError::Store(_)
        ));
    }
    use loonfs_api::wire::control::WriterBlock;
    use loonfs_api::wire::wal::{WalCommitPayload, WalSegmentEnvelope, WalSegmentPayload};
    use loonfs_api::{CheckpointId, CommitId, InodeId, ManifestId, NamespaceId, WriterEpoch};

    fn head(namespace_id: NamespaceId, seq: ChangeSeq) -> HeadState {
        HeadState {
            namespace_id,
            seq,
            head_commit_id: CommitId::parse("c_00000000000000000000000000000000")
                .expect("commit id"),
            writer_epoch: WriterEpoch(1),
            writer: Some(WriterBlock {
                writer_id: "writer-a".to_owned(),
                writer_session_id: "wrs_test".to_owned(),
                acquired_at_ms: 1_000,
            }),
            next_inode_id: InodeId(10),
            name_policy: loonfs_api::NamePolicy::default(),
            current_manifest_id: Some(ManifestId(0)),
            latest_checkpoint_id: Some(
                CheckpointId::parse("chk_00000000000000000000000000000000").expect("checkpoint id"),
            ),
            retention_floor_seq: ChangeSeq(0),
            visible_wal_tip: None,
            recent_segments: Vec::new(),
            state: Default::default(),
        }
    }

    fn plan(namespace_id: NamespaceId, assigned_seq: ChangeSeq) -> CommitPlan {
        CommitPlan {
            namespace_id,
            commit_id: CommitId::parse("publish-plan").expect("valid commit id"),
            apply_after_seq: ChangeSeq(assigned_seq.0.saturating_sub(1)),
            assigned_seq,
            validated_ops: Vec::new(),
            resulting_next_inode_id: InodeId(10),
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
                    seq,
                    commit_id: CommitId::try_new(format!("publish-record-{index}"))
                        .expect("valid commit id"),
                    semantic_commit_fingerprint: format!("fingerprint-{index}"),
                    message: None,
                    deltas: Vec::new(),
                }
            })
            .collect();
        let segment_id = "seg_publish_test".to_owned();
        let payload = WalSegmentPayload {
            namespace_id: namespace_id.clone(),
            segment_id: segment_id.clone(),
            writer_epoch: WriterEpoch(1),
            prev_visible_segment: None,
            base_head_seq,
            start_seq,
            end_seq,
            records,
        };
        let envelope = WalSegmentEnvelope::from_payload("test", payload).expect("wal envelope");
        PreparedWalSegment {
            object_key: wal_segment_key(namespace_id.as_str(), &segment_id),
            segment_id,
            envelope,
            encoded_bytes: Vec::new(),
            checked_invariants: Vec::new(),
        }
    }

    #[test]
    fn head_publish_accepts_segment_connecting_current_head_to_plan() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
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
    fn head_publish_seeds_recent_segments_from_the_prior_tip() {
        // Upgrade path: a head that has only a tip pointer still produces a
        // gap-free hint list.
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let mut current_head = head(namespace_id.clone(), ChangeSeq(7));
        let prior = wal_segment(
            namespace_id.clone(),
            ChangeSeq(5),
            ChangeSeq(6),
            ChangeSeq(7),
            2,
        );
        let prior_tip = prior.envelope.pointer(prior.object_key.clone());
        current_head.visible_wal_tip = Some(prior_tip.clone());
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(7), ChangeSeq(8), ChangeSeq(9), 2);

        let prepared = prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer")
            .expect("prepare head publish");

        let new_tip = wal.envelope.pointer(wal.object_key.clone());
        assert_eq!(
            prepared.resulting_head.recent_segments,
            vec![new_tip.clone(), prior_tip]
        );
        assert_eq!(prepared.resulting_head.visible_wal_tip, Some(new_tip));
    }

    #[test]
    fn head_publish_prepends_the_tip_and_truncates_recent_segments() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let mut current_head = head(namespace_id.clone(), ChangeSeq(100));
        let filler = |index: u64| {
            let segment = wal_segment(
                namespace_id.clone(),
                ChangeSeq(index),
                ChangeSeq(index + 1),
                ChangeSeq(index + 1),
                1,
            );
            segment.envelope.pointer(segment.object_key.clone())
        };
        current_head.recent_segments = (0..32).rev().map(filler).collect();
        let oldest = current_head
            .recent_segments
            .last()
            .cloned()
            .expect("oldest");
        let plan = plan(namespace_id.clone(), ChangeSeq(101));
        let wal = wal_segment(
            namespace_id,
            ChangeSeq(100),
            ChangeSeq(101),
            ChangeSeq(101),
            1,
        );

        let prepared = prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer")
            .expect("prepare head publish");

        let recent = &prepared.resulting_head.recent_segments;
        assert_eq!(recent.len(), 32);
        assert_eq!(recent[0], wal.envelope.pointer(wal.object_key.clone()));
        assert!(!recent.contains(&oldest), "oldest hint must fall off");
    }

    #[test]
    fn head_publish_rejects_segment_base_after_current_head() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
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
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
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
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
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
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
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
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
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
        let current_head = head(
            NamespaceId::parse("demo").expect("valid namespace id"),
            ChangeSeq(7),
        );
        let plan = plan(
            NamespaceId::parse("demo").expect("valid namespace id"),
            ChangeSeq(9),
        );
        let wal = wal_segment(
            NamespaceId::parse("other").expect("valid namespace id"),
            ChangeSeq(7),
            ChangeSeq(8),
            ChangeSeq(9),
            2,
        );

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal, "test-writer"),
            Err(CommitHeadPublishError::WalSegmentNamespaceMismatch { head, wal })
                if head == NamespaceId::parse("demo").expect("valid namespace id") && wal == NamespaceId::parse("other").expect("valid namespace id")
        ));
    }
}
