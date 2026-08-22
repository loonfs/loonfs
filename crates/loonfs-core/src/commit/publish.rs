//! Publishes a prepared WAL segment: the segment PUT and the head
//! compare-and-swap that makes its commits visible.

use super::{CommitHeadPublishError, CommitPlan};
use crate::limits::RECENT_SEGMENTS_LIMIT;
use crate::wal::PreparedWalSegment;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, ControlObjectKind, HeadState, WalSegmentPointer,
};
use loonfs_api::{next_public_ordinal, ChangeSeq};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::ObjectStoreError;
use loonfs_objectstore::{ObjectMetadata, ObjectStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PreparedCommitHeadPublish {
    pub resulting_head: HeadState,
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
}

pub(crate) fn prepare_commit_head_publish(
    current_head: &HeadState,
    plan: &CommitPlan,
    wal: &PreparedWalSegment,
) -> Result<PreparedCommitHeadPublish, CommitHeadPublishError> {
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
        next_public_ordinal(current_head.seq.0).ok_or(CommitHeadPublishError::SeqOverflow)?,
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

    let object_key = wal_head(&current_head.namespace_id);
    let new_tip = wal.envelope.pointer();
    let resulting_head = HeadState {
        namespace_id: current_head.namespace_id.clone(),
        // The head is the only durable home of the namespace's content
        // store, name policy, and fork provenance: every successor carries
        // them forward verbatim, and the assertion below proves it did.
        content_store_id: current_head.content_store_id.clone(),
        created_at_ms: current_head.created_at_ms,
        fork_basis: current_head.fork_basis.clone(),
        seq: plan.assigned_seq,
        head_commit_id: plan.commit_id.clone(),
        writer_epoch: current_head.writer_epoch,
        writer: current_head.writer.clone(),
        next_inode_id: plan.resulting_next_inode_id,
        recent_segments: next_recent_segments(current_head),
        visible_wal_tip: Some(new_tip),
        status: current_head.status,
    };
    current_head
        .ensure_successor_identity(&resulting_head)
        .map_err(CommitHeadPublishError::HeadIdentityDrift)?;
    let encoded_bytes =
        encode_control_state(ControlObjectKind::WalHead, &resulting_head).map_err(|err| {
            CommitHeadPublishError::Codec {
                object_key: object_key.clone(),
                message: err.to_string(),
            }
        })?;

    Ok(PreparedCommitHeadPublish {
        resulting_head,
        object_key,
        encoded_bytes,
    })
}

/// Rebuilds the head's predecessor accelerator below the newly published
/// tip, newest first and capped at [`RECENT_SEGMENTS_LIMIT`]. Readers
/// reaching past it walk the chain links, which remain the only history
/// authority.
fn next_recent_segments(current_head: &HeadState) -> Vec<WalSegmentPointer> {
    let mut recent = Vec::with_capacity(RECENT_SEGMENTS_LIMIT);
    recent.extend(current_head.visible_wal_tip.iter().cloned());
    recent.extend(current_head.recent_segments.iter().cloned());
    recent.truncate(RECENT_SEGMENTS_LIMIT);
    recent
}

pub(crate) async fn publish_commit_head<S: ObjectStore + ?Sized>(
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
        .map_err(|error| map_object_store_error(&prepared.object_key, error))
}

fn map_object_store_error(object_key: &str, err: ObjectStoreError) -> CommitHeadPublishError {
    match err {
        ObjectStoreError::PreconditionFailed { .. } => CommitHeadPublishError::StaleHead,
        // A transport failure after the CAS was sent leaves the outcome
        // unobserved: the head may already reference the new segment.
        error @ ObjectStoreError::Transport { .. } => {
            CommitHeadPublishError::OutcomeUnknown(error.public_message().into_owned())
        }
        other => CommitHeadPublishError::Store {
            object_key: object_key.to_owned(),
            message: other.public_message().into_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitFingerprint;
    use loonfs_api::wire::control::{encode_control_object, HeadStateEnvelope};

    #[test]
    fn head_cas_transport_failure_maps_to_unknown_outcome_not_failure() {
        assert_eq!(
            map_object_store_error(
                "namespaces/demo/control/head.json",
                ObjectStoreError::transport("namespaces/demo/control/head.json", "timeout"),
            ),
            CommitHeadPublishError::OutcomeUnknown(
                loonfs_objectstore::ObjectStoreErrorClass::Other
                    .public_message()
                    .into_owned()
            )
        );
        assert_eq!(
            map_object_store_error(
                "namespaces/demo/control/head.json",
                ObjectStoreError::PreconditionFailed {
                    object_key: "namespaces/demo/control/head.json".to_owned(),
                },
            ),
            CommitHeadPublishError::StaleHead
        );
        assert!(matches!(
            map_object_store_error(
                "namespaces/demo/control/head.json",
                ObjectStoreError::NotFound {
                    object_key: "namespaces/demo/control/head.json".to_owned(),
                },
            ),
            CommitHeadPublishError::Store { .. }
        ));
    }
    use loonfs_api::wire::control::{NamespaceStatus, WriterBlock};
    use loonfs_api::wire::wal::{WalCommitPayload, WalSegmentEnvelope, WalSegmentPayload};
    use loonfs_api::{CommitId, InodeId, NamespaceId, WalSegmentId, WriterEpoch, MAX_ID_BYTES};

    fn head(namespace_id: NamespaceId, seq: ChangeSeq) -> HeadState {
        HeadState {
            namespace_id,
            content_store_id: loonfs_api::ContentStoreId::parse(
                "cs_0123456789abcdef0123456789abcdef",
            )
            .expect("content store id"),
            created_at_ms: 1_000,
            fork_basis: None,
            seq,
            head_commit_id: CommitId::parse("c_00000000000000000000000000000000")
                .expect("commit id"),
            writer_epoch: WriterEpoch(1),
            writer: Some(WriterBlock {
                writer_id: "writer-a".to_owned(),
                acquired_at_ms: 1_000,
            }),
            next_inode_id: InodeId(10),
            visible_wal_tip: None,
            recent_segments: Vec::new(),
            status: NamespaceStatus::Active {},
        }
    }

    fn plan(namespace_id: NamespaceId, assigned_seq: ChangeSeq) -> CommitPlan {
        CommitPlan {
            namespace_id,
            commit_id: CommitId::parse("publish-plan").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            message: None,
            semantic_identity: CommitFingerprint::new_unchecked(
                "v1:sha256:publish-plan".to_owned(),
            ),
            apply_after_seq: ChangeSeq(assigned_seq.0.saturating_sub(1)),
            assigned_seq,
            validated_ops: Vec::new(),
            resulting_next_inode_id: InodeId(10),
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
                    commit_id: CommitId::parse(format!("publish-record-{index}"))
                        .expect("valid commit id"),
                    actor: loonfs_test_support::test_actor(),
                    semantic_commit_fingerprint: format!("fingerprint-{index}"),
                    committed_at_ms: 4_200,
                    message: None,
                    deltas: Vec::new(),
                }
            })
            .collect();
        let segment_id =
            WalSegmentId::parse("00000000000000000001-aaaaaaaaaaaaaaaa").expect("valid segment id");
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
        let envelope = WalSegmentEnvelope::from_payload(payload).expect("wal envelope");
        PreparedWalSegment {
            envelope,
            encoded_bytes: Vec::new(),
        }
    }

    #[test]
    fn head_publish_accepts_segment_connecting_current_head_to_plan() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(7), ChangeSeq(8), ChangeSeq(9), 2);

        let prepared =
            prepare_commit_head_publish(&current_head, &plan, &wal).expect("prepare head publish");

        assert_eq!(prepared.resulting_head.seq, ChangeSeq(9));
        assert_eq!(
            prepared.resulting_head.visible_wal_tip,
            Some(wal.envelope.pointer())
        );
        assert_eq!(
            prepared.object_key,
            wal_head(&prepared.resulting_head.namespace_id),
        );
        let expected_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::WalHead,
            prepared.resulting_head.clone(),
        )
        .expect("head envelope");
        assert_eq!(
            prepared.encoded_bytes,
            encode_control_object(&expected_envelope).expect("encoded head"),
        );
    }

    #[test]
    fn head_publish_accepts_the_public_sequence_maximum() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let current_head = head(
            namespace_id.clone(),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER - 1),
        );
        let plan = plan(
            namespace_id.clone(),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
        );
        let wal = wal_segment(
            namespace_id,
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER - 1),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
            1,
        );

        let prepared =
            prepare_commit_head_publish(&current_head, &plan, &wal).expect("maximum sequence");
        assert_eq!(
            prepared.resulting_head.seq,
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER)
        );
    }

    #[test]
    fn head_publish_rejects_advancing_past_the_public_sequence_maximum() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let current_head = head(
            namespace_id.clone(),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
        );
        let plan = plan(
            namespace_id.clone(),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
        );
        let wal = wal_segment(
            namespace_id,
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
            ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
            1,
        );

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal),
            Err(CommitHeadPublishError::SeqOverflow)
        ));
    }

    #[test]
    fn head_publish_moves_the_old_tip_into_predecessor_hints() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let mut current_head = head(namespace_id.clone(), ChangeSeq(7));
        let prior = wal_segment(
            namespace_id.clone(),
            ChangeSeq(5),
            ChangeSeq(6),
            ChangeSeq(7),
            2,
        );
        let prior_tip = prior.envelope.pointer();
        current_head.visible_wal_tip = Some(prior_tip.clone());
        current_head.recent_segments = Vec::new();
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(7), ChangeSeq(8), ChangeSeq(9), 2);

        let prepared =
            prepare_commit_head_publish(&current_head, &plan, &wal).expect("prepare head publish");

        let new_tip = wal.envelope.pointer();
        assert_eq!(prepared.resulting_head.recent_segments, vec![prior_tip]);
        assert_eq!(prepared.resulting_head.visible_wal_tip, Some(new_tip));
    }

    #[test]
    fn head_publish_prepends_the_old_tip_and_truncates_predecessor_hints() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let limit = u64::try_from(RECENT_SEGMENTS_LIMIT).expect("limit fits a sequence");
        let mut current_head = head(namespace_id.clone(), ChangeSeq(limit));
        let filler = |index: u64| {
            let segment = wal_segment(
                namespace_id.clone(),
                ChangeSeq(index),
                ChangeSeq(index + 1),
                ChangeSeq(index + 1),
                1,
            );
            segment.envelope.pointer()
        };
        let old_tip = filler(limit);
        current_head.visible_wal_tip = Some(old_tip.clone());
        // A predecessor list already at the cap: the old tip has to displace something.
        current_head.recent_segments = (0..limit).rev().map(filler).collect();
        let oldest = current_head
            .recent_segments
            .last()
            .cloned()
            .expect("oldest");
        let plan = plan(namespace_id.clone(), ChangeSeq(limit + 1));
        let wal = wal_segment(
            namespace_id,
            ChangeSeq(limit),
            ChangeSeq(limit + 1),
            ChangeSeq(limit + 1),
            1,
        );

        let prepared =
            prepare_commit_head_publish(&current_head, &plan, &wal).expect("prepare head publish");

        let recent = &prepared.resulting_head.recent_segments;
        assert_eq!(recent.len(), RECENT_SEGMENTS_LIMIT);
        assert_eq!(recent[0], old_tip);
        assert_eq!(
            prepared.resulting_head.visible_wal_tip,
            Some(wal.envelope.pointer())
        );
        assert!(!recent.contains(&oldest), "oldest hint must fall off");
    }

    #[test]
    fn head_publish_rejects_segment_base_after_current_head() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let current_head = head(namespace_id.clone(), ChangeSeq(7));
        let plan = plan(namespace_id.clone(), ChangeSeq(9));
        let wal = wal_segment(namespace_id, ChangeSeq(8), ChangeSeq(9), ChangeSeq(9), 1);

        assert!(matches!(
            prepare_commit_head_publish(&current_head, &plan, &wal),
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
            prepare_commit_head_publish(&current_head, &plan, &wal),
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
            prepare_commit_head_publish(&current_head, &plan, &wal),
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
            prepare_commit_head_publish(&current_head, &plan, &wal),
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
            prepare_commit_head_publish(&current_head, &plan, &wal),
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
            prepare_commit_head_publish(&current_head, &plan, &wal),
            Err(CommitHeadPublishError::WalSegmentNamespaceMismatch { head, wal })
                if head == NamespaceId::parse("demo").expect("valid namespace id") && wal == NamespaceId::parse("other").expect("valid namespace id")
        ));
    }

    /// Encodes a full accelerator through the real codec, so the numbers
    /// the cap is justified by are measured rather than estimated.
    fn encoded_head_bytes(namespace: &str, newest_seq: u64) -> usize {
        let namespace_id = NamespaceId::parse(namespace).expect("valid namespace id");
        let segments: Vec<WalSegmentPointer> = (0..=RECENT_SEGMENTS_LIMIT)
            .map(|index| {
                let offset = u64::try_from(index).expect("test index");
                let seq = ChangeSeq(newest_seq - offset);
                let segment_id = WalSegmentId::parse(format!("{:020}-{offset:016x}", seq.0))
                    .expect("valid segment id");
                WalSegmentPointer {
                    segment_id,
                    start_seq: seq,
                    end_seq: seq,
                    payload_checksum: format!("sha256:{}", "b".repeat(64)),
                }
            })
            .collect();
        let mut state = head(namespace_id, ChangeSeq(newest_seq));
        state.visible_wal_tip = segments.first().cloned();
        state.recent_segments = segments.into_iter().skip(1).collect();

        let envelope = HeadStateEnvelope::from_state(ControlObjectKind::WalHead, state)
            .expect("head envelope");
        encode_control_object(&envelope).expect("encode head").len()
    }

    /// The head is the one control object whose size grows with the tail it
    /// describes, so raising the accelerator's cap is a claim about how big
    /// the head gets. This measures it at the cap, from both ends of what
    /// the identifier grammars allow.
    #[test]
    fn a_head_at_the_accelerator_cap_stays_a_small_object() {
        const CEILING_BYTES: usize = 256 * 1024;

        let realistic = encoded_head_bytes("demo", 4_200);
        let worst_case = encoded_head_bytes(&"n".repeat(MAX_ID_BYTES), u64::MAX);

        assert!(
            worst_case < CEILING_BYTES,
            "a full head encodes to {worst_case} bytes at the longest namespace id and \
             sequence numbers the grammars allow, and {realistic} bytes at realistic ones; \
             the ceiling is {CEILING_BYTES}"
        );
    }
}
