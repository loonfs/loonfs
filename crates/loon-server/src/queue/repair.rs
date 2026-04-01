use crate::queue::broker::{attach_follow_up, merge_seq_payload};
use crate::queue::types::{JobState, QueueJob, QueueShardState, SeqScopedPayload, WorkClass};
use loon_types::{ChangeSeq, HeadState, NamespaceId, ProgressState};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotRepairOutcome {
    NoRepairNeeded,
    Enqueued { through_seq: ChangeSeq },
    RaisedReadyJob { through_seq: ChangeSeq },
    AttachedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotRepairError {
    QueueWorkClassMismatch {
        expected: WorkClass,
        actual: WorkClass,
    },
    ProgressNamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    ProgressWorkClassMismatch {
        expected: String,
        actual: String,
    },
}

pub fn build_snapshot_dedupe_key(namespace_id: &NamespaceId) -> String {
    format!("{}:{namespace_id}", WorkClass::BuildSnapshot.as_str())
}

pub fn repair_lost_snapshot_enqueue(
    shard: &mut QueueShardState,
    head: &HeadState,
    progress: Option<&ProgressState>,
) -> Result<SnapshotRepairOutcome, SnapshotRepairError> {
    if shard.work_class != WorkClass::BuildSnapshot {
        return Err(SnapshotRepairError::QueueWorkClassMismatch {
            expected: WorkClass::BuildSnapshot,
            actual: shard.work_class.clone(),
        });
    }

    if head.seq == ChangeSeq(0) {
        return Ok(SnapshotRepairOutcome::NoRepairNeeded);
    }

    if let Some(progress) = progress {
        if progress.namespace_id != head.namespace_id {
            return Err(SnapshotRepairError::ProgressNamespaceMismatch {
                expected: head.namespace_id.clone(),
                actual: progress.namespace_id.clone(),
            });
        }

        if progress.work_class != WorkClass::BuildSnapshot.as_str() {
            return Err(SnapshotRepairError::ProgressWorkClassMismatch {
                expected: WorkClass::BuildSnapshot.as_str().to_owned(),
                actual: progress.work_class.clone(),
            });
        }

        if progress.through_seq >= head.seq {
            return Ok(SnapshotRepairOutcome::NoRepairNeeded);
        }
    }

    let desired_payload = SeqScopedPayload {
        namespace_id: head.namespace_id.clone(),
        through_seq: head.seq,
    };
    let dedupe_key = build_snapshot_dedupe_key(&head.namespace_id);

    if let Some(job) = shard
        .jobs
        .iter_mut()
        .find(|job| job.dedupe_key == dedupe_key)
    {
        match job.state {
            JobState::Ready => {
                let before = job.payload.through_seq;
                merge_seq_payload(&mut job.payload, desired_payload);

                if job.payload.through_seq > before {
                    return Ok(SnapshotRepairOutcome::RaisedReadyJob {
                        through_seq: job.payload.through_seq,
                    });
                }
            }
            JobState::Claimed => {
                let before = job.follow_up.as_ref().map(|payload| payload.through_seq);
                attach_follow_up(job, desired_payload);
                let after = job
                    .follow_up
                    .as_ref()
                    .map(|payload| payload.through_seq)
                    .expect("claimed job should have follow-up after repair");

                if Some(after) > before {
                    return Ok(SnapshotRepairOutcome::AttachedFollowUp { through_seq: after });
                }
            }
        }

        return Ok(SnapshotRepairOutcome::NoRepairNeeded);
    }

    shard.jobs.push(QueueJob {
        job_id: format!(
            "repair-{}-{}",
            WorkClass::BuildSnapshot.as_str(),
            head.namespace_id
        ),
        dedupe_key,
        state: JobState::Ready,
        payload: desired_payload.clone(),
        follow_up: None,
        claim: None,
        attempts: 0,
    });

    Ok(SnapshotRepairOutcome::Enqueued {
        through_seq: desired_payload.through_seq,
    })
}

#[cfg(test)]
mod tests {
    use super::{build_snapshot_dedupe_key, repair_lost_snapshot_enqueue, SnapshotRepairOutcome};
    use crate::queue::types::{JobState, QueueJob, QueueShardState, SeqScopedPayload, WorkClass};
    use loon_types::{ChangeSeq, FenceToken, HeadState, InodeId, NamespaceId, ProgressState};

    #[test]
    fn repair_enqueues_snapshot_job_when_progress_lags() {
        let mut shard = empty_snapshot_shard();
        let head = sample_head(ChangeSeq(42));
        let progress = ProgressState {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(40),
        };

        let outcome = repair_lost_snapshot_enqueue(&mut shard, &head, Some(&progress))
            .expect("repair should enqueue missing snapshot job");

        assert_eq!(
            outcome,
            SnapshotRepairOutcome::Enqueued {
                through_seq: ChangeSeq(42),
            }
        );
        assert_eq!(shard.jobs.len(), 1);
        assert_eq!(shard.jobs[0].dedupe_key, "BuildSnapshot:ns-1");
    }

    #[test]
    fn repair_raises_ready_snapshot_job_to_head_seq() {
        let mut shard = empty_snapshot_shard();
        shard.jobs.push(QueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: build_snapshot_dedupe_key(&NamespaceId::from("ns-1")),
            state: JobState::Ready,
            payload: SeqScopedPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(40),
            },
            follow_up: None,
            claim: None,
            attempts: 0,
        });

        let outcome = repair_lost_snapshot_enqueue(&mut shard, &sample_head(ChangeSeq(42)), None)
            .expect("repair should raise ready job");

        assert_eq!(
            outcome,
            SnapshotRepairOutcome::RaisedReadyJob {
                through_seq: ChangeSeq(42),
            }
        );
        assert_eq!(shard.jobs[0].payload.through_seq, ChangeSeq(42));
    }

    #[test]
    fn repair_attaches_follow_up_for_claimed_snapshot_job() {
        let mut shard = empty_snapshot_shard();
        shard.jobs.push(QueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: build_snapshot_dedupe_key(&NamespaceId::from("ns-1")),
            state: JobState::Claimed,
            payload: SeqScopedPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(40),
            },
            follow_up: None,
            claim: Some(crate::queue::types::QueueClaim {
                worker_id: "worker-a".to_owned(),
                claim_token: "claim-a".to_owned(),
                heartbeat_at_ms: 0,
                timeout_at_ms: 10_000,
            }),
            attempts: 1,
        });

        let outcome = repair_lost_snapshot_enqueue(&mut shard, &sample_head(ChangeSeq(42)), None)
            .expect("repair should attach follow-up");

        assert_eq!(
            outcome,
            SnapshotRepairOutcome::AttachedFollowUp {
                through_seq: ChangeSeq(42),
            }
        );
        assert_eq!(
            shard.jobs[0].follow_up,
            Some(SeqScopedPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(42),
            })
        );
    }

    #[test]
    fn repair_noops_when_progress_covers_head() {
        let mut shard = empty_snapshot_shard();
        let progress = ProgressState {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(42),
        };

        let outcome =
            repair_lost_snapshot_enqueue(&mut shard, &sample_head(ChangeSeq(42)), Some(&progress))
                .expect("repair should no-op when progress covers head");

        assert_eq!(outcome, SnapshotRepairOutcome::NoRepairNeeded);
        assert!(shard.jobs.is_empty());
    }

    fn empty_snapshot_shard() -> QueueShardState {
        QueueShardState {
            work_class: WorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![],
        }
    }

    fn sample_head(seq: ChangeSeq) -> HeadState {
        HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq,
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        }
    }
}
