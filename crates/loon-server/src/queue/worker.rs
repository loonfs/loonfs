use crate::queue::broker::{ensure_active_broker_lease, BrokerLeaseGuardError};
use crate::queue::types::{JobState, QueueClaim, QueueShardState};
use loon_types::ChangeSeq;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
    pub worker_id: String,
    pub claim_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerMutationError {
    Broker(BrokerLeaseGuardError),
    JobNotFound {
        job_id: String,
    },
    JobBusy {
        job_id: String,
        worker_id: String,
        timeout_at_ms: u64,
        now_ms: u64,
    },
    JobNotClaimed {
        job_id: String,
    },
    ClaimTokenMismatch {
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobClaimOutcome {
    Claimed { claim_token: String },
    Stolen { claim_token: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobHeartbeatOutcome {
    pub claim_token: String,
    pub timeout_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobCompleteOutcome {
    Removed,
    PromotedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobClaimRequest<'a> {
    pub broker_id: &'a str,
    pub broker_epoch: u64,
    pub worker_id: &'a str,
    pub claim_token: &'a str,
    pub job_id: &'a str,
    pub now_ms: u64,
    pub claim_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobHeartbeatRequest<'a> {
    pub broker_id: &'a str,
    pub broker_epoch: u64,
    pub job_id: &'a str,
    pub claim_token: &'a str,
    pub now_ms: u64,
    pub claim_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCompleteRequest<'a> {
    pub broker_id: &'a str,
    pub broker_epoch: u64,
    pub job_id: &'a str,
    pub claim_token: &'a str,
    pub now_ms: u64,
}

pub fn claim_job(
    shard: &mut QueueShardState,
    request: &JobClaimRequest<'_>,
) -> Result<JobClaimOutcome, WorkerMutationError> {
    ensure_active_broker_lease(
        shard,
        request.broker_id,
        request.broker_epoch,
        request.now_ms,
    )
    .map_err(WorkerMutationError::Broker)?;

    let job = shard
        .jobs
        .iter_mut()
        .find(|job| job.job_id == request.job_id)
        .ok_or_else(|| WorkerMutationError::JobNotFound {
            job_id: request.job_id.to_owned(),
        })?;

    let new_claim = QueueClaim {
        worker_id: request.worker_id.to_owned(),
        claim_token: request.claim_token.to_owned(),
        heartbeat_at_ms: request.now_ms,
        timeout_at_ms: request.now_ms.saturating_add(request.claim_timeout_ms),
    };

    match job.state {
        JobState::Ready => {
            job.state = JobState::Claimed;
            job.claim = Some(new_claim.clone());
            job.attempts = job.attempts.saturating_add(1);
            Ok(JobClaimOutcome::Claimed {
                claim_token: new_claim.claim_token,
            })
        }
        JobState::Claimed => {
            let current = job
                .claim
                .as_ref()
                .ok_or_else(|| WorkerMutationError::JobNotClaimed {
                    job_id: request.job_id.to_owned(),
                })?;
            if current.timeout_at_ms > request.now_ms {
                return Err(WorkerMutationError::JobBusy {
                    job_id: request.job_id.to_owned(),
                    worker_id: current.worker_id.clone(),
                    timeout_at_ms: current.timeout_at_ms,
                    now_ms: request.now_ms,
                });
            }

            job.claim = Some(new_claim.clone());
            job.attempts = job.attempts.saturating_add(1);
            Ok(JobClaimOutcome::Stolen {
                claim_token: new_claim.claim_token,
            })
        }
    }
}

pub fn heartbeat_job(
    shard: &mut QueueShardState,
    request: &JobHeartbeatRequest<'_>,
) -> Result<JobHeartbeatOutcome, WorkerMutationError> {
    ensure_active_broker_lease(
        shard,
        request.broker_id,
        request.broker_epoch,
        request.now_ms,
    )
    .map_err(WorkerMutationError::Broker)?;

    let job = shard
        .jobs
        .iter_mut()
        .find(|job| job.job_id == request.job_id)
        .ok_or_else(|| WorkerMutationError::JobNotFound {
            job_id: request.job_id.to_owned(),
        })?;
    let claim = job
        .claim
        .as_mut()
        .ok_or_else(|| WorkerMutationError::JobNotClaimed {
            job_id: request.job_id.to_owned(),
        })?;
    if claim.claim_token != request.claim_token {
        return Err(WorkerMutationError::ClaimTokenMismatch {
            expected: claim.claim_token.clone(),
            actual: request.claim_token.to_owned(),
        });
    }

    claim.heartbeat_at_ms = request.now_ms;
    claim.timeout_at_ms = request.now_ms.saturating_add(request.claim_timeout_ms);
    Ok(JobHeartbeatOutcome {
        claim_token: claim.claim_token.clone(),
        timeout_at_ms: claim.timeout_at_ms,
    })
}

pub fn complete_job(
    shard: &mut QueueShardState,
    request: &JobCompleteRequest<'_>,
) -> Result<JobCompleteOutcome, WorkerMutationError> {
    ensure_active_broker_lease(
        shard,
        request.broker_id,
        request.broker_epoch,
        request.now_ms,
    )
    .map_err(WorkerMutationError::Broker)?;

    let job_index = shard
        .jobs
        .iter()
        .position(|job| job.job_id == request.job_id)
        .ok_or_else(|| WorkerMutationError::JobNotFound {
            job_id: request.job_id.to_owned(),
        })?;

    {
        let claim = shard.jobs[job_index].claim.as_ref().ok_or_else(|| {
            WorkerMutationError::JobNotClaimed {
                job_id: request.job_id.to_owned(),
            }
        })?;
        if claim.claim_token != request.claim_token {
            return Err(WorkerMutationError::ClaimTokenMismatch {
                expected: claim.claim_token.clone(),
                actual: request.claim_token.to_owned(),
            });
        }
    }

    if let Some(follow_up) = shard.jobs[job_index].follow_up.take() {
        let job = &mut shard.jobs[job_index];
        job.state = JobState::Ready;
        job.payload = follow_up.clone();
        job.claim = None;
        return Ok(JobCompleteOutcome::PromotedFollowUp {
            through_seq: follow_up.through_seq,
        });
    }

    shard.jobs.remove(job_index);
    Ok(JobCompleteOutcome::Removed)
}

#[cfg(test)]
mod tests {
    use super::{
        claim_job, complete_job, heartbeat_job, JobClaimOutcome, JobClaimRequest,
        JobCompleteOutcome, JobCompleteRequest, JobHeartbeatRequest, WorkerMutationError,
    };
    use crate::queue::broker::renew_broker_lease;
    use crate::queue::types::{JobState, QueueJob, QueueShardState, SeqScopedPayload, WorkClass};
    use loon_types::{ChangeSeq, NamespaceId};

    #[test]
    fn claim_ready_job_records_claim_and_attempt() {
        let mut shard = ready_snapshot_shard();
        renew_broker_lease(&mut shard, "broker-a", 0, 10_000).expect("initial broker lease");

        let outcome = claim_job(
            &mut shard,
            &JobClaimRequest {
                broker_id: "broker-a",
                broker_epoch: 1,
                worker_id: "worker-a",
                claim_token: "claim-a",
                job_id: "job-1",
                now_ms: 0,
                claim_timeout_ms: 10_000,
            },
        )
        .expect("claim should succeed");

        assert_eq!(
            outcome,
            JobClaimOutcome::Claimed {
                claim_token: "claim-a".to_owned(),
            }
        );
        assert_eq!(shard.jobs[0].attempts, 1);
        assert_eq!(shard.jobs[0].state, JobState::Claimed);
    }

    #[test]
    fn heartbeat_extends_timeout_for_matching_claim() {
        let mut shard = ready_snapshot_shard();
        renew_broker_lease(&mut shard, "broker-a", 0, 10_000).expect("initial broker lease");
        claim_job(
            &mut shard,
            &JobClaimRequest {
                broker_id: "broker-a",
                broker_epoch: 1,
                worker_id: "worker-a",
                claim_token: "claim-a",
                job_id: "job-1",
                now_ms: 0,
                claim_timeout_ms: 10_000,
            },
        )
        .expect("claim should succeed");

        let outcome = heartbeat_job(
            &mut shard,
            &JobHeartbeatRequest {
                broker_id: "broker-a",
                broker_epoch: 1,
                job_id: "job-1",
                claim_token: "claim-a",
                now_ms: 5_000,
                claim_timeout_ms: 10_000,
            },
        )
        .expect("heartbeat should succeed");

        assert_eq!(outcome.timeout_at_ms, 15_000);
        assert_eq!(
            shard.jobs[0]
                .claim
                .as_ref()
                .expect("claimed job should keep claim")
                .timeout_at_ms,
            15_000
        );
    }

    #[test]
    fn timed_out_claim_can_be_stolen_and_stale_complete_is_rejected() {
        let mut shard = ready_snapshot_shard();
        renew_broker_lease(&mut shard, "broker-a", 0, 10_000).expect("initial broker lease");
        claim_job(
            &mut shard,
            &JobClaimRequest {
                broker_id: "broker-a",
                broker_epoch: 1,
                worker_id: "worker-a",
                claim_token: "claim-a",
                job_id: "job-1",
                now_ms: 0,
                claim_timeout_ms: 10_000,
            },
        )
        .expect("claim should succeed");

        renew_broker_lease(&mut shard, "broker-b", 30_000, 10_000)
            .expect("expired lease should allow takeover");
        assert_eq!(
            claim_job(
                &mut shard,
                &JobClaimRequest {
                    broker_id: "broker-b",
                    broker_epoch: 2,
                    worker_id: "worker-b",
                    claim_token: "claim-b",
                    job_id: "job-1",
                    now_ms: 30_000,
                    claim_timeout_ms: 10_000,
                },
            )
            .expect("timed-out claim should be stealable"),
            JobClaimOutcome::Stolen {
                claim_token: "claim-b".to_owned(),
            }
        );

        let stale_complete = complete_job(
            &mut shard,
            &JobCompleteRequest {
                broker_id: "broker-b",
                broker_epoch: 2,
                job_id: "job-1",
                claim_token: "claim-a",
                now_ms: 30_001,
            },
        )
        .unwrap_err();
        assert!(matches!(
            stale_complete,
            WorkerMutationError::ClaimTokenMismatch { .. }
        ));

        assert_eq!(
            complete_job(
                &mut shard,
                &JobCompleteRequest {
                    broker_id: "broker-b",
                    broker_epoch: 2,
                    job_id: "job-1",
                    claim_token: "claim-b",
                    now_ms: 30_001,
                },
            )
            .expect("fresh claim should complete"),
            JobCompleteOutcome::Removed
        );
        assert!(shard.jobs.is_empty());
    }

    fn ready_snapshot_shard() -> QueueShardState {
        QueueShardState {
            work_class: WorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![QueueJob {
                job_id: "job-1".to_owned(),
                dedupe_key: "BuildSnapshot:ns-1".to_owned(),
                state: JobState::Ready,
                payload: SeqScopedPayload {
                    namespace_id: NamespaceId::from("ns-1"),
                    through_seq: ChangeSeq(42),
                },
                follow_up: None,
                claim: None,
                attempts: 0,
            }],
        }
    }
}
