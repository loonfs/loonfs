use crate::{
    ModelBrokerLeaseOutcome, ModelError, ModelJobClaimOutcome, ModelJobClaimParams,
    ModelJobCompleteOutcome, ModelQueueBroker, ModelQueueClaim, ModelQueueJobState,
    ModelQueueShard,
};
use loon_types::NamespaceId;

impl ModelQueueShard {
    pub fn renew_broker_lease(
        &mut self,
        broker_id: &str,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<ModelBrokerLeaseOutcome, ModelError> {
        match &mut self.broker {
            None => {
                self.broker = Some(ModelQueueBroker {
                    broker_id: broker_id.to_owned(),
                    epoch: 1,
                    lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
                });

                Ok(ModelBrokerLeaseOutcome::Acquired { epoch: 1 })
            }
            Some(current)
                if current.broker_id == broker_id && current.lease_expires_at_ms > now_ms =>
            {
                current.lease_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
                Ok(ModelBrokerLeaseOutcome::Renewed {
                    epoch: current.epoch,
                })
            }
            Some(current) if current.lease_expires_at_ms <= now_ms => {
                current.broker_id = broker_id.to_owned();
                current.epoch = current.epoch.saturating_add(1);
                current.lease_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
                Ok(ModelBrokerLeaseOutcome::TakenOver {
                    epoch: current.epoch,
                })
            }
            Some(current) => Err(ModelError::BrokerLeaseHeldByOther {
                active_broker_id: current.broker_id.clone(),
                active_epoch: current.epoch,
                lease_expires_at_ms: current.lease_expires_at_ms,
                now_ms,
            }),
        }
    }

    pub fn claim_job(
        &mut self,
        job_id: &str,
        params: &ModelJobClaimParams,
    ) -> Result<ModelJobClaimOutcome, ModelError> {
        ensure_active_broker_lease(self, &params.broker_id, params.broker_epoch, params.now_ms)?;

        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .ok_or_else(|| ModelError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;

        let new_claim = ModelQueueClaim {
            worker_id: params.worker_id.clone(),
            claim_token: params.claim_token.clone(),
            heartbeat_at_ms: params.now_ms,
            timeout_at_ms: params.now_ms.saturating_add(params.claim_timeout_ms),
        };

        match job.state {
            ModelQueueJobState::Ready => {
                job.state = ModelQueueJobState::Claimed;
                job.claim = Some(new_claim.clone());
                job.attempts = job.attempts.saturating_add(1);
                Ok(ModelJobClaimOutcome::Claimed {
                    claim_token: new_claim.claim_token,
                })
            }
            ModelQueueJobState::Claimed => {
                let current = job
                    .claim
                    .as_ref()
                    .ok_or_else(|| ModelError::JobNotClaimed {
                        job_id: job_id.to_owned(),
                    })?;
                if current.timeout_at_ms > params.now_ms {
                    return Err(ModelError::JobBusy {
                        job_id: job_id.to_owned(),
                        worker_id: current.worker_id.clone(),
                        timeout_at_ms: current.timeout_at_ms,
                        now_ms: params.now_ms,
                    });
                }

                job.claim = Some(new_claim.clone());
                job.attempts = job.attempts.saturating_add(1);
                Ok(ModelJobClaimOutcome::Stolen {
                    claim_token: new_claim.claim_token,
                })
            }
        }
    }

    pub fn heartbeat_job(
        &mut self,
        broker_id: &str,
        broker_epoch: u64,
        job_id: &str,
        claim_token: &str,
        now_ms: u64,
        claim_timeout_ms: u64,
    ) -> Result<(), ModelError> {
        ensure_active_broker_lease(self, broker_id, broker_epoch, now_ms)?;

        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .ok_or_else(|| ModelError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        let claim = job
            .claim
            .as_mut()
            .ok_or_else(|| ModelError::JobNotClaimed {
                job_id: job_id.to_owned(),
            })?;
        if claim.claim_token != claim_token {
            return Err(ModelError::ClaimTokenMismatch {
                expected: claim.claim_token.clone(),
                actual: claim_token.to_owned(),
            });
        }

        claim.heartbeat_at_ms = now_ms;
        claim.timeout_at_ms = now_ms.saturating_add(claim_timeout_ms);
        Ok(())
    }

    pub fn complete_job(
        &mut self,
        broker_id: &str,
        broker_epoch: u64,
        job_id: &str,
        claim_token: &str,
        now_ms: u64,
    ) -> Result<ModelJobCompleteOutcome, ModelError> {
        ensure_active_broker_lease(self, broker_id, broker_epoch, now_ms)?;

        let job_index = self
            .jobs
            .iter()
            .position(|job| job.job_id == job_id)
            .ok_or_else(|| ModelError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;

        {
            let claim =
                self.jobs[job_index]
                    .claim
                    .as_ref()
                    .ok_or_else(|| ModelError::JobNotClaimed {
                        job_id: job_id.to_owned(),
                    })?;
            if claim.claim_token != claim_token {
                return Err(ModelError::ClaimTokenMismatch {
                    expected: claim.claim_token.clone(),
                    actual: claim_token.to_owned(),
                });
            }
        }

        if let Some(follow_up) = self.jobs[job_index].follow_up.take() {
            let job = &mut self.jobs[job_index];
            job.state = ModelQueueJobState::Ready;
            job.payload = follow_up.clone();
            job.claim = None;
            return Ok(ModelJobCompleteOutcome::PromotedFollowUp {
                through_seq: follow_up.through_seq,
            });
        }

        self.jobs.remove(job_index);
        Ok(ModelJobCompleteOutcome::Removed)
    }
}

pub(crate) fn ensure_active_broker_lease(
    queue: &ModelQueueShard,
    broker_id: &str,
    broker_epoch: u64,
    now_ms: u64,
) -> Result<(), ModelError> {
    let broker = queue
        .broker
        .as_ref()
        .ok_or(ModelError::MissingBrokerLease)?;

    if broker.broker_id != broker_id || broker.epoch != broker_epoch {
        return Err(ModelError::BrokerLeaseMismatch {
            expected_broker_id: broker.broker_id.clone(),
            expected_epoch: broker.epoch,
            actual_broker_id: broker_id.to_owned(),
            actual_epoch: broker_epoch,
        });
    }

    if broker.lease_expires_at_ms <= now_ms {
        return Err(ModelError::BrokerLeaseExpired {
            broker_id: broker.broker_id.clone(),
            epoch: broker.epoch,
            lease_expires_at_ms: broker.lease_expires_at_ms,
            now_ms,
        });
    }

    Ok(())
}

pub(crate) fn build_snapshot_work_class() -> &'static str {
    "BuildSnapshot"
}

pub(crate) fn build_snapshot_dedupe_key(namespace_id: &NamespaceId) -> String {
    format!("{}:{namespace_id}", build_snapshot_work_class())
}

pub(crate) fn build_snapshot_repair_job_id(namespace_id: &NamespaceId) -> String {
    format!("repair-{}-{namespace_id}", build_snapshot_work_class())
}
