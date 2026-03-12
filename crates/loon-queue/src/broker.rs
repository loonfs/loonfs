use crate::types::{QueueBroker, QueueJob, QueueShardState, SeqScopedPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerLeaseError {
    LeaseHeldByOther {
        active_broker_id: String,
        active_epoch: u64,
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerLeaseOutcome {
    Acquired { epoch: u64 },
    Renewed { epoch: u64 },
    TakenOver { epoch: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerLeaseGuardError {
    MissingBrokerLease,
    BrokerLeaseMismatch {
        expected_broker_id: String,
        expected_epoch: u64,
        actual_broker_id: String,
        actual_epoch: u64,
    },
    BrokerLeaseExpired {
        broker_id: String,
        epoch: u64,
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
}

pub fn merge_seq_payload(existing: &mut SeqScopedPayload, incoming: SeqScopedPayload) {
    if incoming.through_seq.0 > existing.through_seq.0 {
        existing.through_seq = incoming.through_seq;
    }
}

pub fn attach_follow_up(job: &mut QueueJob, incoming: SeqScopedPayload) {
    match &mut job.follow_up {
        Some(existing) => merge_seq_payload(existing, incoming),
        None => job.follow_up = Some(incoming),
    }
}

pub fn renew_broker_lease(
    shard: &mut QueueShardState,
    broker_id: &str,
    now_ms: u64,
    lease_duration_ms: u64,
) -> Result<BrokerLeaseOutcome, BrokerLeaseError> {
    match &mut shard.broker {
        None => {
            shard.broker = Some(QueueBroker {
                broker_id: broker_id.to_owned(),
                epoch: 1,
                lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
            });
            Ok(BrokerLeaseOutcome::Acquired { epoch: 1 })
        }
        Some(current) if current.broker_id == broker_id && current.lease_expires_at_ms > now_ms => {
            current.lease_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
            Ok(BrokerLeaseOutcome::Renewed {
                epoch: current.epoch,
            })
        }
        Some(current) if current.lease_expires_at_ms <= now_ms => {
            current.broker_id = broker_id.to_owned();
            current.epoch = current.epoch.saturating_add(1);
            current.lease_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
            Ok(BrokerLeaseOutcome::TakenOver {
                epoch: current.epoch,
            })
        }
        Some(current) => Err(BrokerLeaseError::LeaseHeldByOther {
            active_broker_id: current.broker_id.clone(),
            active_epoch: current.epoch,
            lease_expires_at_ms: current.lease_expires_at_ms,
            now_ms,
        }),
    }
}

pub(crate) fn ensure_active_broker_lease(
    shard: &QueueShardState,
    broker_id: &str,
    broker_epoch: u64,
    now_ms: u64,
) -> Result<(), BrokerLeaseGuardError> {
    let current = shard
        .broker
        .as_ref()
        .ok_or(BrokerLeaseGuardError::MissingBrokerLease)?;
    if current.broker_id != broker_id || current.epoch != broker_epoch {
        return Err(BrokerLeaseGuardError::BrokerLeaseMismatch {
            expected_broker_id: broker_id.to_owned(),
            expected_epoch: broker_epoch,
            actual_broker_id: current.broker_id.clone(),
            actual_epoch: current.epoch,
        });
    }
    if current.lease_expires_at_ms <= now_ms {
        return Err(BrokerLeaseGuardError::BrokerLeaseExpired {
            broker_id: current.broker_id.clone(),
            epoch: current.epoch,
            lease_expires_at_ms: current.lease_expires_at_ms,
            now_ms,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ensure_active_broker_lease, renew_broker_lease, BrokerLeaseOutcome};
    use crate::types::{QueueShardState, WorkClass};

    #[test]
    fn broker_lease_takeover_increments_epoch() {
        let mut shard = empty_snapshot_shard();

        assert_eq!(
            renew_broker_lease(&mut shard, "broker-a", 0, 10_000).expect("initial acquire"),
            BrokerLeaseOutcome::Acquired { epoch: 1 }
        );
        assert_eq!(
            renew_broker_lease(&mut shard, "broker-a", 5_000, 10_000).expect("renew same broker"),
            BrokerLeaseOutcome::Renewed { epoch: 1 }
        );
        assert_eq!(
            renew_broker_lease(&mut shard, "broker-b", 30_000, 10_000)
                .expect("expired lease should allow takeover"),
            BrokerLeaseOutcome::TakenOver { epoch: 2 }
        );
        assert_eq!(
            shard
                .broker
                .as_ref()
                .expect("takeover should keep broker state")
                .broker_id,
            "broker-b"
        );
    }

    #[test]
    fn active_broker_guard_rejects_expired_generation() {
        let mut shard = empty_snapshot_shard();
        renew_broker_lease(&mut shard, "broker-a", 0, 10_000).expect("initial acquire");

        assert!(ensure_active_broker_lease(&shard, "broker-a", 1, 9_999).is_ok());
        assert!(ensure_active_broker_lease(&shard, "broker-a", 1, 10_000).is_err());
    }

    fn empty_snapshot_shard() -> QueueShardState {
        QueueShardState {
            work_class: WorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![],
        }
    }
}
