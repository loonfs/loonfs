use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SimActorId(String);

impl SimActorId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SimActorId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for SimActorId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for SimActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimDelivery {
    pub delivery_id: u64,
    pub sender: SimActorId,
    pub recipient: SimActorId,
    pub kind: String,
    pub enqueued_at_ms: u64,
    pub deliver_at_ms: u64,
}

impl SimDelivery {
    pub fn summary(&self) -> String {
        format!(
            "delivery_id={} kind={} sender={} recipient={} enqueued_at_ms={} deliver_at_ms={}",
            self.delivery_id,
            self.kind,
            self.sender,
            self.recipient,
            self.enqueued_at_ms,
            self.deliver_at_ms
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimTraceEvent {
    ActorStep {
        actor_id: SimActorId,
        step: String,
        now_ms: u64,
    },
    DeliveryEnqueued {
        delivery: SimDelivery,
    },
    DeliveryDelivered {
        delivery: SimDelivery,
        now_ms: u64,
    },
    ActorRestarted {
        actor_id: SimActorId,
        now_ms: u64,
    },
    TimeAdvanced {
        delta_ms: u64,
        now_ms: u64,
    },
    FaultInjected {
        fault: String,
        now_ms: u64,
    },
}

impl SimTraceEvent {
    pub fn render_line(&self) -> String {
        match self {
            Self::ActorStep {
                actor_id,
                step,
                now_ms,
            } => {
                format!("actor_step actor={} now_ms={} {}", actor_id, now_ms, step)
            }
            Self::DeliveryEnqueued { delivery } => {
                format!("delivery_enqueued {}", delivery.summary())
            }
            Self::DeliveryDelivered { delivery, now_ms } => {
                format!(
                    "delivery_delivered now_ms={} {}",
                    now_ms,
                    delivery.summary()
                )
            }
            Self::ActorRestarted { actor_id, now_ms } => {
                format!("actor_restarted actor={} now_ms={}", actor_id, now_ms)
            }
            Self::TimeAdvanced { delta_ms, now_ms } => {
                format!("time_advanced delta_ms={} now_ms={}", delta_ms, now_ms)
            }
            Self::FaultInjected { fault, now_ms } => {
                format!("fault_injected now_ms={} {}", now_ms, fault)
            }
        }
    }
}
