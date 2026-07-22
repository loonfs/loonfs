//! Deterministic virtual clock primitives for simulation scenarios.

use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimInstant {
    nanos_since_epoch: u64,
}

impl SimInstant {
    pub const fn from_nanos(nanos_since_epoch: u64) -> Self {
        Self { nanos_since_epoch }
    }

    pub const fn as_nanos(self) -> u64 {
        self.nanos_since_epoch
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimDuration {
    nanos: u64,
}

impl SimDuration {
    pub const fn from_nanos(nanos: u64) -> Self {
        Self { nanos }
    }

    pub const fn from_millis(millis: u64) -> Self {
        Self {
            nanos: millis.saturating_mul(1_000_000),
        }
    }

    pub const fn as_nanos(self) -> u64 {
        self.nanos
    }
}

pub trait SimClock: Clone + Send + Sync + 'static {
    fn now(&self) -> SimInstant;
    fn advance(&self, delta: SimDuration);
}

#[derive(Debug, Clone, Default)]
pub struct DeterministicClock {
    nanos_since_epoch: Arc<Mutex<u64>>,
}

impl DeterministicClock {
    pub fn new(start: SimInstant) -> Self {
        Self {
            nanos_since_epoch: Arc::new(Mutex::new(start.as_nanos())),
        }
    }
}

impl SimClock for DeterministicClock {
    fn now(&self) -> SimInstant {
        SimInstant::from_nanos(*self.nanos_since_epoch.lock().expect("clock lock poisoned"))
    }

    fn advance(&self, delta: SimDuration) {
        let mut now = self.nanos_since_epoch.lock().expect("clock lock poisoned");
        *now = now.saturating_add(delta.as_nanos());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_clock_advances_only_when_requested() {
        let clock = DeterministicClock::default();
        assert_eq!(clock.now(), SimInstant::from_nanos(0));

        clock.advance(SimDuration::from_millis(2));
        assert_eq!(clock.now(), SimInstant::from_nanos(2_000_000));

        let cloned = clock.clone();
        assert_eq!(cloned.now(), SimInstant::from_nanos(2_000_000));
        cloned.advance(SimDuration::from_nanos(7));
        assert_eq!(clock.now(), SimInstant::from_nanos(2_000_007));
    }
}
