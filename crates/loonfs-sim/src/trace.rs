//! Serializable simulation traces and their shared recorder.

use crate::fault::ObjectStoreFault;
use crate::object_op::ObjectOp;
use crate::rng::SimSeed;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimTrace {
    pub run: RunId,
    pub seed: SimSeed,
    pub events: Vec<SimTraceEvent>,
}

impl SimTrace {
    pub fn new(run: RunId, seed: SimSeed) -> Self {
        Self {
            run,
            seed,
            events: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimTraceEvent {
    pub step: u64,
    pub actor: Option<String>,
    pub operation: String,
    pub object_op: Option<ObjectOp>,
    pub injected_fault: Option<ObjectStoreFault>,
    pub result: SimEventResult,
    pub model_hash: Option<String>,
    pub core_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum SimEventResult {
    Ok,
    Error { class: String },
    Skipped { reason: String },
}

#[derive(Debug, Clone)]
pub struct SharedSimTrace {
    inner: Arc<Mutex<SimTrace>>,
}

impl SharedSimTrace {
    pub fn new(trace: SimTrace) -> Self {
        Self {
            inner: Arc::new(Mutex::new(trace)),
        }
    }

    pub fn empty(run: RunId, seed: SimSeed) -> Self {
        Self::new(SimTrace::new(run, seed))
    }

    pub fn push(&self, event: SimTraceEvent) {
        self.inner
            .lock()
            .expect("sim trace lock poisoned")
            .events
            .push(event);
    }

    pub fn snapshot(&self) -> SimTrace {
        self.inner.lock().expect("sim trace lock poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_trace_records_events() {
        let trace = SharedSimTrace::empty(RunId("run".to_owned()), SimSeed(1));
        trace.push(SimTraceEvent {
            step: 1,
            actor: Some("writer-1".to_owned()),
            operation: "put".to_owned(),
            object_op: None,
            injected_fault: None,
            result: SimEventResult::Ok,
            model_hash: None,
            core_hash: None,
        });

        assert_eq!(trace.snapshot().events.len(), 1);
    }
}
