use crate::trace::TraceEvent;

#[derive(Debug, Default)]
pub struct SimRuntime {
    pub now_ms: u64,
    pub trace: Vec<TraceEvent>,
}

impl SimRuntime {
    pub fn advance_time(&mut self, delta_ms: u64) {
        self.now_ms += delta_ms;
        self.trace.push(TraceEvent::TimeAdvanced {
            now_ms: self.now_ms,
        });
    }
}
