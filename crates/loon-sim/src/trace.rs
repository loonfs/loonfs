#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceEvent {
    TimeAdvanced { now_ms: u64 },
    Message(String),
}
