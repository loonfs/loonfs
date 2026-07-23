//! HTTP-client setup shared by raw server tests.

/// Raw-request agent with socket inactivity timeouts. A starved or wedged
/// server must produce a readable transport error, not an indefinite hang
/// or a panic inside the HTTP client's response path — the failure mode a
/// loaded CI runner once hit through the default timeout-less agent.
pub fn raw_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_read(std::time::Duration::from_secs(30))
        .timeout_write(std::time::Duration::from_secs(30))
        .build()
}
