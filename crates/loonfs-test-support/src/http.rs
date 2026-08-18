//! HTTP helpers shared by server tests.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

/// Creates an HTTP client with read and write timeouts so a stalled server
/// fails the test instead of waiting indefinitely.
pub fn raw_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_read(std::time::Duration::from_secs(30))
        .timeout_write(std::time::Duration::from_secs(30))
        .build()
}

/// Retries an HTTP exchange when ureq hits a known macOS socket error.
///
/// This can happen when the server rejects a request before reading its body.
/// Only the matching EINVAL panic is retried. Other panics are rethrown, and
/// the final attempt runs normally so a persistent failure keeps its message.
#[allow(clippy::print_stderr)]
pub fn retry_on_macos_teardown_einval<T>(exchange: impl Fn() -> T) -> T {
    if !cfg!(target_os = "macos") {
        return exchange();
    }
    const ATTEMPTS: usize = 3;
    for _ in 1..ATTEMPTS {
        match catch_unwind(AssertUnwindSafe(&exchange)) {
            Ok(value) => return value,
            Err(payload) => {
                if !panic_is_teardown_einval(payload.as_ref()) {
                    resume_unwind(payload);
                }
                eprintln!("retrying: macOS reset the connection before the client's timeout reset");
            }
        }
    }
    exchange()
}

/// Returns true for the ureq panic caused by the macOS socket error.
fn panic_is_teardown_einval(payload: &(dyn std::any::Any + Send)) -> bool {
    let message = if let Some(owned) = payload.downcast_ref::<String>() {
        owned.as_str()
    } else if let Some(literal) = payload.downcast_ref::<&'static str>() {
        literal
    } else {
        return false;
    };
    message.contains("kind: InvalidInput") && message.contains("Invalid argument")
}
