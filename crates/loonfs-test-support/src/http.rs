//! HTTP-client setup shared by raw server tests.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

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

/// Runs one raw HTTP exchange, retrying it when macOS tears the socket
/// down under the client between the response head and the body read.
///
/// When the server answers without consuming the request body and then
/// closes the connection, the unread bytes make the close a TCP reset.
/// Once the reset lands, the client socket is shut down in both
/// directions, and macOS rejects every later setsockopt on it with
/// EINVAL, where Linux accepts them. ureq resets its socket read timeout
/// after parsing the response head and again when it pools the
/// connection, so losing that race turns an already-delivered response
/// into either a body read that fails with "Invalid argument (os error
/// 22)" or a panic inside ureq's own pool-return expect. The server
/// answered correctly in both shapes; only the client-side timeout
/// bookkeeping raced the teardown. Under load the window stretches from
/// microseconds to milliseconds and the race becomes reachable.
///
/// The retry stays honest about real failures. Only a panic carrying the
/// EINVAL signature is retried, so a wrong or missing envelope fails on
/// the first attempt, and the last attempt runs outside the catch so a
/// persistent failure surfaces with its real message.
// The breadcrumb prints straight to the test log so a tolerated race
// stays visible instead of vanishing into a silent retry.
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

/// Matches both shapes the teardown race takes inside ureq: the reader
/// that reports `Custom { kind: InvalidInput, error: "Invalid argument
/// (os error 22)" }` and the internal expect that panics with
/// `Os { code: 22, kind: InvalidInput, ... }`.
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
