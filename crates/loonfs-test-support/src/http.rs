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

/// Retries the known macOS socket panic caused by an early server disconnect.
pub fn retry_on_macos_teardown_einval<T>(exchange: impl Fn() -> T) -> T {
    match retry_result_on_macos_teardown_einval(|| Ok::<T, std::convert::Infallible>(exchange())) {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

/// Retries the same macOS socket failure when an HTTP exchange returns it.
#[allow(clippy::print_stderr)]
pub fn retry_result_on_macos_teardown_einval<T, E: std::fmt::Debug>(
    exchange: impl Fn() -> Result<T, E>,
) -> Result<T, E> {
    if !cfg!(target_os = "macos") {
        return exchange();
    }
    const ATTEMPTS: usize = 3;
    for _ in 1..ATTEMPTS {
        match catch_unwind(AssertUnwindSafe(&exchange)) {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => {
                if !message_is_teardown_einval(&format!("{error:?}")) {
                    return Err(error);
                }
            }
            Err(payload) => {
                if !panic_is_teardown_einval(payload.as_ref()) {
                    resume_unwind(payload);
                }
            }
        }
        eprintln!("retrying: macOS reset the connection before the client's timeout reset");
    }
    exchange()
}

fn panic_is_teardown_einval(payload: &(dyn std::any::Any + Send)) -> bool {
    let message = if let Some(owned) = payload.downcast_ref::<String>() {
        owned.as_str()
    } else if let Some(literal) = payload.downcast_ref::<&'static str>() {
        literal
    } else {
        return false;
    };
    message_is_teardown_einval(message)
}

fn message_is_teardown_einval(message: &str) -> bool {
    message.contains("Invalid argument")
        && (message.contains("kind: InvalidInput") || message.contains("os error 22"))
}
