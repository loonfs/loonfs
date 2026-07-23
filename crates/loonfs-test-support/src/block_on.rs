//! Current-thread runtime setup for synchronous tests.

use std::future::Future;

/// Runs `future` to completion on a fresh current-thread Tokio runtime.
pub fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}
