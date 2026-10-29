//! Current-thread runtime setup for synchronous tests.

use std::future::Future;

thread_local! {
    static RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test runtime should build");
}

/// Runs `future` to completion on the calling thread's current-thread Tokio
/// runtime.
///
/// The runtime lasts as long as the thread, so the background tasks that a
/// handle spawns in one call keep running in the next call.
pub fn block_on<T>(future: impl Future<Output = T>) -> T {
    RUNTIME.with(|runtime| runtime.block_on(future))
}
