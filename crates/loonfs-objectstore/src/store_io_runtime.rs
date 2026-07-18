//! The per-store HTTP IO runtime: provider requests are driven by a thread
//! this runtime owns, never by the caller's runtime.
//!
//! The default provider connector performs HTTP IO on whichever runtime
//! issues the request. A store shared across current-thread runtimes then
//! parks pooled connections until the client's 30s request timeout fires —
//! reproduced and fixed in the S3 benchmark investigation by routing IO
//! through a dedicated runtime. Every provider client is constructed with a
//! connector onto its store's own runtime, so caller runtime topology can
//! never affect provider IO.

use crate::object_store::Result;
use crate::transfer_timeouts::TransferTimeoutConnector;
use crate::ObjectStoreError;
use std::fmt;
use std::sync::Arc;

/// Owns the Tokio runtime that drives one configured store's HTTP IO.
///
/// The provider connector holds only a runtime `Handle`; this type keeps the
/// runtime itself alive for as long as the provider client lives. Clones
/// share one runtime. Dropping the last clone shuts the runtime down in the
/// background, so a store dropped inside an async context never blocks or
/// panics.
#[derive(Clone)]
pub(crate) struct StoreIoRuntime {
    inner: Arc<OwnedRuntime>,
}

struct OwnedRuntime {
    runtime: Option<tokio::runtime::Runtime>,
}

impl StoreIoRuntime {
    /// Starts the IO runtime for one configured store.
    ///
    /// One worker thread multiplexes every request for the store; HTTP IO is
    /// reactor-driven, so request concurrency does not need more threads.
    pub(crate) fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("loonfs-store-io")
            .enable_all()
            .build()
            .map_err(|error| {
                ObjectStoreError::Configuration(format!(
                    "failed to start store io runtime: {error}"
                ))
            })?;
        Ok(Self {
            inner: Arc::new(OwnedRuntime {
                runtime: Some(runtime),
            }),
        })
    }

    /// Connector that routes a provider client's requests onto this runtime
    /// and applies the transfer-aware timeout scheme
    /// ([`crate::transfer_timeouts`]).
    pub(crate) fn connector(&self) -> TransferTimeoutConnector {
        let handle = self
            .inner
            .runtime
            .as_ref()
            .expect("store io runtime should live until the last store clone drops")
            .handle()
            .clone();
        TransferTimeoutConnector::new(handle)
    }
}

impl fmt::Debug for StoreIoRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreIoRuntime").finish_non_exhaustive()
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // A store may be dropped inside an async context, where a
            // blocking runtime drop panics; background shutdown never blocks.
            runtime.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StoreIoRuntime;

    #[tokio::test]
    async fn store_io_runtime_drops_safely_inside_an_async_context() {
        let runtime = StoreIoRuntime::new().expect("start io runtime");
        let clone = runtime.clone();
        drop(runtime);
        drop(clone);
    }

    #[tokio::test]
    async fn connector_construction_does_not_touch_the_caller_runtime() {
        let runtime = StoreIoRuntime::new().expect("start io runtime");
        let _connector = runtime.connector();
    }
}
