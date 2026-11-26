//! Composable object-store wrappers for test fault injection and observation.

mod before_store;
mod blocking_store;
mod buffer_watch_store;
mod concurrency_watch_store;
mod delegate;
mod fail_store;
mod fake_multipart_store;
mod intercept_store;
mod key_predicate;
mod latency_store;
mod lost_create_store;
mod memory_store;
mod metadata_map_store;
mod namespace_overlap_store;
mod newer_manifest_store;
mod operation;
mod panic_store;
mod recording_store;
mod replace;

pub use crate::delegate_object_store;
pub use before_store::{BeforeInterceptor, BeforeStore};
pub use blocking_store::{BlockingInterceptor, BlockingStore};
pub use buffer_watch_store::{BufferPeaks, BufferWatchStore};
pub use concurrency_watch_store::{
    ConcurrencyWatchInterceptor, ConcurrencyWatchStore, InFlightGuard, StoreConcurrency,
};
pub use fail_store::{FailInterceptor, FailStore, InjectedError};
pub use fake_multipart_store::{FakeMultipartStore, MultipartChecksumEnforcement};
pub use intercept_store::{Intercept, InterceptStore, Interceptor, Outcome};
pub use key_predicate::KeyPredicate;
pub use latency_store::{LatencyInterceptor, LatencyStore};
pub use lost_create_store::LostCreateStore;
pub use memory_store::MemoryStore;
pub use metadata_map_store::MetadataMapStore;
pub use namespace_overlap_store::{
    NamespaceInFlightGuard, NamespaceOverlapInterceptor, NamespaceOverlapStore,
};
pub use newer_manifest_store::{
    publish_next_manifest, NewerManifestInterceptor, NewerManifestStore,
};
pub use operation::{
    request_lines, sort_head_runs, write_keys, OperationContext, OperationKind, OperationSelector,
    RecordedOperation,
};
pub use panic_store::{PanicInterceptor, PanicStore};
pub use recording_store::{RecordedGet, RecordingInterceptor, RecordingStore, StoreCounts};
pub use replace::replace_object;

#[cfg(test)]
mod tests;
