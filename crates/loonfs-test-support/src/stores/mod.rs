//! Composable object-store wrappers for test fault injection and observation.

mod blocking_store;
mod counting_store;
mod fail_store;
mod key_predicate;
mod metadata_map_store;
mod multipart_store;
mod operation;
mod recording_store;

pub use blocking_store::BlockingStore;
pub use counting_store::{CountingStore, StoreCounts};
pub use fail_store::{FailStore, FailureMode, InjectedError};
pub use key_predicate::KeyPredicate;
pub use metadata_map_store::MetadataMapStore;
pub use multipart_store::{MultipartChecksumEnforcement, MultipartStore};
pub use operation::{OperationClass, OperationContext, OperationKind};
pub use recording_store::{RecordedGet, RecordedOperation, RecordingStore};
