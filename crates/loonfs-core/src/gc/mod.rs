//! Stateless garbage collection: a namespace's collection of its own objects
//! from its current manifest and pins, and a pool's collection of its
//! content, upload records, and temporary objects from the roots of its
//! members.

mod charged_set;
mod collect;
mod deletes;
mod families;
mod live_set;
mod options;
mod pinned;
mod pins;
mod pool;
mod retire;
mod session_cache;
mod sweep;
#[cfg(test)]
mod tests;

pub use options::GcOptions;
pub(crate) use pool::MemberState;
pub(crate) use session_cache::CompletedUploadSession;
pub use session_cache::{CompletedUploadSessionCache, SharedCompletedUploadSessions};
