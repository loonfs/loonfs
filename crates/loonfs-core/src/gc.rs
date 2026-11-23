//! Stateless garbage collection: a namespace's collection of its own objects
//! from its current manifest and snapshots, and a pool's collection of its
//! content, upload records, and temporary objects from the roots of its
//! members.

mod charged_set;
mod collect;
mod deletes;
mod families;
mod live_set;
mod pool;
mod retire;
mod roots;
mod session_cache;
mod snapshots;
mod sweep;
#[cfg(test)]
mod tests;

pub(crate) use roots::RetirementStatus;
pub(crate) use session_cache::CompletedUploadSession;
pub use session_cache::{CompletedUploadSessionCache, SharedCompletedUploadSessions};
