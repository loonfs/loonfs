//! The read core every handle shares, the writer-side state a write-capable
//! handle owns on top of it, and the operation surface — each method a thin,
//! cache-aware delegation to `loonfs-core`.

mod core;
mod maintenance;
mod namespaces;
mod reads;
#[cfg(test)]
mod tests;
mod uploads;
mod writes;

pub use maintenance::MetadataCompactionOutcome;

pub(crate) use core::{should_invalidate_after_result, ReadCore, WriterBits, WriterIdentity};
pub(crate) use namespaces::delete_namespace_with_engine;
pub(crate) use writes::publish_batch_with_engine;
