//! The shared runtime core behind the purpose-specific handles: caches,
//! writer session identity, and the operation surface, each method a thin,
//! cache-aware delegation to `loonfs-core`.

mod core;
mod maintenance;
mod namespaces;
mod reads;
#[cfg(test)]
mod tests;
mod uploads;
mod writes;

pub(crate) use core::{should_invalidate_after_result, FsCore, FsInner};
