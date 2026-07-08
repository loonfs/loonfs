//! Namespace protocol operations: upload sessions, publish batches, and the
//! change feed.
//!
//! These are the object-store implementations behind
//! [`NamespaceEngine`](crate::engine::NamespaceEngine) calls; the module tree
//! is crate-internal, so callers reach every operation through the re-exports
//! below. Submodules follow the life of a mutation:
//!
//! - [`uploads`] stages content before metadata can reference it: begin,
//!   stage, and complete durable upload sessions, including direct-put
//!   targets that move bytes past the server.
//! - [`publish_view`] loads the publish-time metadata view: the current head
//!   plus the WAL tail replayed over the manifest, with head-etag freshness
//!   checks against concurrent publishers.
//! - [`candidates`] admits one batch candidate at a time: request conversion,
//!   commit-id validation, and duplicate resolution against durable receipts
//!   and same-batch primaries.
//! - [`batch`] publishes admitted candidates as one WAL segment plus one head
//!   compare-and-swap, then fans outcomes back to every candidate slot.
//! - [`changes`] reads committed changes after a sequence number and converts
//!   durable WAL deltas to API deltas.

mod batch;
mod candidates;
mod changes;
mod publish_view;
mod uploads;

pub use self::batch::ContentDurabilityGate;
pub(crate) use self::batch::{
    commit_operations, commit_operations_batch,
    publish_namespace_mutations_batch_against_publish_view, PublishBatchAgainstViewResult,
};
// Consumed only by publisher-crate tests that drive the publish budget.
#[cfg(test)]
pub(crate) use self::batch::PUBLISH_BUDGET_MS;
pub(crate) use self::changes::list_changes_after;
pub(crate) use self::publish_view::{
    load_publish_metadata_view, PublishTailOptions, PublishTailProjection,
};
pub(crate) use self::uploads::{
    begin_direct_put_upload_target, begin_upload, complete_upload, upload_content,
};
