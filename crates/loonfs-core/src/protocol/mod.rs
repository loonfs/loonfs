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

pub(crate) use self::batch::{
    publish_namespace_commits_batch_against_publish_view, PublishBatchAgainstViewResult,
};
pub(crate) use self::changes::list_changes_after;
pub use self::publish_view::PublishTailOptions;
pub(crate) use self::publish_view::{load_publish_metadata_view, PublishTailProjection};
pub use self::uploads::CompletedUpload;
pub(crate) use self::uploads::{
    abort_upload, begin_direct_put_upload_target, begin_upload, complete_upload,
    read_upload_status, upload_content,
};
