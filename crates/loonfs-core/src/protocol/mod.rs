//! Object-store implementations for uploads, commit publication, and the
//! change feed. [`NamespaceEngine`](crate::engine::NamespaceEngine) exposes
//! the supported entry points.

mod batch;
mod candidates;

pub(crate) use candidates::validate_inline_content_references;
mod changes;
mod publish_view;
mod uploads;

pub(crate) use self::batch::{
    publish_namespace_commits_batch_against_publish_view, PublicationClock, PublishViewEffect,
};
#[cfg(test)]
pub(crate) use self::changes::events_from_wal_deltas;
pub(crate) use self::changes::list_changes_after;
pub(crate) use self::publish_view::PublishMetadataView;
pub use self::publish_view::WriterPosition;
pub(crate) use self::uploads::AbandonedUpload;
pub use self::uploads::{
    CompletedUpload, DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTarget,
    MultipartPartTargets, UploadSessionView,
};
