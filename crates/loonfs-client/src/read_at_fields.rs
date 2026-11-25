//! The wire fields that say what a read reads, shared by query parameters
//! and request bodies.

use crate::transport::QueryBuilder;
use loonfs_types::options::Consistency;
use loonfs_types::SnapshotId;

/// The fields that say what a read reads: `snapshot_id` for a read through a
/// [`SnapshotReader`](crate::SnapshotReader), and `consistency` for a
/// `strong` read through a [`Client::strong`](crate::Client::strong) copy. An
/// `eventual` live read sends neither, because a live read without
/// `consistency` is `eventual`.
#[derive(Debug, Clone)]
pub(crate) struct ReadAtFields {
    pub(crate) snapshot_id: Option<SnapshotId>,
    pub(crate) consistency: Option<Consistency>,
}

impl ReadAtFields {
    /// Returns the fields of a read of the live head at `consistency`.
    pub(crate) fn head(consistency: Consistency) -> Self {
        Self {
            snapshot_id: None,
            consistency: match consistency {
                Consistency::Eventual => None,
                Consistency::Strong => Some(Consistency::Strong),
            },
        }
    }

    /// Returns the fields of a read of `snapshot_id`.
    pub(crate) fn snapshot(snapshot_id: SnapshotId) -> Self {
        Self {
            snapshot_id: Some(snapshot_id),
            consistency: None,
        }
    }

    /// Adds these fields to a request's query parameters.
    pub(crate) fn push(&self, query: &mut QueryBuilder) {
        if let Some(snapshot_id) = &self.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        if self.consistency == Some(Consistency::Strong) {
            query.push("consistency", "strong");
        }
    }
}
