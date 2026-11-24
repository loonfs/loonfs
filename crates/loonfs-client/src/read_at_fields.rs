//! The wire fields that say what a read reads at, shared by query
//! parameters and request bodies.

use loonfs_types::options::{Consistency, ReadAt};
use loonfs_types::SnapshotId;

/// The fields that say what a read reads at: `snapshot_id` for a snapshot,
/// and `consistency` for a `strong` live read. An `eventual` live read sends
/// neither, because a live read without `consistency` is `eventual`.
pub(crate) struct ReadAtFields {
    pub(crate) snapshot_id: Option<SnapshotId>,
    pub(crate) consistency: Option<Consistency>,
}

impl From<&ReadAt> for ReadAtFields {
    fn from(at: &ReadAt) -> Self {
        let (snapshot_id, consistency) = match at {
            ReadAt::Snapshot(snapshot_id) => (Some(snapshot_id.clone()), None),
            ReadAt::Live(Consistency::Eventual) => (None, None),
            ReadAt::Live(Consistency::Strong) => (None, Some(Consistency::Strong)),
        };
        Self {
            snapshot_id,
            consistency,
        }
    }
}
