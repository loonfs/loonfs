//! The wire fields that say what a read reads at, shared by query
//! parameters and request bodies.

use loonfs_types::options::{Consistency, ReadAt, ReadConsistency};
use loonfs_types::PinId;

/// The fields that say what a read reads at: `snapshot_id` for a snapshot,
/// and the consistency fields of a live read that sets a consistency. A live
/// read that sets none sends neither, so the server's default applies.
pub(crate) struct ReadAtFields {
    pub(crate) snapshot_id: Option<PinId>,
    pub(crate) consistency: Option<ReadConsistency>,
    pub(crate) max_staleness_ms: Option<u64>,
}

impl From<&ReadAt> for ReadAtFields {
    fn from(at: &ReadAt) -> Self {
        let (snapshot_id, consistency, max_staleness_ms) = match at {
            ReadAt::Snapshot(snapshot_id) => (Some(snapshot_id.clone()), None, None),
            ReadAt::Live(None) => (None, None, None),
            ReadAt::Live(Some(Consistency::Strong)) => (None, Some(ReadConsistency::Strong), None),
            ReadAt::Live(Some(Consistency::Eventual { max_staleness_ms })) => {
                (None, Some(ReadConsistency::Eventual), *max_staleness_ms)
            }
        };
        Self {
            snapshot_id,
            consistency,
            max_staleness_ms,
        }
    }
}
