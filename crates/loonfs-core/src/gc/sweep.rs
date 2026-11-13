//! The decisions of a namespace's collection about the objects that it lists
//! under the namespace's prefix.

use super::deletes::Decision;
use super::families::CandidateFamily;
use super::live_set::LiveSet;
use crate::time::OperationClock;
use loonfs_objectstore::ListedObject;
use loonfs_types::{GcResponse, RetainedReason};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct Sweep<'a> {
    pub(super) grace_window_ms: u64,
    pub(super) clock: &'a OperationClock,
    pub(super) live: &'a LiveSet,
    /// The bytes of the listed objects that reached this sweep, as the
    /// listings reported them.
    pub(super) listed_bytes: AtomicU64,
}

impl Sweep<'_> {
    pub(super) fn candidate(
        &self,
        family: CandidateFamily,
        entry: ListedObject,
    ) -> Decision<GcResponse> {
        self.listed_bytes
            .fetch_add(entry.size_bytes, Ordering::Relaxed);
        if !family.recognizes(&entry.key) {
            return self.retain(RetainedReason::UnrecognizedKey, None);
        }
        if self.live.protects(family, &entry.key) {
            return self.retain(RetainedReason::Referenced, None);
        }
        Decision::aged(
            entry,
            family.min_age_ms(self.grace_window_ms),
            self.clock.request_at_ms(),
            family.deleted_counter(),
        )
    }

    fn retain(
        &self,
        reason: RetainedReason,
        reclaimable_at_ms: Option<u64>,
    ) -> Decision<GcResponse> {
        Decision::retain(reason, reclaimable_at_ms, self.clock.request_at_ms())
    }
}
