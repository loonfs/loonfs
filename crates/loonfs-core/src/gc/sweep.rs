//! The decisions of a namespace's collection about the objects that it lists
//! under the namespace's prefix.

use super::deletes::Decision;
use super::families::CandidateFamily;
use super::live_set::LiveSet;
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::time::OperationClock;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::ListedObject;
use loonfs_types::{GcResponse, RetainedReason};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct Sweep<'a> {
    pub(super) clock: &'a OperationClock,
    /// The roots of the namespace, or `None` when the namespace is retired
    /// and has no roots.
    pub(super) live: Option<&'a LiveSet>,
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
        if self
            .live
            .is_some_and(|live| live.protects(family, &entry.key))
        {
            return self.retain(RetainedReason::Referenced, None);
        }
        Decision::aged(
            entry,
            family.min_age_ms(),
            self.clock.request_at_ms(),
            family.deleted_counter(),
        )
    }

    /// Decides about one key under the snapshot prefix of a retired
    /// namespace. A snapshot record is deleted once its listed modification
    /// time is older than the grace window, and the report counts it as a
    /// user snapshot, because the collection doesn't read its owner.
    pub(super) fn retired_snapshot(&self, entry: ListedObject) -> Decision<GcResponse> {
        self.listed_bytes
            .fetch_add(entry.size_bytes, Ordering::Relaxed);
        if parse_object_key(&entry.key).map(|parsed| parsed.family())
            != Some(DurableObjectFamily::Snapshot)
        {
            return self.retain(RetainedReason::UnrecognizedKey, None);
        }
        Decision::aged(
            entry,
            GC_GRACE_WINDOW_MS,
            self.clock.request_at_ms(),
            |report| &mut report.deleted_snapshots_by_owner.user,
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
