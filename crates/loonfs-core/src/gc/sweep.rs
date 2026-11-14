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
    /// The roots of the namespace.
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
            family.min_age_ms(),
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

/// Decides about one key under the prefix of a retired namespace, which has
/// no roots. The caller leaves out the boundary object, which is not a
/// candidate.
///
/// A key of a candidate family is deleted by the family's age. A snapshot
/// record is deleted once its listed modification time is at least the grace
/// window old, and the report counts it as a user snapshot, because the
/// collection doesn't read its owner. Any other key is kept.
pub(super) fn retired_candidate(
    entry: ListedObject,
    now_ms: u64,
    listed_bytes: &mut u64,
) -> Decision<GcResponse> {
    *listed_bytes += entry.size_bytes;
    if let Some(family) = CandidateFamily::ALL
        .into_iter()
        .find(|family| family.recognizes(&entry.key))
    {
        return Decision::aged(entry, family.min_age_ms(), now_ms, family.deleted_counter());
    }
    if parse_object_key(&entry.key).map(|parsed| parsed.family())
        == Some(DurableObjectFamily::Snapshot)
    {
        return Decision::aged(entry, GC_GRACE_WINDOW_MS, now_ms, |report| {
            &mut report.deleted_snapshots_by_owner.user
        });
    }
    Decision::retain(RetainedReason::UnrecognizedKey, None, now_ms)
}
