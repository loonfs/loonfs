//! Garbage collection requests and reports for the v0 HTTP API.

use crate::{NamespaceId, PoolId};
use serde::{Deserialize, Serialize};

/// Optional overrides for one garbage collection.
///
/// The server rejects unknown fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    /// The minimum age, in milliseconds, that an object must reach before the collection can delete
    /// it. The value must be at least the server's advertised safety floor. The grep index's
    /// collector keeps its own age rules, so this value doesn't change them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub grace_window_ms: Option<u64>,
}

/// The candidates that one garbage collection of a namespace inspected but didn't delete,
/// counted by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RetainedCandidates {
    /// The candidates that a current root protected, or that manifest discovery needed.
    pub referenced: u64,
    /// The unreferenced candidates that were younger than the grace window, according to their
    /// provider timestamps.
    pub within_grace_window: u64,
    /// The unreferenced candidates that had no provider timestamp, so their age was unknown.
    pub no_provider_timestamp: u64,
    /// The keys that garbage collection didn't recognize in the object families it scanned. The
    /// collection keeps these objects.
    pub unrecognized_key: u64,
    /// The snapshot records that the collection couldn't delete because of their owner or their
    /// grace window.
    pub snapshot_not_deletable: u64,
}

/// The candidates that one garbage collection of a pool inspected but didn't delete, counted
/// by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PoolRetainedCandidates {
    /// The content objects that the roots of a pool member protected. A content object is
    /// protected when an extent filter of a rooted layout segment matches it, and a false match
    /// keeps the object until no rooted view lists that segment.
    pub referenced: u64,
    /// The unreferenced candidates that were younger than the grace window, according to their
    /// provider timestamps.
    pub within_grace_window: u64,
    /// The unreferenced candidates that had no provider timestamp, so their age was unknown.
    pub no_provider_timestamp: u64,
    /// The keys that garbage collection didn't recognize in the object families it scanned. The
    /// collection keeps these objects.
    pub unrecognized_key: u64,
    /// The upload sessions that a lease or grace window still protected.
    pub upload_session_window: u64,
    /// The upload sessions that the collection couldn't prove safe to delete.
    pub upload_session_undecided: u64,
}

/// The number of objects that one garbage collection of a namespace deleted, grouped by
/// object family.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedObjectCounts {
    /// The unreferenced WAL objects that the collection deleted.
    pub wal_objects: u64,
    /// The unreferenced metadata segments that the collection deleted.
    pub metadata_segments: u64,
    /// The unreferenced extension segments, such as the grep index's segments, that the collection
    /// deleted.
    pub extension_segments: u64,
    /// The unreferenced manifests that the collection deleted.
    pub manifests: u64,
}

impl DeletedObjectCounts {
    /// Adds counts from another collection.
    pub fn add(&mut self, other: &Self) {
        let Self {
            wal_objects,
            metadata_segments,
            extension_segments,
            manifests,
        } = other;
        self.wal_objects += wal_objects;
        self.metadata_segments += metadata_segments;
        self.extension_segments += extension_segments;
        self.manifests += manifests;
    }
}

/// The number of objects that one garbage collection of a pool deleted, grouped by object
/// family.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PoolDeletedObjectCounts {
    /// The content objects the collection deleted because no member's retained view referenced them
    /// and they were older than the grace window.
    pub content_objects: u64,
    /// The upload sessions whose records the collection deleted after their grace windows passed.
    pub upload_sessions: u64,
    /// The temporary objects that the collection deleted because they were older than the grace
    /// window.
    pub temporary_objects: u64,
}

impl PoolDeletedObjectCounts {
    /// Adds counts from another collection.
    pub fn add(&mut self, other: &Self) {
        let Self {
            content_objects,
            upload_sessions,
            temporary_objects,
        } = other;
        self.content_objects += content_objects;
        self.upload_sessions += upload_sessions;
        self.temporary_objects += temporary_objects;
    }
}

/// The number of snapshot records that one garbage collection deleted, grouped by owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedSnapshotsByOwner {
    /// The user snapshots that the collection deleted after they expired or their namespace was deleted.
    pub user: u64,
    /// The fork snapshots that the collection deleted because their target namespaces no longer exist.
    pub fork: u64,
    /// The operation snapshots that the collection deleted after they expired or their namespace
    /// was deleted.
    pub operation: u64,
}

impl DeletedSnapshotsByOwner {
    /// Adds counts from another collection.
    pub fn add(&mut self, other: &Self) {
        let Self {
            user,
            fork,
            operation,
        } = other;
        self.user += user;
        self.fork += fork;
        self.operation += operation;
    }
}

/// The result of one stateless garbage collection call on a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// The namespace that the collection ran against.
    pub namespace_id: NamespaceId,
    /// The objects that the collection deleted, counted by object family.
    pub deleted: DeletedObjectCounts,
    /// The snapshot records that the collection deleted, counted by owner.
    pub deleted_snapshots_by_owner: DeletedSnapshotsByOwner,
    /// The candidates that the collection kept at deletion time, counted by reason.
    pub retained: RetainedCandidates,
    /// The bytes of every object that the collection listed under the namespace, each object once,
    /// before the collection's deletes.
    pub listed_bytes: u64,
    /// The earliest future retirement deadline or snapshot deletion time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_reclamation_at_ms: Option<u64>,
    /// The deletion time of the current tombstone plus the configured retirement grace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub reclaimable_at_ms: Option<u64>,
}

/// The result of one stateless garbage collection call on a pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PoolGcResponse {
    /// The pool that the collection ran against.
    pub pool_id: PoolId,
    /// The objects that the collection deleted, counted by object family.
    pub deleted: PoolDeletedObjectCounts,
    /// The candidates that the collection kept at deletion time, counted by reason.
    pub retained: PoolRetainedCandidates,
    /// The bytes of every object that the collection listed under the pool, each object once,
    /// before the collection's deletes.
    pub listed_bytes: u64,
    /// The earliest future upload cleanup time, or the end of the pool record's creation grace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_reclamation_at_ms: Option<u64>,
    /// The work that this collection skipped at a limit, by reason. Empty when the collection
    /// skipped nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<GcSkippedWork>,
}

/// A part of a collection of a pool that the collection skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum GcSkippedWork {
    /// The collection deleted no content objects, because it couldn't read every pool member's
    /// roots within its budget.
    ContentSweepPoolBudget,
    /// The collection deleted no content objects, because it couldn't read one pool member's roots.
    ContentSweepUnreadableMember,
    /// A reason from a newer server.
    #[serde(other)]
    Unknown,
}

impl GcResponse {
    /// Creates an empty report for `namespace_id`, before any candidate is
    /// examined.
    pub fn empty(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            deleted: DeletedObjectCounts::default(),
            deleted_snapshots_by_owner: DeletedSnapshotsByOwner::default(),
            retained: RetainedCandidates::default(),
            listed_bytes: 0,
            next_reclamation_at_ms: None,
            reclaimable_at_ms: None,
        }
    }

    /// Records one retained candidate under the reason that spared it.
    pub fn retain(&mut self, reason: RetainedReason) {
        *reason.counter(&mut self.retained) += 1;
    }
}

impl PoolGcResponse {
    /// Creates an empty report for `pool_id`, before any candidate is
    /// examined.
    pub fn empty(pool_id: PoolId) -> Self {
        Self {
            pool_id,
            deleted: PoolDeletedObjectCounts::default(),
            retained: PoolRetainedCandidates::default(),
            listed_bytes: 0,
            next_reclamation_at_ms: None,
            skipped: Vec::new(),
        }
    }

    /// Records one retained candidate under the reason that spared it.
    pub fn retain(&mut self, reason: PoolRetainedReason) {
        *reason.counter(&mut self.retained) += 1;
    }
}

/// The reason that a namespace's collection retained one candidate, and the corresponding
/// [`RetainedCandidates`] field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedReason {
    /// Counts into [`RetainedCandidates::referenced`].
    Referenced,
    /// Counts into [`RetainedCandidates::within_grace_window`].
    WithinGraceWindow,
    /// Counts into [`RetainedCandidates::no_provider_timestamp`].
    NoProviderTimestamp,
    /// Counts into [`RetainedCandidates::unrecognized_key`].
    UnrecognizedKey,
    /// Counts into [`RetainedCandidates::snapshot_not_deletable`].
    SnapshotNotDeletable,
}

impl RetainedReason {
    fn counter(self, retained: &mut RetainedCandidates) -> &mut u64 {
        match self {
            Self::Referenced => &mut retained.referenced,
            Self::WithinGraceWindow => &mut retained.within_grace_window,
            Self::NoProviderTimestamp => &mut retained.no_provider_timestamp,
            Self::UnrecognizedKey => &mut retained.unrecognized_key,
            Self::SnapshotNotDeletable => &mut retained.snapshot_not_deletable,
        }
    }
}

/// The reason that a pool's collection retained one candidate, and the corresponding
/// [`PoolRetainedCandidates`] field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolRetainedReason {
    /// Counts into [`PoolRetainedCandidates::referenced`].
    Referenced,
    /// Counts into [`PoolRetainedCandidates::within_grace_window`].
    WithinGraceWindow,
    /// Counts into [`PoolRetainedCandidates::no_provider_timestamp`].
    NoProviderTimestamp,
    /// Counts into [`PoolRetainedCandidates::unrecognized_key`].
    UnrecognizedKey,
    /// Counts into [`PoolRetainedCandidates::upload_session_window`].
    UploadSessionWindow,
    /// Counts into [`PoolRetainedCandidates::upload_session_undecided`].
    UploadSessionUndecided,
}

impl PoolRetainedReason {
    fn counter(self, retained: &mut PoolRetainedCandidates) -> &mut u64 {
        match self {
            Self::Referenced => &mut retained.referenced,
            Self::WithinGraceWindow => &mut retained.within_grace_window,
            Self::NoProviderTimestamp => &mut retained.no_provider_timestamp,
            Self::UnrecognizedKey => &mut retained.unrecognized_key,
            Self::UploadSessionWindow => &mut retained.upload_session_window,
            Self::UploadSessionUndecided => &mut retained.upload_session_undecided,
        }
    }
}

impl RetainedCandidates {
    /// Counts all candidates retained by the collection.
    pub fn total(&self) -> u64 {
        self.by_reason().into_iter().map(|(_, count)| count).sum()
    }

    /// Returns every reason and count in a fixed order.
    pub(crate) fn by_reason(&self) -> [(&'static str, u64); 5] {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            snapshot_not_deletable,
        } = *self;
        [
            ("referenced", referenced),
            ("within_grace_window", within_grace_window),
            ("no_provider_timestamp", no_provider_timestamp),
            ("unrecognized_key", unrecognized_key),
            ("snapshot_not_deletable", snapshot_not_deletable),
        ]
    }

    /// Adds counts from another collection.
    pub fn add(&mut self, other: &Self) {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            snapshot_not_deletable,
        } = other;
        self.referenced += referenced;
        self.within_grace_window += within_grace_window;
        self.no_provider_timestamp += no_provider_timestamp;
        self.unrecognized_key += unrecognized_key;
        self.snapshot_not_deletable += snapshot_not_deletable;
    }

    /// Returns the reason with the highest count, and that count.
    ///
    /// `None` when nothing was retained. Ties go to the first reason in the
    /// fixed table order, so one collection's report is stable.
    pub fn top_reason(&self) -> Option<(&'static str, u64)> {
        top_reason(self.by_reason())
    }
}

impl PoolRetainedCandidates {
    /// Counts all candidates retained by the collection.
    pub fn total(&self) -> u64 {
        self.by_reason().into_iter().map(|(_, count)| count).sum()
    }

    /// Returns every reason and count in a fixed order.
    pub(crate) fn by_reason(&self) -> [(&'static str, u64); 6] {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            upload_session_window,
            upload_session_undecided,
        } = *self;
        [
            ("referenced", referenced),
            ("within_grace_window", within_grace_window),
            ("no_provider_timestamp", no_provider_timestamp),
            ("unrecognized_key", unrecognized_key),
            ("upload_session_window", upload_session_window),
            ("upload_session_undecided", upload_session_undecided),
        ]
    }

    /// Adds counts from another collection.
    pub fn add(&mut self, other: &Self) {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            upload_session_window,
            upload_session_undecided,
        } = other;
        self.referenced += referenced;
        self.within_grace_window += within_grace_window;
        self.no_provider_timestamp += no_provider_timestamp;
        self.unrecognized_key += unrecognized_key;
        self.upload_session_window += upload_session_window;
        self.upload_session_undecided += upload_session_undecided;
    }

    /// Returns the reason with the highest count, and that count.
    ///
    /// `None` when nothing was retained. Ties go to the first reason in the
    /// fixed table order, so one collection's report is stable.
    pub fn top_reason(&self) -> Option<(&'static str, u64)> {
        top_reason(self.by_reason())
    }
}

/// Returns the first reason with the highest nonzero count in `by_reason`.
fn top_reason<const N: usize>(by_reason: [(&'static str, u64); N]) -> Option<(&'static str, u64)> {
    by_reason
        .into_iter()
        .filter(|(_, count)| *count > 0)
        // `max_by_key` keeps the last of equal maxima, so the reversal
        // is what makes a tie report the earlier reason.
        .rev()
        .max_by_key(|(_, count)| *count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_gc_response_fields_are_omitted_and_default_when_absent() {
        let gc = GcResponse::empty(NamespaceId::parse("demo").expect("namespace id"));
        let gc_json = serde_json::to_value(gc).expect("serialize gc response");
        assert!(gc_json.get("next_reclamation_at_ms").is_none());
        assert!(gc_json.get("reclaimable_at_ms").is_none());
        assert!(gc_json.get("skipped").is_none());
        assert_eq!(
            gc_json["deleted_snapshots_by_owner"],
            serde_json::json!({"user": 0, "fork": 0, "operation": 0})
        );
        assert_eq!(gc_json["listed_bytes"], 0);
        let gc: GcResponse =
            serde_json::from_value(gc_json).expect("decode gc response without optional fields");
        assert_eq!(gc.next_reclamation_at_ms, None);
        assert_eq!(gc.reclaimable_at_ms, None);
        let retired = GcResponse {
            listed_bytes: 4_096,
            reclaimable_at_ms: Some(2_000_000),
            ..gc
        };
        let json = serde_json::to_value(&retired).expect("encode retirement");
        assert_eq!(json["listed_bytes"], 4_096);
        assert_eq!(json["reclaimable_at_ms"], 2_000_000);
        assert_eq!(json["deleted"]["extension_segments"], 0);
        assert_eq!(
            serde_json::from_value::<GcResponse>(json).expect("decode retirement"),
            retired
        );
    }

    #[test]
    fn optional_pool_gc_response_fields_are_omitted_and_default_when_absent() {
        let pool_id = PoolId::parse("pool_3s0drmj8em84yb4mnetqkz2j0x").expect("pool id");
        let gc = PoolGcResponse::empty(pool_id);
        let gc_json = serde_json::to_value(&gc).expect("serialize pool gc response");
        assert!(gc_json.get("next_reclamation_at_ms").is_none());
        assert!(gc_json.get("skipped").is_none());
        assert_eq!(
            gc_json["deleted"],
            serde_json::json!({"content_objects": 0, "upload_sessions": 0, "temporary_objects": 0})
        );
        let skipped = PoolGcResponse {
            listed_bytes: 4_096,
            skipped: vec![GcSkippedWork::ContentSweepPoolBudget],
            ..gc
        };
        let json = serde_json::to_value(&skipped).expect("encode skipped work");
        assert_eq!(json["listed_bytes"], 4_096);
        assert_eq!(
            json["skipped"],
            serde_json::json!(["content_sweep_pool_budget"])
        );
        assert_eq!(
            serde_json::from_value::<PoolGcResponse>(json).expect("decode skipped work"),
            skipped
        );
        let newer: Vec<GcSkippedWork> = serde_json::from_value(serde_json::json!([
            "content_sweep_unreadable_member",
            "a_newer_limit"
        ]))
        .expect("decode a reason from a newer server");
        assert_eq!(
            newer,
            [
                GcSkippedWork::ContentSweepUnreadableMember,
                GcSkippedWork::Unknown
            ]
        );
    }
}
