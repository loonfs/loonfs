//! Garbage collection requests and reports for the v0 HTTP API.

use crate::NamespaceId;
use serde::{Deserialize, Serialize};

/// Optional overrides for one garbage collection pass.
///
/// The server rejects unknown fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    /// The minimum age, in milliseconds, that an object must reach before the pass can delete it.
    /// The value must be at least the server's advertised safety floor. The grep index's collector
    /// keeps its own age rules, so this value doesn't change them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub grace_window_ms: Option<u64>,
}

/// The candidates that one garbage collection pass inspected but didn't delete, counted by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RetainedCandidates {
    /// The candidates that a current root or the shared base filter protected, or that manifest
    /// discovery needed.
    pub referenced: u64,
    /// The unreferenced candidates that were younger than the grace window, according to their
    /// provider timestamps.
    pub within_grace_window: u64,
    /// The unreferenced candidates that had no provider timestamp, so their age was unknown.
    pub no_provider_timestamp: u64,
    /// The keys that garbage collection didn't recognize in the object families it scanned. The
    /// pass keeps these objects.
    pub unrecognized_key: u64,
    /// The checkpoint records that the pass couldn't delete because of their owner or their grace
    /// window.
    pub checkpoint_not_deletable: u64,
    /// The upload sessions that a lease or grace window still protected.
    pub upload_session_window: u64,
    /// The upload sessions that the pass couldn't prove safe to delete.
    pub upload_session_undecided: u64,
}

/// The number of objects that one garbage collection pass deleted, grouped by object family.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedObjectCounts {
    /// The unreferenced WAL objects that the pass deleted.
    pub wal_objects: u64,
    /// The unreferenced metadata segments that the pass deleted.
    pub metadata_segments: u64,
    /// The unreferenced extension segments, such as the grep index's segments, that the pass
    /// deleted.
    pub extension_segments: u64,
    /// The unreferenced manifests that the pass deleted.
    pub manifests: u64,
    /// The upload session records that the pass deleted after their grace windows passed.
    pub upload_sessions: u64,
    /// The content objects the pass deleted because no retained view referenced them and they were
    /// older than the grace window.
    pub content_objects: u64,
    /// The content objects that the pass listed under a retired namespace and deleted.
    pub retired_content_objects: u64,
    /// The store's temporary objects that the pass deleted because they were older than the grace
    /// window.
    pub temporary_objects: u64,
}

impl DeletedObjectCounts {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            wal_objects,
            metadata_segments,
            extension_segments,
            manifests,
            upload_sessions,
            content_objects,
            retired_content_objects,
            temporary_objects,
        } = other;
        self.wal_objects += wal_objects;
        self.metadata_segments += metadata_segments;
        self.extension_segments += extension_segments;
        self.manifests += manifests;
        self.upload_sessions += upload_sessions;
        self.content_objects += content_objects;
        self.retired_content_objects += retired_content_objects;
        self.temporary_objects += temporary_objects;
    }
}

/// The number of checkpoint records that one garbage collection pass deleted, grouped by owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedCheckpointsByOwner {
    /// The user pins that the pass deleted after they expired or their namespace was deleted.
    pub user: u64,
    /// The fork pins that the pass deleted because their target namespaces no longer exist.
    pub fork: u64,
}

impl DeletedCheckpointsByOwner {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self { user, fork } = other;
        self.user += user;
        self.fork += fork;
    }
}

/// The result of one stateless garbage collection call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// The namespace that the pass ran against.
    pub namespace_id: NamespaceId,
    /// The objects that the pass deleted, counted by object family.
    pub deleted: DeletedObjectCounts,
    /// The checkpoint records that the pass deleted, counted by owner.
    pub deleted_checkpoints_by_owner: DeletedCheckpointsByOwner,
    /// The candidates that the pass kept at deletion time, counted by reason.
    pub retained: RetainedCandidates,
    /// The bytes of every object that the pass listed under the namespace, each object once, before
    /// the pass's deletes.
    pub listed_bytes: u64,
    /// The earliest future retirement deadline, pin deletion time, or upload cleanup time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_reclamation_at_ms: Option<u64>,
    /// The deletion time of the current tombstone plus the configured retirement grace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub reclaimable_at_ms: Option<u64>,
}

impl GcResponse {
    /// Creates an empty report for `namespace_id`, before any candidate is
    /// examined.
    pub fn empty(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            deleted: DeletedObjectCounts::default(),
            deleted_checkpoints_by_owner: DeletedCheckpointsByOwner::default(),
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

/// The reason one candidate was retained and the corresponding [`RetainedCandidates`] field.
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
    /// Counts into [`RetainedCandidates::checkpoint_not_deletable`].
    CheckpointNotDeletable,
    /// Counts into [`RetainedCandidates::upload_session_window`].
    UploadSessionWindow,
    /// Counts into [`RetainedCandidates::upload_session_undecided`].
    UploadSessionUndecided,
}

impl RetainedReason {
    fn counter(self, retained: &mut RetainedCandidates) -> &mut u64 {
        match self {
            Self::Referenced => &mut retained.referenced,
            Self::WithinGraceWindow => &mut retained.within_grace_window,
            Self::NoProviderTimestamp => &mut retained.no_provider_timestamp,
            Self::UnrecognizedKey => &mut retained.unrecognized_key,
            Self::CheckpointNotDeletable => &mut retained.checkpoint_not_deletable,
            Self::UploadSessionWindow => &mut retained.upload_session_window,
            Self::UploadSessionUndecided => &mut retained.upload_session_undecided,
        }
    }
}

impl RetainedCandidates {
    /// Counts all candidates retained by the pass.
    pub fn total(&self) -> u64 {
        self.by_reason().into_iter().map(|(_, count)| count).sum()
    }

    /// Returns every reason and count in a fixed order.
    pub(crate) fn by_reason(&self) -> [(&'static str, u64); 7] {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            checkpoint_not_deletable,
            upload_session_window,
            upload_session_undecided,
        } = *self;
        [
            ("referenced", referenced),
            ("within_grace_window", within_grace_window),
            ("no_provider_timestamp", no_provider_timestamp),
            ("unrecognized_key", unrecognized_key),
            ("checkpoint_not_deletable", checkpoint_not_deletable),
            ("upload_session_window", upload_session_window),
            ("upload_session_undecided", upload_session_undecided),
        ]
    }

    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            checkpoint_not_deletable,
            upload_session_window,
            upload_session_undecided,
        } = other;
        self.referenced += referenced;
        self.within_grace_window += within_grace_window;
        self.no_provider_timestamp += no_provider_timestamp;
        self.unrecognized_key += unrecognized_key;
        self.checkpoint_not_deletable += checkpoint_not_deletable;
        self.upload_session_window += upload_session_window;
        self.upload_session_undecided += upload_session_undecided;
    }

    /// Returns the reason with the highest count, and that count.
    ///
    /// `None` when nothing was retained. Ties go to the first reason in the
    /// fixed table order, so one pass's report is stable.
    pub fn top_reason(&self) -> Option<(&'static str, u64)> {
        self.by_reason()
            .into_iter()
            .filter(|(_, count)| *count > 0)
            // `max_by_key` keeps the last of equal maxima, so the reversal
            // is what makes a tie report the earlier reason.
            .rev()
            .max_by_key(|(_, count)| *count)
    }
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
        assert_eq!(
            gc_json["deleted_checkpoints_by_owner"],
            serde_json::json!({"user": 0, "fork": 0})
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
}
