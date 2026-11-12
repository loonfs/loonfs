//! The object families under a namespace's prefix that the namespace's
//! collection sweeps one key at a time.

use super::deletes::Counter;
use crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS;
use loonfs_objectstore::keys::{
    extensions_prefix, metadata_manifest_prefix, metadata_segment_prefix, snapshot_prefix,
    wal_prefix,
};
use loonfs_objectstore::layout::{manifest_no_of, parse_object_key, DurableObjectFamily};
use loonfs_types::{GcResponse, NamespaceId};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CandidateFamily {
    Manifests,
    WalObjects,
    MetadataSegments,
    /// The segments of every extension, which the extensions prefix lists.
    ExtensionSegments,
    Snapshots,
}

impl CandidateFamily {
    pub(super) const ALL: [Self; 5] = [
        Self::Manifests,
        Self::WalObjects,
        Self::MetadataSegments,
        Self::ExtensionSegments,
        Self::Snapshots,
    ];

    pub(super) fn recognizes(self, key: &str) -> bool {
        let Some(family) = parse_object_key(key).map(|parsed| parsed.family()) else {
            return false;
        };
        match self {
            Self::Manifests => manifest_no_of(key).is_some(),
            Self::WalObjects => loonfs_objectstore::layout::wal_no_of(key).is_some(),
            Self::MetadataSegments => family == DurableObjectFamily::MetadataSegment,
            Self::ExtensionSegments => family == DurableObjectFamily::ExtensionSegment,
            Self::Snapshots => family == DurableObjectFamily::Snapshot,
        }
    }

    /// Returns how old an unreferenced object of this family has to be before
    /// a collection deletes the object.
    pub(super) fn min_age_ms(self, grace_window_ms: u64) -> u64 {
        match self {
            // A compaction may publish output that is exactly the minimum age
            // old, so a segment must be strictly older before it goes.
            Self::MetadataSegments | Self::ExtensionSegments => {
                UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1
            }
            Self::Manifests | Self::WalObjects | Self::Snapshots => grace_window_ms,
        }
    }

    /// Returns the report counter of the family's deleted objects, or `None`
    /// for snapshots, which a collection counts by owner.
    pub(super) fn deleted_counter(self) -> Option<Counter<GcResponse>> {
        Some(match self {
            Self::Manifests => |report| &mut report.deleted.manifests,
            Self::WalObjects => |report| &mut report.deleted.wal_objects,
            Self::MetadataSegments => |report| &mut report.deleted.metadata_segments,
            Self::ExtensionSegments => |report| &mut report.deleted.extension_segments,
            Self::Snapshots => return None,
        })
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId) -> String {
        match self {
            Self::Manifests => metadata_manifest_prefix(namespace_id),
            Self::WalObjects => wal_prefix(namespace_id),
            Self::MetadataSegments => metadata_segment_prefix(namespace_id),
            Self::ExtensionSegments => extensions_prefix(namespace_id),
            Self::Snapshots => snapshot_prefix(namespace_id),
        }
    }
}
