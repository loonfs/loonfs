//! Object families swept one key at a time by namespace collection.
//!
//! Upload sessions aren't one of these families: each session has two
//! records, so the pass sweeps them from their own listing (see
//! `super::uploads`).

use super::deletes::DeletedCounter;
use crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS;
use loonfs_objectstore::keys::{
    content_prefix, metadata_manifest_prefix, metadata_segment_prefix, pin_prefix,
    temporary_prefix, wal_prefix,
};
use loonfs_objectstore::layout::{
    content_id_of, manifest_no_of, parse_object_key, DurableObjectFamily,
};
use loonfs_types::NamespaceId;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CandidateFamily {
    Manifests,
    WalObjects,
    MetadataSegments,
    Pins,
    Content,
    Temporary,
}

impl CandidateFamily {
    pub(super) const ALL: [Self; 6] = [
        Self::Manifests,
        Self::WalObjects,
        Self::MetadataSegments,
        Self::Pins,
        Self::Content,
        Self::Temporary,
    ];

    pub(super) fn recognizes(self, key: &str) -> bool {
        let Some(family) = parse_object_key(key).map(|parsed| parsed.family()) else {
            return false;
        };
        match self {
            Self::Manifests => manifest_no_of(key).is_some(),
            Self::WalObjects => loonfs_objectstore::layout::wal_no_of(key).is_some(),
            Self::MetadataSegments => family == DurableObjectFamily::MetadataSegment,
            Self::Pins => family == DurableObjectFamily::Pin,
            Self::Content => content_id_of(key).is_some(),
            Self::Temporary => family == DurableObjectFamily::TemporaryObject,
        }
    }

    /// Returns how old an unreferenced object of this family has to be before
    /// a pass deletes the object.
    pub(super) fn min_age_ms(self, grace_window_ms: u64) -> u64 {
        match self {
            // A compaction may publish output that is exactly the minimum age
            // old, so a segment must be strictly older before it goes.
            Self::MetadataSegments => UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
            Self::Manifests | Self::WalObjects | Self::Pins | Self::Content | Self::Temporary => {
                grace_window_ms
            }
        }
    }

    /// Returns the report counter of the family's deleted objects, or `None`
    /// for pins, which a pass counts by owner.
    pub(super) fn deleted_counter(self) -> Option<DeletedCounter> {
        Some(match self {
            Self::Manifests => |report| &mut report.deleted.manifests,
            Self::WalObjects => |report| &mut report.deleted.wal_objects,
            Self::MetadataSegments => |report| &mut report.deleted.metadata_segments,
            Self::Content => |report| &mut report.deleted.content_objects,
            Self::Temporary => |report| &mut report.deleted.temporary_objects,
            Self::Pins => return None,
        })
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId) -> String {
        match self {
            Self::Manifests => metadata_manifest_prefix(namespace_id),
            Self::WalObjects => wal_prefix(namespace_id),
            Self::MetadataSegments => metadata_segment_prefix(namespace_id),
            Self::Pins => pin_prefix(namespace_id),
            Self::Content => content_prefix(namespace_id),
            Self::Temporary => temporary_prefix(namespace_id),
        }
    }
}
