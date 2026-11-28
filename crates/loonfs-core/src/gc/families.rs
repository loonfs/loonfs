//! The object families under a namespace's prefix that the namespace's
//! collection sweeps one key at a time.

use super::deletes::Counter;
use loonfs_objectstore::keys::{
    extensions_prefix, metadata_manifest_prefix, metadata_segment_prefix, wal_prefix,
};
use loonfs_objectstore::layout::{manifest_no_of, parse_object_key, DurableObjectFamily};
use loonfs_objectstore::ListedObject;
use loonfs_types::format::wal::MAX_FENCE_BYTES;
use loonfs_types::{GcResponse, NamespaceId};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CandidateFamily {
    Manifests,
    WalObjects,
    MetadataSegments,
    /// The segments of every extension, which the extensions prefix lists.
    ExtensionSegments,
}

impl CandidateFamily {
    pub(super) const ALL: [Self; 4] = [
        Self::Manifests,
        Self::WalObjects,
        Self::MetadataSegments,
        Self::ExtensionSegments,
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
        }
    }

    /// Reports whether a listed object of this family is a fence, which no
    /// collection deletes. A WAL object stored in [`MAX_FENCE_BYTES`] or
    /// fewer is a fence, because the encoder pads every commit object above that
    /// size.
    pub(super) fn is_fence(self, entry: &ListedObject) -> bool {
        self == Self::WalObjects && entry.size_bytes <= MAX_FENCE_BYTES as u64
    }

    /// Returns the report counter of the family's deleted objects.
    pub(super) fn deleted_counter(self) -> Counter<GcResponse> {
        match self {
            Self::Manifests => |report| &mut report.deleted.manifests,
            Self::WalObjects => |report| &mut report.deleted.wal_objects,
            Self::MetadataSegments => |report| &mut report.deleted.metadata_segments,
            Self::ExtensionSegments => |report| &mut report.deleted.extension_segments,
        }
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId) -> String {
        match self {
            Self::Manifests => metadata_manifest_prefix(namespace_id),
            Self::WalObjects => wal_prefix(namespace_id),
            Self::MetadataSegments => metadata_segment_prefix(namespace_id),
            Self::ExtensionSegments => extensions_prefix(namespace_id),
        }
    }
}
