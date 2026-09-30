//! Object families swept by namespace collection.

use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{
    metadata_manifest_prefix, metadata_segment_prefix, pin_prefix, upload_session_prefix,
    wal_segment_prefix,
};
use loonfs_objectstore::layout::{manifest_no_of, parse_object_key, DurableObjectFamily};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CandidateFamily {
    Manifests,
    WalSegments,
    MetadataSegments,
    Pins,
    UploadSessions,
}

impl CandidateFamily {
    pub(super) const ALL: [Self; 5] = [
        Self::Manifests,
        Self::WalSegments,
        Self::MetadataSegments,
        Self::Pins,
        Self::UploadSessions,
    ];

    pub(super) fn recognizes(self, key: &str) -> bool {
        let Some(family) = parse_object_key(key).map(|parsed| parsed.family()) else {
            return false;
        };
        match self {
            Self::Manifests => manifest_no_of(key).is_some(),
            Self::WalSegments => loonfs_objectstore::layout::wal_no_of(key).is_some(),
            Self::MetadataSegments => family == DurableObjectFamily::MetadataSegment,
            Self::Pins => family == DurableObjectFamily::Pin,
            Self::UploadSessions => family == DurableObjectFamily::UploadSession,
        }
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId) -> String {
        match self {
            Self::Manifests => metadata_manifest_prefix(namespace_id),
            Self::WalSegments => wal_segment_prefix(namespace_id),
            Self::MetadataSegments => metadata_segment_prefix(namespace_id),
            Self::Pins => pin_prefix(namespace_id),
            Self::UploadSessions => upload_session_prefix(namespace_id),
        }
    }
}
