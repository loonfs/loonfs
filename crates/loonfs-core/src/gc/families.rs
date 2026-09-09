//! Object families swept by namespace collection.

use super::live_set::LiveSet;
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{
    checkpoint_prefix, content_owner_prefix, metadata_manifest_prefix, metadata_segment_prefix,
    upload_session_prefix, wal_segment_prefix,
};
use loonfs_objectstore::layout::{manifest_no_of, parse_object_key, DurableObjectFamily};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CandidateFamily {
    Manifests,
    WalSegments,
    MetadataSegments,
    Checkpoints,
    UploadSessions,
    OwnedContent,
}

impl CandidateFamily {
    pub(super) const ALL: [Self; 6] = [
        Self::Manifests,
        Self::WalSegments,
        Self::MetadataSegments,
        Self::Checkpoints,
        Self::UploadSessions,
        Self::OwnedContent,
    ];

    pub(super) fn recognizes(self, key: &str) -> bool {
        let Some(family) = parse_object_key(key).map(|parsed| parsed.family()) else {
            return false;
        };
        match self {
            Self::Manifests => manifest_no_of(key).is_some(),
            Self::WalSegments => loonfs_objectstore::layout::wal_no_of(key).is_some(),
            Self::MetadataSegments => family == DurableObjectFamily::MetadataSegment,
            Self::Checkpoints => family == DurableObjectFamily::CheckpointRecord,
            Self::UploadSessions => family == DurableObjectFamily::UploadSession,
            Self::OwnedContent => family == DurableObjectFamily::ContentBlob,
        }
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId, live: &LiveSet) -> String {
        match self {
            Self::Manifests => metadata_manifest_prefix(namespace_id),
            Self::WalSegments => wal_segment_prefix(namespace_id),
            Self::MetadataSegments => metadata_segment_prefix(namespace_id),
            Self::Checkpoints => checkpoint_prefix(namespace_id),
            Self::UploadSessions => upload_session_prefix(namespace_id),
            Self::OwnedContent => content_owner_prefix(&live.content_store_id, namespace_id),
        }
    }
}
