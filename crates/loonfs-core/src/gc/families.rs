//! Object families swept by namespace collection.

use super::live_set::LiveSet;
use futures::{future, stream::BoxStream, StreamExt};
use loonfs_api::{ManifestNo, NamespaceId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, content_owner_prefix, metadata_manifest_prefix, metadata_segment_prefix,
    upload_session_prefix, wal_segment_prefix,
};
use loonfs_objectstore::layout::{manifest_no_of, parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use sha2::{Digest, Sha256};

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

    pub(super) fn listing<S: ObjectStore + ?Sized>(
        self,
        store: &S,
        prefix: &str,
        current_manifest_no: ManifestNo,
        now_ms: u64,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        let hash = Sha256::digest(now_ms.to_le_bytes());
        let random =
            u128::from_le_bytes(hash[..16].try_into().expect("hash should contain 16 bytes"));
        let start = match self {
            Self::Checkpoints => {
                let number = u64::from_le_bytes(
                    hash[..8]
                        .try_into()
                        .expect("hash should contain eight bytes"),
                );
                let suffix = u64::from_le_bytes(
                    hash[8..16]
                        .try_into()
                        .expect("hash should contain sixteen bytes"),
                );
                let manifest_no = ManifestNo(1 + number % current_manifest_no.0);
                format!("{prefix}pin_{:020}-{suffix:016x}.json", manifest_no.0)
            }
            Self::MetadataSegments => format!("{prefix}seg_{random:032x}.sst.zst"),
            Self::UploadSessions => format!("{prefix}upl_{random:032x}.json"),
            _ => return store.list_prefix_stream(prefix),
        };
        store
            .list_prefix_from_stream(prefix, Some(&start))
            .chain(store.list_prefix_stream(prefix).take_while(move |key| {
                future::ready(key.as_ref().map_or(true, |key| key < &start))
            }))
            .boxed()
    }
}
