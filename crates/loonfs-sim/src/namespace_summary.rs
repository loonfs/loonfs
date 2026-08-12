//! Namespace object inventory for deterministic oracle assertions.

use loonfs_api::NamespaceId;
use loonfs_grep::keyspace::{parse_key as parse_grep_key, GrepKeyKind};
use loonfs_objectstore::keys::namespace_prefix;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimNamespaceObjectSummary {
    pub namespace_id: NamespaceId,
    pub namespace_objects: usize,
    pub control_objects: usize,
    pub wal_objects: usize,
    pub manifest_objects: usize,
    pub compacted_metadata_objects: usize,
    pub grep_root_objects: usize,
    pub grep_manifest_objects: usize,
    pub grep_segment_objects: usize,
}

pub async fn summarize_namespace_objects<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<SimNamespaceObjectSummary, ObjectStoreError> {
    let prefix = namespace_prefix(namespace_id);
    let keys = store.list_prefix(&prefix).await?;
    let mut summary = SimNamespaceObjectSummary {
        namespace_id: namespace_id.clone(),
        namespace_objects: 0,
        control_objects: 0,
        wal_objects: 0,
        manifest_objects: 0,
        compacted_metadata_objects: 0,
        grep_root_objects: 0,
        grep_manifest_objects: 0,
        grep_segment_objects: 0,
    };

    for key in keys {
        if let Some(parsed) = parse_object_key(&key) {
            if parsed.owner_namespace_id() != Some(namespace_id.as_str()) {
                continue;
            }
            summary.namespace_objects += 1;
            match parsed.family() {
                DurableObjectFamily::WalHead => {
                    summary.control_objects += 1;
                }
                DurableObjectFamily::WalSegment => {
                    summary.wal_objects += 1;
                }
                DurableObjectFamily::MetadataManifest => {
                    summary.manifest_objects += 1;
                }
                DurableObjectFamily::MetadataTable
                | DurableObjectFamily::MetadataCompactionStaging => {
                    summary.compacted_metadata_objects += 1;
                }
                DurableObjectFamily::WalFloor
                | DurableObjectFamily::MetadataRoot
                | DurableObjectFamily::CheckpointRecord
                | DurableObjectFamily::MetadataCompactionLease
                | DurableObjectFamily::UploadSession
                | DurableObjectFamily::ContentBlob => {}
            }
            continue;
        }

        let Some(parsed) = parse_grep_key(&key) else {
            continue;
        };
        if parsed.namespace_id != *namespace_id {
            continue;
        }
        summary.namespace_objects += 1;
        match parsed.kind {
            GrepKeyKind::Root => summary.grep_root_objects += 1,
            GrepKeyKind::Manifest { .. } => summary.grep_manifest_objects += 1,
            GrepKeyKind::Segment { .. } => summary.grep_segment_objects += 1,
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use loonfs_api::IndexSegmentId;
    use loonfs_grep::keyspace::{manifest_key, root_key, segment_key};
    use loonfs_grep::root::GrepManifestId;
    use loonfs_objectstore::keys::{wal_head, wal_segment};
    use loonfs_objectstore::local_fs_store::LocalFsStore;

    #[tokio::test]
    async fn namespace_object_summary_counts_core_and_grep_key_families() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = LocalFsStore::new(temp_dir.path()).expect("local store");
        let namespace_id = NamespaceId::parse("sim").expect("valid namespace id");

        store
            .put_overwrite(&wal_head(&namespace_id), Bytes::from_static(b"head"))
            .await
            .expect("head");
        store
            .put_overwrite(
                &wal_segment(
                    &namespace_id,
                    &loonfs_api::WalSegmentId::parse("00000000000000000001-644e4d336fd4ee33")
                        .expect("valid WAL segment id"),
                ),
                Bytes::from_static(b"wal"),
            )
            .await
            .expect("wal");
        store
            .put_overwrite(&root_key(&namespace_id), Bytes::from_static(b"grep root"))
            .await
            .expect("grep root");
        let grep_manifest_id = GrepManifestId::parse("gmf_0123456789abcdef0123456789abcdef")
            .expect("valid grep manifest id");
        store
            .put_overwrite(
                &manifest_key(&namespace_id, &grep_manifest_id),
                Bytes::from_static(b"grep manifest"),
            )
            .await
            .expect("grep manifest");
        let grep_segment_id = IndexSegmentId::parse("idx_00000000000000000000000000000001")
            .expect("valid grep segment id");
        store
            .put_overwrite(
                &segment_key(&namespace_id, &grep_segment_id),
                Bytes::from_static(b"grep segment"),
            )
            .await
            .expect("grep segment");

        let summary = summarize_namespace_objects(&store, &namespace_id)
            .await
            .expect("summary");
        assert_eq!(summary.namespace_objects, 5);
        assert_eq!(summary.control_objects, 1);
        assert_eq!(summary.wal_objects, 1);
        assert_eq!(summary.grep_root_objects, 1);
        assert_eq!(summary.grep_manifest_objects, 1);
        assert_eq!(summary.grep_segment_objects, 1);
    }
}
