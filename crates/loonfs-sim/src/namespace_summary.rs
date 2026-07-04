use loonfs_api::NamespaceId;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily, ObjectLayout};
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
    pub gc_pin_objects: usize,
    pub content_blob_count_hint: Option<usize>,
}

pub async fn summarize_namespace_objects<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<SimNamespaceObjectSummary, ObjectStoreError> {
    let layout = ObjectLayout::new();
    let prefix = layout.namespace_root_prefix(namespace_id.as_str());
    let keys = store.list_prefix(&prefix).await?;
    let mut summary = SimNamespaceObjectSummary {
        namespace_id: namespace_id.clone(),
        namespace_objects: 0,
        control_objects: 0,
        wal_objects: 0,
        manifest_objects: 0,
        compacted_metadata_objects: 0,
        gc_pin_objects: 0,
        content_blob_count_hint: None,
    };

    for key in keys {
        let Some(parsed) = parse_object_key(&key) else {
            continue;
        };
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
            DurableObjectFamily::MetadataTable => {
                summary.compacted_metadata_objects += 1;
            }
            DurableObjectFamily::Pin => {
                summary.gc_pin_objects += 1;
            }
            DurableObjectFamily::NamespaceConfig
            | DurableObjectFamily::WalFloor
            | DurableObjectFamily::WalIndex
            | DurableObjectFamily::WalIndexRun
            | DurableObjectFamily::MetadataRoot
            | DurableObjectFamily::CheckpointRecord
            | DurableObjectFamily::UploadSession
            | DurableObjectFamily::ContentStoreDescriptor
            | DurableObjectFamily::ContentBlob => {}
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use loonfs_objectstore::fs::LocalFsStore;

    #[tokio::test]
    async fn namespace_object_summary_counts_known_key_families() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = LocalFsStore::new(temp_dir.path()).expect("local store");
        let namespace_id = NamespaceId::parse("sim").expect("valid namespace id");
        let layout = ObjectLayout::new();

        store
            .put_overwrite(
                layout.wal_head(namespace_id.as_str()).as_str(),
                Bytes::from_static(b"head"),
            )
            .await
            .expect("head");
        store
            .put_overwrite(
                layout
                    .wal_segment(
                        namespace_id.as_str(),
                        "seg_00000000000000000000000000000001",
                    )
                    .as_str(),
                Bytes::from_static(b"wal"),
            )
            .await
            .expect("wal");
        store
            .put_overwrite(
                layout.pin(namespace_id.as_str(), "pin_abc").as_str(),
                Bytes::from_static(b"pin"),
            )
            .await
            .expect("pin");

        let summary = summarize_namespace_objects(&store, &namespace_id)
            .await
            .expect("summary");
        assert_eq!(summary.namespace_objects, 3);
        assert_eq!(summary.control_objects, 1);
        assert_eq!(summary.wal_objects, 1);
        assert_eq!(summary.gc_pin_objects, 1);
    }
}
