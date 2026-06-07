use loon_api::NamespaceId;
use loon_objectstore::layout::{parse_object_key, DurableObjectFamily, ObjectLayout};
use loon_objectstore::{ObjectStore, ObjectStoreError};
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

pub fn summarize_namespace_objects<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<SimNamespaceObjectSummary, ObjectStoreError> {
    let layout = ObjectLayout::new();
    let prefix = layout.namespace_root_prefix(namespace_id.as_str());
    let keys = store.list_prefix(&prefix)?;
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
            DurableObjectFamily::NamespaceHead
            | DurableObjectFamily::NamespaceLease
            | DurableObjectFamily::NamespaceForkState => {
                summary.control_objects += 1;
            }
            DurableObjectFamily::WalSegment => {
                summary.wal_objects += 1;
            }
            DurableObjectFamily::NamespaceManifest => {
                summary.manifest_objects += 1;
            }
            DurableObjectFamily::CompactedMetadataSst => {
                summary.compacted_metadata_objects += 1;
            }
            DurableObjectFamily::GcPin => {
                summary.gc_pin_objects += 1;
            }
            DurableObjectFamily::NamespaceDescriptor
            | DurableObjectFamily::UploadSession
            | DurableObjectFamily::ConflictArtifact
            | DurableObjectFamily::NamespaceCompactions
            | DurableObjectFamily::CompactedIndexSst
            | DurableObjectFamily::IndexManifest
            | DurableObjectFamily::IndexGcBoundary
            | DurableObjectFamily::NamespaceGcBoundary
            | DurableObjectFamily::DerivedProgress
            | DurableObjectFamily::ContentStoreDescriptor
            | DurableObjectFamily::ContentBlob
            | DurableObjectFamily::QueueShard => {}
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_objectstore::fs::LocalFsStore;

    #[test]
    fn namespace_object_summary_counts_known_key_families() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = LocalFsStore::new(temp_dir.path()).expect("local store");
        let namespace_id = NamespaceId::parse("sim").expect("valid namespace id");
        let layout = ObjectLayout::new();

        store
            .put_overwrite(
                layout.namespace_head(namespace_id.as_str()).as_str(),
                b"head",
            )
            .expect("head");
        store
            .put_overwrite(
                layout
                    .wal_segment(
                        namespace_id.as_str(),
                        "seg_00000000000000000000000000000001",
                    )
                    .as_str(),
                b"wal",
            )
            .expect("wal");
        store
            .put_overwrite(
                layout.gc_pin(namespace_id.as_str(), "pin_abc").as_str(),
                b"pin",
            )
            .expect("pin");

        let summary = summarize_namespace_objects(&store, &namespace_id).expect("summary");
        assert_eq!(summary.namespace_objects, 3);
        assert_eq!(summary.control_objects, 1);
        assert_eq!(summary.wal_objects, 1);
        assert_eq!(summary.gc_pin_objects, 1);
    }
}
