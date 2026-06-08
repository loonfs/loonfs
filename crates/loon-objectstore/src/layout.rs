use crate::keys::{CheckpointTableFamily, DerivedWorkClass};
use crate::ObjectStoreError;

#[derive(Debug, Clone, Copy, Default)]
pub struct ObjectLayout;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectKey(String);

macro_rules! typed_key {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(ObjectKey);

        impl $name {
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }

            pub fn into_string(self) -> String {
                self.0.into_string()
            }
        }

        impl From<$name> for String {
            fn from(key: $name) -> Self {
                key.into_string()
            }
        }
    };
}

typed_key!(NamespaceDescriptorKey);
typed_key!(NamespaceHeadKey);
typed_key!(NamespaceLeaseKey);
typed_key!(WalSegmentKey);
typed_key!(ContentStoreDescriptorKey);
typed_key!(ContentBlobKey);
typed_key!(ConflictArtifactKey);
typed_key!(UploadSessionKey);
typed_key!(CheckpointManifestKey);
typed_key!(CheckpointRunTableKey);
typed_key!(DerivedProgressKey);
typed_key!(QueueShardKey);

impl ObjectKey {
    fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    fn as_str(&self) -> &str {
        &self.0
    }

    fn into_string(self) -> String {
        self.0
    }
}

impl ObjectLayout {
    pub fn new() -> Self {
        Self
    }

    pub fn namespace_descriptor(&self, namespace: &str) -> NamespaceDescriptorKey {
        NamespaceDescriptorKey(ObjectKey::new(format!(
            "namespaces/{namespace}/descriptor.json"
        )))
    }

    pub fn namespace_head(&self, namespace: &str) -> NamespaceHeadKey {
        NamespaceHeadKey(ObjectKey::new(format!(
            "namespaces/{namespace}/control/head.json"
        )))
    }

    pub fn namespace_lease(&self, namespace: &str) -> NamespaceLeaseKey {
        NamespaceLeaseKey(ObjectKey::new(format!(
            "namespaces/{namespace}/control/lease.json"
        )))
    }

    pub fn wal_segment(
        &self,
        namespace: &str,
        start_seq: u64,
        end_seq: u64,
        segment_id: &str,
    ) -> WalSegmentKey {
        WalSegmentKey(ObjectKey::new(format!(
            "namespaces/{namespace}/wal/{start_seq:020}-{end_seq:020}-{segment_id}.cbor.zst"
        )))
    }

    pub fn content_store_descriptor(&self, content_store: &str) -> ContentStoreDescriptorKey {
        ContentStoreDescriptorKey(ObjectKey::new(format!(
            "content-stores/{content_store}/descriptor.json"
        )))
    }

    pub fn content_blob(
        &self,
        content_store: &str,
        digest: &str,
    ) -> Result<ContentBlobKey, ObjectStoreError> {
        let hex = sha256_hex_from_digest(digest)?;
        Ok(ContentBlobKey(ObjectKey::new(format!(
            "content-stores/{content_store}/blobs/sha256/{}/{}/{}",
            &hex[0..2],
            &hex[2..4],
            hex
        ))))
    }

    pub fn conflict_artifact(&self, namespace: &str, conflict_id: &str) -> ConflictArtifactKey {
        ConflictArtifactKey(ObjectKey::new(format!(
            "namespaces/{namespace}/conflicts/{conflict_id}.json"
        )))
    }

    pub fn conflict_artifact_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/conflicts/")
    }

    pub fn upload_session(&self, namespace: &str, upload_id: &str) -> UploadSessionKey {
        UploadSessionKey(ObjectKey::new(format!(
            "namespaces/{namespace}/uploads/{upload_id}.json"
        )))
    }

    pub fn upload_session_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/uploads/")
    }

    pub fn checkpoint_manifest(&self, namespace: &str, seq: u64) -> CheckpointManifestKey {
        CheckpointManifestKey(ObjectKey::new(format!(
            "namespaces/{namespace}/compacted/checkpoints/{seq:020}/manifest.json"
        )))
    }

    pub fn checkpoint_run_table(
        &self,
        namespace: &str,
        run_seq: u64,
        run_id: &str,
        family: CheckpointTableFamily,
        segment_index: u32,
    ) -> CheckpointRunTableKey {
        CheckpointRunTableKey(ObjectKey::new(format!(
            "namespaces/{namespace}/compacted/checkpoints/{run_seq:020}/runs/{run_id}/tables/{}/{segment_index:05}.sst.zst",
            family.as_str()
        )))
    }

    pub fn derived_progress(
        &self,
        namespace: &str,
        work_class: DerivedWorkClass,
    ) -> DerivedProgressKey {
        let work_class = work_class.as_str();
        DerivedProgressKey(ObjectKey::new(format!(
            "namespaces/{namespace}/derived/{work_class}/progress.json"
        )))
    }

    pub fn queue_shard(&self, shard_index: u32) -> QueueShardKey {
        QueueShardKey(ObjectKey::new(format!(
            "queue/shards/{shard_index:05}.json"
        )))
    }
}

pub fn sha256_hex_from_digest(digest: &str) -> Result<&str, ObjectStoreError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(ObjectStoreError::InvalidKey(digest.to_owned()));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ObjectStoreError::InvalidKey(digest.to_owned()));
    }
    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(ObjectStoreError::InvalidKey(digest.to_owned()));
    }
    Ok(hex)
}

#[cfg(test)]
mod tests {
    use super::{sha256_hex_from_digest, ObjectLayout};
    use crate::keys::{CheckpointTableFamily, DerivedWorkClass};

    #[test]
    fn layout_golden_tree_matches_expected_paths() {
        let layout = ObjectLayout::new();

        assert_eq!(
            layout.namespace_descriptor("ns-1").as_str(),
            "namespaces/ns-1/descriptor.json"
        );
        assert_eq!(
            layout.namespace_head("ns-1").as_str(),
            "namespaces/ns-1/control/head.json"
        );
        assert_eq!(
            layout.namespace_lease("ns-1").as_str(),
            "namespaces/ns-1/control/lease.json"
        );
        assert_eq!(
            layout
                .content_store_descriptor("cs_00000000000000000000000000000001")
                .as_str(),
            "content-stores/cs_00000000000000000000000000000001/descriptor.json"
        );
        assert_eq!(
            layout
                .wal_segment("ns-1", 420, 425, "seg_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/wal/00000000000000000420-00000000000000000425-seg_00000000000000000000000000000001.cbor.zst"
        );
        assert_eq!(
            layout.checkpoint_manifest("ns-1", 400).as_str(),
            "namespaces/ns-1/compacted/checkpoints/00000000000000000400/manifest.json"
        );
        assert_eq!(
            layout
                .checkpoint_run_table(
                    "ns-1",
                    400,
                    "run_00000000000000000000000000000001",
                    CheckpointTableFamily::DirentryBinds,
                    7
                )
                .as_str(),
            "namespaces/ns-1/compacted/checkpoints/00000000000000000400/runs/run_00000000000000000000000000000001/tables/direntry-binds/00007.sst.zst"
        );
        assert_eq!(
            layout
                .derived_progress("ns-1", DerivedWorkClass::CheckpointBuilder)
                .as_str(),
            "namespaces/ns-1/derived/checkpoint-builder/progress.json"
        );
        assert_eq!(
            layout
                .upload_session("ns-1", "upl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
        assert_eq!(layout.queue_shard(17).as_str(), "queue/shards/00017.json");
        assert_eq!(
            layout
                .content_blob(
                    "cs_00000000000000000000000000000001",
                    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                )
                .expect("content key")
                .as_str(),
            "content-stores/cs_00000000000000000000000000000001/blobs/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn content_blob_rejects_invalid_sha256_digest() {
        assert!(sha256_hex_from_digest("sha1:abcdef").is_err());
        assert!(sha256_hex_from_digest("sha256:abcd").is_err());
        assert!(sha256_hex_from_digest(
            "sha256:ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )
        .is_err());
        assert!(ObjectLayout::new()
            .content_blob("ns-1", "sha256:not-hex")
            .is_err());
    }
}
