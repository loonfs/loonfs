use crate::layout::{self, ObjectLayout};
use crate::ObjectStoreError;
use loonfs_api::ManifestId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTableFamily {
    Inodes,
    DirentryBinds,
    DirentryChildBinds,
    DirentryUnbinds,
    Revisions,
    Tombstones,
    CommitReceipts,
}

impl MetadataTableFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inodes => "inodes",
            Self::DirentryBinds => "direntry-binds",
            Self::DirentryChildBinds => "direntry-child-binds",
            Self::DirentryUnbinds => "direntry-unbinds",
            Self::Revisions => "revisions",
            Self::Tombstones => "tombstones",
            Self::CommitReceipts => "commit-receipts",
        }
    }
}

pub fn namespace_config(namespace: &str) -> String {
    ObjectLayout::new()
        .namespace_config(namespace)
        .into_string()
}

pub fn wal_head(namespace: &str) -> String {
    ObjectLayout::new().wal_head(namespace).into_string()
}

pub fn wal_floor(namespace: &str) -> String {
    ObjectLayout::new().wal_floor(namespace).into_string()
}

pub fn wal_index(namespace: &str) -> String {
    ObjectLayout::new().wal_index(namespace).into_string()
}

pub fn wal_index_run(namespace: &str, index_id: &str) -> String {
    ObjectLayout::new()
        .wal_index_run(namespace, index_id)
        .into_string()
}

pub fn wal_segment(namespace: &str, segment_id: &str) -> String {
    ObjectLayout::new()
        .wal_segment(namespace, segment_id)
        .into_string()
}

pub fn wal_segment_prefix(namespace: &str) -> String {
    ObjectLayout::new().wal_segment_prefix(namespace)
}

pub fn wal_segment_id_from_key(key: &str) -> Option<&str> {
    ObjectLayout::new().wal_segment_id_from_key(key)
}

pub fn metadata_root(namespace: &str) -> String {
    ObjectLayout::new().metadata_root(namespace).into_string()
}

pub fn metadata_manifest(namespace: &str, manifest_id: ManifestId) -> String {
    ObjectLayout::new()
        .metadata_manifest(namespace, manifest_id)
        .into_string()
}

pub fn metadata_table(namespace: &str, table_id: &str) -> String {
    ObjectLayout::new()
        .metadata_table(namespace, table_id)
        .into_string()
}

pub fn checkpoint_record(namespace: &str, checkpoint_id: &str) -> String {
    ObjectLayout::new()
        .checkpoint_record(namespace, checkpoint_id)
        .into_string()
}

pub fn checkpoint_prefix(namespace: &str) -> String {
    ObjectLayout::new().checkpoint_prefix(namespace)
}

pub fn pin(source_namespace: &str, pin_id: &str) -> String {
    ObjectLayout::new()
        .pin(source_namespace, pin_id)
        .into_string()
}

pub fn pin_prefix(source_namespace: &str) -> String {
    ObjectLayout::new().pin_prefix(source_namespace)
}

pub fn upload_session(namespace: &str, upload_id: &str) -> String {
    ObjectLayout::new()
        .upload_session(namespace, upload_id)
        .into_string()
}

pub fn upload_session_prefix(namespace: &str) -> String {
    ObjectLayout::new().upload_session_prefix(namespace)
}

pub fn content_store_descriptor(content_store: &str) -> String {
    ObjectLayout::new()
        .content_store_descriptor(content_store)
        .into_string()
}

pub fn content_blob(content_store: &str, digest: &str) -> Result<String, ObjectStoreError> {
    ObjectLayout::new()
        .content_blob(content_store, digest)
        .map(|key| key.into_string())
}

pub fn sha256_hex_from_digest(digest: &str) -> Result<&str, ObjectStoreError> {
    layout::sha256_hex_from_digest(digest)
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_record, content_blob, content_store_descriptor, metadata_manifest,
        metadata_root, metadata_table, namespace_config, pin, sha256_hex_from_digest,
        upload_session, upload_session_prefix, wal_floor, wal_head, wal_segment,
        wal_segment_id_from_key, wal_segment_prefix,
    };
    use loonfs_api::ManifestId;

    #[test]
    fn key_builders_match_spec_examples() {
        assert_eq!(namespace_config("ns-1"), "namespaces/ns-1/namespace.json");
        assert_eq!(wal_head("ns-1"), "namespaces/ns-1/wal/head.json");
        assert_eq!(wal_floor("ns-1"), "namespaces/ns-1/wal/floor.json");
        assert_eq!(
            content_store_descriptor("cs_00000000000000000000000000000001"),
            "content-stores/cs_00000000000000000000000000000001/descriptor.json"
        );
        assert_eq!(
            wal_segment("ns-1", "seg_00000000000000000000000000000001"),
            "namespaces/ns-1/wal/segments/seg_00000000000000000000000000000001.wal.zst"
        );
        assert_eq!(wal_segment_prefix("ns-1"), "namespaces/ns-1/wal/segments/");
        assert!(wal_segment("ns-1", "00000000000000000042-0123456789abcdef")
            .starts_with(&wal_segment_prefix("ns-1")));
        assert!(!wal_head("ns-1").starts_with(&wal_segment_prefix("ns-1")));
        assert!(!wal_floor("ns-1").starts_with(&wal_segment_prefix("ns-1")));
        assert_eq!(
            wal_segment_id_from_key(&wal_segment(
                "ns-1",
                "00000000000000000042-0123456789abcdef"
            )),
            Some("00000000000000000042-0123456789abcdef")
        );
        assert_eq!(
            wal_segment_id_from_key("namespaces/ns-1/wal/segments/random.tmp"),
            None
        );
        assert_eq!(metadata_root("ns-1"), "namespaces/ns-1/metadata/root.json");
        assert_eq!(
            metadata_manifest("ns-1", ManifestId(400)),
            "namespaces/ns-1/metadata/manifests/00000000000000000400.json"
        );
        assert_eq!(
            metadata_table("ns-1", "tbl_00000000000000000000000000000001"),
            "namespaces/ns-1/metadata/tables/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            checkpoint_record("ns-1", "chk_00000000000000000000000000000001"),
            "namespaces/ns-1/checkpoints/chk_00000000000000000000000000000001.json"
        );
        assert_eq!(
            pin("source-ns", "pin_00000000000000000000000000000001"),
            "namespaces/source-ns/pins/pin_00000000000000000000000000000001.json"
        );
        assert_eq!(
            content_blob(
                "cs_00000000000000000000000000000001",
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            )
            .expect("content key"),
            "content-stores/cs_00000000000000000000000000000001/blobs/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
        assert_eq!(
            upload_session("ns-1", "upl_00000000000000000000000000000001"),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
        assert_eq!(upload_session_prefix("ns-1"), "namespaces/ns-1/uploads/");
    }

    #[test]
    fn content_blob_rejects_invalid_sha256_digest() {
        assert!(sha256_hex_from_digest("sha1:abcdef").is_err());
        assert!(sha256_hex_from_digest("sha256:abcd").is_err());
        assert!(sha256_hex_from_digest(
            "sha256:ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )
        .is_err());
        assert!(content_blob("ns-1", "sha256:not-hex").is_err());
    }
}
