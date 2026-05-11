use crate::ObjectStoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotTableFamily {
    Inodes,
    Direntries,
    Revisions,
    Tombstones,
    CommitReceipts,
}

impl SnapshotTableFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inodes => "inodes",
            Self::Direntries => "direntries",
            Self::Revisions => "revisions",
            Self::Tombstones => "tombstones",
            Self::CommitReceipts => "commit-receipts",
        }
    }
}

pub fn namespace_head(namespace: &str) -> String {
    format!("namespaces/{namespace}/head.json")
}

pub fn namespace_descriptor(namespace: &str) -> String {
    format!("namespaces/{namespace}/descriptor.json")
}

pub fn namespace_lease(namespace: &str) -> String {
    format!("namespaces/{namespace}/lease.json")
}

pub fn wal_segment(namespace: &str, start_seq: u64, end_seq: u64, segment_id: &str) -> String {
    format!("namespaces/{namespace}/wal/{start_seq:020}-{end_seq:020}-{segment_id}.cbor.zst")
}

pub fn content_store_descriptor(content_store: &str) -> String {
    format!("content-stores/{content_store}/descriptor.json")
}

pub fn content_blob(content_store: &str, digest: &str) -> Result<String, ObjectStoreError> {
    let hex = sha256_hex_from_digest(digest)?;
    Ok(format!(
        "content-stores/{content_store}/blobs/sha256/{}/{}/{}",
        &hex[0..2],
        &hex[2..4],
        hex
    ))
}

pub fn conflict_artifact(namespace: &str, conflict_id: &str) -> String {
    format!("namespaces/{namespace}/conflicts/{conflict_id}.json")
}

pub fn conflict_artifact_prefix(namespace: &str) -> String {
    format!("namespaces/{namespace}/conflicts/")
}

pub fn upload_session(namespace: &str, upload_id: &str) -> String {
    format!("namespaces/{namespace}/control/uploads/{upload_id}.json")
}

pub fn upload_session_prefix(namespace: &str) -> String {
    format!("namespaces/{namespace}/control/uploads/")
}

pub fn snapshot_manifest(namespace: &str, seq: u64) -> String {
    format!("namespaces/{namespace}/snapshots/{seq:020}/manifest.json")
}

pub fn snapshot_table(
    namespace: &str,
    seq: u64,
    family: SnapshotTableFamily,
    segment_index: u32,
) -> String {
    format!(
        "namespaces/{namespace}/snapshots/{seq:020}/tables/{}-{segment_index:05}.sst.zst",
        family.as_str()
    )
}

pub fn derived_progress(namespace: &str, work_class: &str) -> String {
    format!("namespaces/{namespace}/derived/{work_class}/progress.json")
}

pub fn queue_shard(shard_index: u32) -> String {
    format!("queue/shards/{shard_index:05}.json")
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
    use super::{
        conflict_artifact, conflict_artifact_prefix, content_blob, content_store_descriptor,
        derived_progress, namespace_descriptor, namespace_head, namespace_lease, queue_shard,
        sha256_hex_from_digest, snapshot_manifest, snapshot_table, upload_session,
        upload_session_prefix, wal_segment, SnapshotTableFamily,
    };

    #[test]
    fn key_builders_match_spec_examples() {
        assert_eq!(namespace_head("ns-1"), "namespaces/ns-1/head.json");
        assert_eq!(
            namespace_descriptor("ns-1"),
            "namespaces/ns-1/descriptor.json"
        );
        assert_eq!(namespace_lease("ns-1"), "namespaces/ns-1/lease.json");
        assert_eq!(
            content_store_descriptor("cs-1"),
            "content-stores/cs-1/descriptor.json"
        );
        assert_eq!(
            wal_segment("ns-1", 420, 425, "seg-123"),
            "namespaces/ns-1/wal/00000000000000000420-00000000000000000425-seg-123.cbor.zst"
        );
        assert_eq!(
            snapshot_manifest("ns-1", 400),
            "namespaces/ns-1/snapshots/00000000000000000400/manifest.json"
        );
        assert_eq!(
            snapshot_table("ns-1", 400, SnapshotTableFamily::Direntries, 7),
            "namespaces/ns-1/snapshots/00000000000000000400/tables/direntries-00007.sst.zst"
        );
        assert_eq!(
            snapshot_table("ns-1", 400, SnapshotTableFamily::CommitReceipts, 0),
            "namespaces/ns-1/snapshots/00000000000000000400/tables/commit-receipts-00000.sst.zst"
        );
        assert_eq!(
            derived_progress("ns-1", "BuildSnapshot"),
            "namespaces/ns-1/derived/BuildSnapshot/progress.json"
        );
        assert_eq!(queue_shard(17), "queue/shards/00017.json");
        assert_eq!(
            content_blob(
                "cs-1",
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            )
            .expect("content key"),
            "content-stores/cs-1/blobs/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
        assert_eq!(
            conflict_artifact("ns-1", "conflict-deadbeef"),
            "namespaces/ns-1/conflicts/conflict-deadbeef.json"
        );
        assert_eq!(
            conflict_artifact_prefix("ns-1"),
            "namespaces/ns-1/conflicts/"
        );
        assert_eq!(
            upload_session("ns-1", "upl-123"),
            "namespaces/ns-1/control/uploads/upl-123.json"
        );
        assert_eq!(
            upload_session_prefix("ns-1"),
            "namespaces/ns-1/control/uploads/"
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
        assert!(content_blob("ns-1", "sha256:not-hex").is_err());
    }
}
