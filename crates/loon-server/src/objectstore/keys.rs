// Re-export client-visible key builders from loon-types.
pub use loon_types::object_store_keys::{
    blob, conflict_artifact, conflict_artifact_archive, conflict_artifact_archive_prefix,
    conflict_artifact_prefix, content_manifest,
};

// Server-only key builders and types below.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotTableFamily {
    Inodes,
    Direntries,
    Revisions,
    Tombstones,
}

impl SnapshotTableFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inodes => "inodes",
            Self::Direntries => "direntries",
            Self::Revisions => "revisions",
            Self::Tombstones => "tombstones",
        }
    }
}

pub fn namespace_head(namespace: &str) -> String {
    format!("namespaces/{namespace}/head.json")
}

pub fn namespace_lease(namespace: &str) -> String {
    format!("namespaces/{namespace}/lease.json")
}

pub fn wal_commit(namespace: &str, seq: u64, commit_id: &str) -> String {
    format!("namespaces/{namespace}/wal/{seq:020}-{commit_id}.cbor.zst")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_key_builders_match_spec_examples() {
        assert_eq!(namespace_head("ns-1"), "namespaces/ns-1/head.json");
        assert_eq!(namespace_lease("ns-1"), "namespaces/ns-1/lease.json");
        assert_eq!(
            wal_commit("ns-1", 420, "commit-123"),
            "namespaces/ns-1/wal/00000000000000000420-commit-123.cbor.zst"
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
            derived_progress("ns-1", "BuildSnapshot"),
            "namespaces/ns-1/derived/BuildSnapshot/progress.json"
        );
        assert_eq!(queue_shard(17), "queue/shards/00017.json");
    }

    #[test]
    fn client_key_re_exports_match_spec_examples() {
        assert_eq!(
            blob("ns-1", "sha256:abcd"),
            "namespaces/ns-1/blobs/sha256:abcd"
        );
        assert_eq!(
            content_manifest("ns-1", "sha256:manifest-abcd"),
            "namespaces/ns-1/manifests/sha256:manifest-abcd.json"
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
            conflict_artifact_archive("ns-1", "conflict-deadbeef"),
            "namespaces/ns-1/conflict-archives/conflict-deadbeef.json"
        );
        assert_eq!(
            conflict_artifact_archive_prefix("ns-1"),
            "namespaces/ns-1/conflict-archives/"
        );
    }
}
