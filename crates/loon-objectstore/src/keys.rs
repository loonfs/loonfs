use crate::layout::{self, ObjectLayout};
use crate::ObjectStoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointTableFamily {
    Inodes,
    DirentryBinds,
    DirentryChildBinds,
    DirentryUnbinds,
    Revisions,
    Tombstones,
    CommitReceipts,
}

impl CheckpointTableFamily {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedWorkClass {
    CheckpointBuilder,
}

impl DerivedWorkClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CheckpointBuilder => "checkpoint-builder",
        }
    }
}

pub fn namespace_head(namespace: &str) -> String {
    ObjectLayout::new().namespace_head(namespace).into_string()
}

pub fn namespace_descriptor(namespace: &str) -> String {
    ObjectLayout::new()
        .namespace_descriptor(namespace)
        .into_string()
}

pub fn namespace_lease(namespace: &str) -> String {
    ObjectLayout::new().namespace_lease(namespace).into_string()
}

pub fn namespace_fork_state(namespace: &str) -> String {
    ObjectLayout::new()
        .namespace_fork_state(namespace)
        .into_string()
}

pub fn wal_segment(namespace: &str, start_seq: u64, end_seq: u64, segment_id: &str) -> String {
    ObjectLayout::new()
        .wal_segment(namespace, start_seq, end_seq, segment_id)
        .into_string()
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

pub fn conflict_artifact(namespace: &str, conflict_id: &str) -> String {
    ObjectLayout::new()
        .conflict_artifact(namespace, conflict_id)
        .into_string()
}

pub fn conflict_artifact_prefix(namespace: &str) -> String {
    ObjectLayout::new().conflict_artifact_prefix(namespace)
}

pub fn upload_session(namespace: &str, upload_id: &str) -> String {
    ObjectLayout::new()
        .upload_session(namespace, upload_id)
        .into_string()
}

pub fn upload_session_prefix(namespace: &str) -> String {
    ObjectLayout::new().upload_session_prefix(namespace)
}

pub fn checkpoint_manifest(namespace: &str, seq: u64) -> String {
    ObjectLayout::new()
        .checkpoint_manifest(namespace, seq)
        .into_string()
}

pub fn checkpoint_run_table(
    namespace: &str,
    run_seq: u64,
    run_id: &str,
    family: CheckpointTableFamily,
    segment_index: u32,
) -> String {
    ObjectLayout::new()
        .checkpoint_run_table(namespace, run_seq, run_id, family, segment_index)
        .into_string()
}

pub fn derived_progress(namespace: &str, work_class: DerivedWorkClass) -> String {
    ObjectLayout::new()
        .derived_progress(namespace, work_class)
        .into_string()
}

pub fn queue_shard(shard_index: u32) -> String {
    ObjectLayout::new().queue_shard(shard_index).into_string()
}

pub fn sha256_hex_from_digest(digest: &str) -> Result<&str, ObjectStoreError> {
    layout::sha256_hex_from_digest(digest)
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_manifest, checkpoint_run_table, conflict_artifact, conflict_artifact_prefix,
        content_blob, content_store_descriptor, derived_progress, namespace_descriptor,
        namespace_fork_state, namespace_head, namespace_lease, queue_shard, sha256_hex_from_digest,
        upload_session, upload_session_prefix, wal_segment, CheckpointTableFamily,
        DerivedWorkClass,
    };

    #[test]
    fn key_builders_match_spec_examples() {
        assert_eq!(namespace_head("ns-1"), "namespaces/ns-1/control/head.json");
        assert_eq!(
            namespace_descriptor("ns-1"),
            "namespaces/ns-1/descriptor.json"
        );
        assert_eq!(
            namespace_lease("ns-1"),
            "namespaces/ns-1/control/lease.json"
        );
        assert_eq!(
            namespace_fork_state("ns-1"),
            "namespaces/ns-1/control/fork.json"
        );
        assert_eq!(
            content_store_descriptor("cs_00000000000000000000000000000001"),
            "content-stores/cs_00000000000000000000000000000001/descriptor.json"
        );
        assert_eq!(
            wal_segment("ns-1", 420, 425, "seg_00000000000000000000000000000001"),
            "namespaces/ns-1/wal/00000000000000000420-00000000000000000425-seg_00000000000000000000000000000001.cbor.zst"
        );
        assert_eq!(
            checkpoint_manifest("ns-1", 400),
            "namespaces/ns-1/compacted/checkpoints/00000000000000000400/manifest.json"
        );
        assert_eq!(
            checkpoint_run_table(
                "ns-1",
                400,
                "run_00000000000000000000000000000001",
                CheckpointTableFamily::DirentryBinds,
                7
            ),
            "namespaces/ns-1/compacted/checkpoints/00000000000000000400/runs/run_00000000000000000000000000000001/tables/direntry-binds/00007.sst.zst"
        );
        assert_eq!(
            derived_progress("ns-1", DerivedWorkClass::CheckpointBuilder),
            "namespaces/ns-1/derived/checkpoint-builder/progress.json"
        );
        assert_eq!(queue_shard(17), "queue/shards/00017.json");
        assert_eq!(
            content_blob(
                "cs_00000000000000000000000000000001",
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            )
            .expect("content key"),
            "content-stores/cs_00000000000000000000000000000001/blobs/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
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
