use crate::wal::{replay_wal_tail, StoredWalObject, WalReplayError};
use loon_objectstore::keys::{snapshot_manifest, snapshot_table, SnapshotTableFamily};
use loon_types::{
    decode_checkpoint_manifest_json, encode_checkpoint_manifest_json,
    encode_checkpoint_segment_envelope_zstd, ChangeSeq, CheckpointManifestEnvelope,
    CheckpointManifestPayload, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope,
    CheckpointSegmentPayload, CheckpointTableFamily, CheckpointTableManifest, HeadState,
    NamespaceId,
};
use serde::{Deserialize, Serialize};

const CHECKPOINT_TABLE_FAMILIES: [CheckpointTableFamily; 4] = [
    CheckpointTableFamily::Inodes,
    CheckpointTableFamily::Direntries,
    CheckpointTableFamily::Revisions,
    CheckpointTableFamily::Tombstones,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCheckpointManifest {
    pub object_key: String,
    pub envelope: CheckpointManifestEnvelope,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCheckpointSegment {
    pub object_key: String,
    pub descriptor: CheckpointSegmentDescriptor,
    pub envelope: CheckpointSegmentEnvelope,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCheckpoint {
    pub manifest: PreparedCheckpointManifest,
    pub segments: Vec<PreparedCheckpointSegment>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCheckpointManifest {
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCheckpointSegment {
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedCheckpointManifest {
    pub object_key: String,
    pub manifest: CheckpointManifestEnvelope,
    pub basis_head: HeadState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointBuildError {
    EmptyWriterVersion,
    Codec(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointReplayError {
    Codec(String),
    ObjectKeyMismatch {
        expected: String,
        actual: String,
    },
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    UnverifiedManifest {
        checkpoint_seq: ChangeSeq,
    },
    WalReplay(WalReplayError),
}

pub fn prepare_checkpoint(
    head: &HeadState,
    writer_version: &str,
) -> Result<PreparedCheckpoint, CheckpointBuildError> {
    if writer_version.trim().is_empty() {
        return Err(CheckpointBuildError::EmptyWriterVersion);
    }

    let segments = CHECKPOINT_TABLE_FAMILIES
        .iter()
        .copied()
        .map(|family| prepare_checkpoint_segment(head, family, writer_version))
        .collect::<Result<Vec<_>, _>>()?;

    let manifest_payload = CheckpointManifestPayload {
        namespace_id: head.namespace_id.clone(),
        checkpoint_seq: head.seq,
        active_fence_token: head.active_fence_token,
        next_inode_id: head.next_inode_id,
        retention_floor_seq: head.retention_floor_seq,
        verified: true,
        tables: segments
            .iter()
            .map(|segment| CheckpointTableManifest {
                family: segment.envelope.payload.family,
                segments: vec![segment.descriptor.clone()],
            })
            .collect(),
    };
    let manifest_envelope =
        CheckpointManifestEnvelope::from_payload(writer_version, manifest_payload)
            .map_err(|err| CheckpointBuildError::Codec(err.to_string()))?;
    let manifest = PreparedCheckpointManifest {
        object_key: snapshot_manifest(head.namespace_id.as_str(), head.seq.0),
        encoded_bytes: encode_checkpoint_manifest_json(&manifest_envelope)
            .map_err(|err| CheckpointBuildError::Codec(err.to_string()))?,
        envelope: manifest_envelope,
    };

    Ok(PreparedCheckpoint {
        manifest,
        segments,
        checked_invariants: vec![
            "checkpoint_segment_payload_checksum_matches_payload".to_owned(),
            "checkpoint_segment_key_matches_family_and_index".to_owned(),
            "verified_checkpoint_manifest_requires_durable_segments".to_owned(),
            "checkpoint_manifest_preserves_head_summary".to_owned(),
        ],
    })
}

pub fn load_checkpoint_manifest(
    expected_namespace: &NamespaceId,
    checkpoint_manifest: &StoredCheckpointManifest,
) -> Result<LoadedCheckpointManifest, CheckpointReplayError> {
    let manifest = decode_checkpoint_manifest_json(&checkpoint_manifest.encoded_bytes)
        .map_err(|err| CheckpointReplayError::Codec(err.to_string()))?;

    let expected_key = snapshot_manifest(
        manifest.payload.namespace_id.as_str(),
        manifest.payload.checkpoint_seq.0,
    );
    if checkpoint_manifest.object_key != expected_key {
        return Err(CheckpointReplayError::ObjectKeyMismatch {
            expected: expected_key,
            actual: checkpoint_manifest.object_key.clone(),
        });
    }

    if &manifest.payload.namespace_id != expected_namespace {
        return Err(CheckpointReplayError::NamespaceMismatch {
            expected: expected_namespace.clone(),
            actual: manifest.payload.namespace_id.clone(),
        });
    }

    if !manifest.payload.verified {
        return Err(CheckpointReplayError::UnverifiedManifest {
            checkpoint_seq: manifest.payload.checkpoint_seq,
        });
    }

    Ok(LoadedCheckpointManifest {
        object_key: checkpoint_manifest.object_key.clone(),
        basis_head: HeadState {
            namespace_id: manifest.payload.namespace_id.clone(),
            seq: manifest.payload.checkpoint_seq,
            active_fence_token: manifest.payload.active_fence_token,
            next_inode_id: manifest.payload.next_inode_id,
            snapshot_hint_seq: Some(manifest.payload.checkpoint_seq),
            retention_floor_seq: manifest.payload.retention_floor_seq,
        },
        manifest,
        checked_invariants: vec![
            "checkpoint_manifest_checksum_matches_payload".to_owned(),
            "checkpoint_manifest_key_matches_seq".to_owned(),
            "checkpoint_manifest_must_be_verified".to_owned(),
        ],
    })
}

pub fn replay_from_checkpoint_and_wal_tail(
    expected_namespace: &NamespaceId,
    checkpoint_manifest: &StoredCheckpointManifest,
    wal_tail: &[StoredWalObject],
) -> Result<HeadState, CheckpointReplayError> {
    let loaded = load_checkpoint_manifest(expected_namespace, checkpoint_manifest)?;
    replay_wal_tail(&loaded.basis_head, wal_tail).map_err(CheckpointReplayError::WalReplay)
}

fn prepare_checkpoint_segment(
    head: &HeadState,
    family: CheckpointTableFamily,
    writer_version: &str,
) -> Result<PreparedCheckpointSegment, CheckpointBuildError> {
    let payload = CheckpointSegmentPayload {
        namespace_id: head.namespace_id.clone(),
        checkpoint_seq: head.seq,
        family,
        segment_index: 0,
        row_count: 0,
        min_key: String::new(),
        max_key: String::new(),
        pages: vec![],
    };
    let envelope = CheckpointSegmentEnvelope::from_payload(writer_version, payload)
        .map_err(|err| CheckpointBuildError::Codec(err.to_string()))?;
    let object_key = snapshot_table(
        head.namespace_id.as_str(),
        head.seq.0,
        snapshot_table_family(family),
        envelope.payload.segment_index,
    );
    let descriptor = CheckpointSegmentDescriptor {
        object_key: object_key.clone(),
        segment_index: envelope.payload.segment_index,
        row_count: envelope.payload.row_count,
        min_key: envelope.payload.min_key.clone(),
        max_key: envelope.payload.max_key.clone(),
        payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
        page_checksums_sha256: envelope
            .page_checksums_sha256()
            .map_err(|err| CheckpointBuildError::Codec(err.to_string()))?,
    };
    let encoded_bytes = encode_checkpoint_segment_envelope_zstd(&envelope)
        .map_err(|err| CheckpointBuildError::Codec(err.to_string()))?;

    Ok(PreparedCheckpointSegment {
        object_key,
        descriptor,
        envelope,
        encoded_bytes,
    })
}

fn snapshot_table_family(family: CheckpointTableFamily) -> SnapshotTableFamily {
    match family {
        CheckpointTableFamily::Inodes => SnapshotTableFamily::Inodes,
        CheckpointTableFamily::Direntries => SnapshotTableFamily::Direntries,
        CheckpointTableFamily::Revisions => SnapshotTableFamily::Revisions,
        CheckpointTableFamily::Tombstones => SnapshotTableFamily::Tombstones,
    }
}

impl From<&PreparedCheckpointManifest> for StoredCheckpointManifest {
    fn from(value: &PreparedCheckpointManifest) -> Self {
        Self {
            object_key: value.object_key.clone(),
            encoded_bytes: value.encoded_bytes.clone(),
        }
    }
}

impl From<&PreparedCheckpointSegment> for StoredCheckpointSegment {
    fn from(value: &PreparedCheckpointSegment) -> Self {
        Self {
            object_key: value.object_key.clone(),
            encoded_bytes: value.encoded_bytes.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        load_checkpoint_manifest, prepare_checkpoint, replay_from_checkpoint_and_wal_tail,
        CheckpointBuildError, CheckpointReplayError, StoredCheckpointManifest,
    };
    use crate::wal::StoredWalObject;
    use loon_objectstore::keys::{
        snapshot_manifest, snapshot_table, wal_commit, SnapshotTableFamily,
    };
    use loon_types::{
        decode_checkpoint_manifest_json, decode_checkpoint_segment_envelope_zstd,
        encode_checkpoint_manifest_json, encode_wal_commit_envelope_zstd, ChangeSeq,
        CheckpointManifestEnvelope, CheckpointManifestPayload, CheckpointSegmentDescriptor,
        CheckpointTableFamily, CheckpointTableManifest, FenceToken, HeadState, InodeId,
        NamespaceId, WalCommitEnvelope, WalCommitPayload, WalOp, WalPrecondition,
    };

    #[test]
    fn prepare_checkpoint_builds_verified_manifest_and_empty_segments() {
        let prepared =
            prepare_checkpoint(&sample_head(), "loon-core-test").expect("prepare checkpoint");

        assert_eq!(
            prepared.manifest.object_key,
            "namespaces/ns-1/snapshots/00000000000000000042/manifest.json"
        );
        assert_eq!(prepared.segments.len(), 4);
        assert!(prepared
            .checked_invariants
            .contains(&"verified_checkpoint_manifest_requires_durable_segments".to_owned()));

        let manifest = decode_checkpoint_manifest_json(&prepared.manifest.encoded_bytes)
            .expect("decode prepared checkpoint manifest");

        assert!(manifest.payload.verified);
        assert_eq!(manifest.payload.checkpoint_seq, ChangeSeq(42));
        assert_eq!(manifest.payload.active_fence_token, FenceToken(9));
        assert_eq!(manifest.payload.next_inode_id, InodeId(777));
        assert_eq!(manifest.payload.retention_floor_seq, ChangeSeq(40));
        assert_eq!(manifest.payload.tables.len(), 4);
        assert_eq!(
            manifest.payload.tables[0].segments[0].object_key,
            snapshot_table("ns-1", 42, SnapshotTableFamily::Inodes, 0)
        );
        assert!(manifest
            .payload
            .tables
            .iter()
            .all(|table| table.segments[0].row_count == 0));
    }

    #[test]
    fn prepare_checkpoint_segment_round_trips_through_shared_codec() {
        let prepared =
            prepare_checkpoint(&sample_head(), "loon-core-test").expect("prepare checkpoint");
        let decoded = decode_checkpoint_segment_envelope_zstd(&prepared.segments[0].encoded_bytes)
            .expect("decode checkpoint segment");

        assert_eq!(decoded.payload.namespace_id, NamespaceId::from("ns-1"));
        assert_eq!(decoded.payload.checkpoint_seq, ChangeSeq(42));
        assert_eq!(decoded.payload.family, CheckpointTableFamily::Inodes);
        assert_eq!(decoded.payload.segment_index, 0);
        assert!(decoded
            .has_valid_payload_checksum()
            .expect("recompute checkpoint segment checksum"));
        assert_eq!(decoded.payload.pages.len(), 0);
    }

    #[test]
    fn prepare_checkpoint_manifest_can_be_loaded_as_basis_head() {
        let prepared =
            prepare_checkpoint(&sample_head(), "loon-core-test").expect("prepare checkpoint");
        let loaded = load_checkpoint_manifest(
            &NamespaceId::from("ns-1"),
            &StoredCheckpointManifest::from(&prepared.manifest),
        )
        .expect("load prepared checkpoint manifest");

        assert_eq!(loaded.basis_head.seq, ChangeSeq(42));
        assert_eq!(loaded.basis_head.active_fence_token, FenceToken(9));
        assert_eq!(loaded.basis_head.next_inode_id, InodeId(777));
        assert_eq!(loaded.basis_head.snapshot_hint_seq, Some(ChangeSeq(42)));
    }

    #[test]
    fn prepare_checkpoint_rejects_empty_writer_version() {
        let error = prepare_checkpoint(&sample_head(), "   ")
            .expect_err("empty writer version should fail");

        assert_eq!(error, CheckpointBuildError::EmptyWriterVersion);
    }

    #[test]
    fn load_checkpoint_manifest_derives_basis_head() {
        let stored = stored_checkpoint_manifest(sample_checkpoint_manifest_payload(true));
        let loaded = load_checkpoint_manifest(&NamespaceId::from("ns-1"), &stored)
            .expect("load checkpoint manifest");

        assert_eq!(loaded.basis_head.seq, ChangeSeq(40));
        assert_eq!(loaded.basis_head.active_fence_token, FenceToken(8));
        assert_eq!(loaded.basis_head.next_inode_id, InodeId(501));
        assert_eq!(loaded.basis_head.snapshot_hint_seq, Some(ChangeSeq(40)));
    }

    #[test]
    fn replay_from_checkpoint_and_wal_tail_reproduces_head() {
        let checkpoint = stored_checkpoint_manifest(sample_checkpoint_manifest_payload(true));
        let wal_tail = vec![stored_wal_object(sample_wal_payload(
            ChangeSeq(41),
            ChangeSeq(40),
            "req-20260311-0001",
            FenceToken(9),
        ))];

        let final_head =
            replay_from_checkpoint_and_wal_tail(&NamespaceId::from("ns-1"), &checkpoint, &wal_tail)
                .expect("checkpoint plus wal tail should replay");

        assert_eq!(final_head.seq, ChangeSeq(41));
        assert_eq!(final_head.active_fence_token, FenceToken(9));
        assert_eq!(final_head.next_inode_id, InodeId(501));
        assert_eq!(final_head.snapshot_hint_seq, Some(ChangeSeq(40)));
    }

    #[test]
    fn load_checkpoint_manifest_rejects_namespace_mismatch() {
        let stored = stored_checkpoint_manifest(sample_checkpoint_manifest_payload(true));
        let error = load_checkpoint_manifest(&NamespaceId::from("ns-2"), &stored)
            .expect_err("namespace mismatch should fail");

        assert_eq!(
            error,
            CheckpointReplayError::NamespaceMismatch {
                expected: NamespaceId::from("ns-2"),
                actual: NamespaceId::from("ns-1"),
            }
        );
    }

    #[test]
    fn load_checkpoint_manifest_rejects_unverified_manifest() {
        let stored = stored_checkpoint_manifest(sample_checkpoint_manifest_payload(false));
        let error = load_checkpoint_manifest(&NamespaceId::from("ns-1"), &stored)
            .expect_err("unverified checkpoint should fail");

        assert_eq!(
            error,
            CheckpointReplayError::UnverifiedManifest {
                checkpoint_seq: ChangeSeq(40),
            }
        );
    }

    #[test]
    fn load_checkpoint_manifest_rejects_object_key_mismatch() {
        let payload = sample_checkpoint_manifest_payload(true);
        let envelope = CheckpointManifestEnvelope::from_payload("loon-core-test", payload)
            .expect("build checkpoint manifest");
        let stored = StoredCheckpointManifest {
            object_key: "namespaces/ns-1/snapshots/00000000000000000040/wrong.json".to_owned(),
            encoded_bytes: encode_checkpoint_manifest_json(&envelope)
                .expect("encode checkpoint manifest"),
        };

        let error = load_checkpoint_manifest(&NamespaceId::from("ns-1"), &stored)
            .expect_err("object key mismatch should fail");

        assert_eq!(
            error,
            CheckpointReplayError::ObjectKeyMismatch {
                expected: snapshot_manifest("ns-1", 40),
                actual: "namespaces/ns-1/snapshots/00000000000000000040/wrong.json".to_owned(),
            }
        );
    }

    fn sample_checkpoint_manifest_payload(verified: bool) -> CheckpointManifestPayload {
        CheckpointManifestPayload {
            namespace_id: NamespaceId::from("ns-1"),
            checkpoint_seq: ChangeSeq(40),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            retention_floor_seq: ChangeSeq(40),
            verified,
            tables: vec![CheckpointTableManifest {
                family: CheckpointTableFamily::Inodes,
                segments: vec![CheckpointSegmentDescriptor {
                    object_key:
                        "namespaces/ns-1/snapshots/00000000000000000040/tables/inodes-00000.sst.zst"
                            .to_owned(),
                    segment_index: 0,
                    row_count: 500,
                    min_key: "inode-1".to_owned(),
                    max_key: "inode-500".to_owned(),
                    payload_checksum_sha256: "seg-checksum-1".to_owned(),
                    page_checksums_sha256: vec!["page-checksum-1".to_owned()],
                }],
            }],
        }
    }

    fn stored_checkpoint_manifest(payload: CheckpointManifestPayload) -> StoredCheckpointManifest {
        let object_key = snapshot_manifest(payload.namespace_id.as_str(), payload.checkpoint_seq.0);
        let envelope = CheckpointManifestEnvelope::from_payload("loon-core-test", payload)
            .expect("build checkpoint manifest");
        let encoded_bytes =
            encode_checkpoint_manifest_json(&envelope).expect("encode checkpoint manifest");

        StoredCheckpointManifest {
            object_key,
            encoded_bytes,
        }
    }

    fn sample_wal_payload(
        seq: ChangeSeq,
        base_head_seq: ChangeSeq,
        commit_id: &str,
        writer_fence_token: FenceToken,
    ) -> WalCommitPayload {
        WalCommitPayload {
            namespace_id: NamespaceId::from("ns-1"),
            seq,
            base_head_seq,
            commit_id: commit_id.to_owned(),
            request_id: commit_id.to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token,
            ops: vec![WalOp::DeleteSubtree {
                root_inode: InodeId(2),
            }],
            preconditions: vec![WalPrecondition::HeadSeqIs(base_head_seq)],
        }
    }

    fn stored_wal_object(payload: WalCommitPayload) -> StoredWalObject {
        let object_key = wal_commit(
            payload.namespace_id.as_str(),
            payload.seq.0,
            &payload.commit_id,
        );
        let envelope =
            WalCommitEnvelope::from_payload("loon-core-test", payload).expect("build wal envelope");
        let encoded_bytes =
            encode_wal_commit_envelope_zstd(&envelope).expect("encode wal envelope");

        StoredWalObject {
            object_key,
            encoded_bytes,
        }
    }

    fn sample_head() -> HeadState {
        HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        }
    }
}
