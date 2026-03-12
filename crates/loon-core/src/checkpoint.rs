use crate::progress::LoadedRetentionAuthorizers;
use crate::wal::{replay_wal_tail, StoredWalObject, WalReplayError};
use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::{
    namespace_head, snapshot_manifest, snapshot_table, SnapshotTableFamily,
};
use loon_objectstore::{ObjectMetadata, ObjectStore};
use loon_types::{
    decode_checkpoint_manifest_json, decode_checkpoint_segment_envelope_zstd,
    encode_checkpoint_manifest_json, encode_checkpoint_segment_envelope_zstd, ChangeSeq,
    CheckpointManifestEnvelope, CheckpointManifestPayload, CheckpointSegmentDescriptor,
    CheckpointSegmentEnvelope, CheckpointSegmentPayload, CheckpointTableFamily,
    CheckpointTableManifest, ControlObjectKind, HeadState, HeadStateEnvelope, NamespaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
pub struct LoadedCheckpointSegment {
    pub object_key: String,
    pub descriptor: CheckpointSegmentDescriptor,
    pub envelope: CheckpointSegmentEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedCheckpoint {
    pub object_key: String,
    pub manifest: CheckpointManifestEnvelope,
    pub basis_head: HeadState,
    pub segments: Vec<LoadedCheckpointSegment>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointHeadPublishRequest {
    pub requested_retention_floor_seq: Option<ChangeSeq>,
    pub retention_authorizers: Option<LoadedRetentionAuthorizers>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCheckpointHeadPublish {
    pub object_key: String,
    pub resulting_head: HeadState,
    pub envelope: HeadStateEnvelope,
    pub encoded_bytes: Vec<u8>,
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
    MissingSegment {
        object_key: String,
    },
    UnexpectedSegment {
        object_key: String,
    },
    SegmentObjectKeyMismatch {
        expected: String,
        actual: String,
    },
    SegmentDescriptorMismatch {
        object_key: String,
        expected: CheckpointSegmentDescriptor,
        actual: CheckpointSegmentDescriptor,
    },
    WalReplay(WalReplayError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointPublishError {
    EmptyWriterVersion,
    EmptyExpectedHeadEtag,
    UnverifiedCheckpoint {
        checkpoint_seq: ChangeSeq,
    },
    NamespaceMismatch {
        head: NamespaceId,
        checkpoint: NamespaceId,
    },
    CheckpointAheadOfHead {
        checkpoint_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
    RetentionFloorRegression {
        current: ChangeSeq,
        requested: ChangeSeq,
    },
    RetentionFloorBeyondCheckpoint {
        checkpoint_seq: ChangeSeq,
        requested: ChangeSeq,
    },
    MissingRetentionAuthorizers {
        requested: ChangeSeq,
    },
    RequiredProgressLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    RetentionPolicyLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    NoHeadChangeRequired,
    HeadCasPreconditionFailed,
    Codec(String),
    Store(String),
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

pub fn load_checkpoint(
    expected_namespace: &NamespaceId,
    checkpoint_manifest: &StoredCheckpointManifest,
    checkpoint_segments: &[StoredCheckpointSegment],
) -> Result<LoadedCheckpoint, CheckpointReplayError> {
    let loaded_manifest = load_checkpoint_manifest(expected_namespace, checkpoint_manifest)?;
    let expected_segments = loaded_manifest
        .manifest
        .payload
        .tables
        .iter()
        .flat_map(|table| table.segments.iter().cloned())
        .map(|descriptor| (descriptor.object_key.clone(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let mut provided_segments = checkpoint_segments
        .iter()
        .map(|segment| (segment.object_key.clone(), segment))
        .collect::<BTreeMap<_, _>>();

    let mut loaded_segments = Vec::with_capacity(expected_segments.len());
    for (object_key, expected_descriptor) in &expected_segments {
        let stored_segment = provided_segments.remove(object_key).ok_or_else(|| {
            CheckpointReplayError::MissingSegment {
                object_key: object_key.clone(),
            }
        })?;
        let envelope = decode_checkpoint_segment_envelope_zstd(&stored_segment.encoded_bytes)
            .map_err(|err| CheckpointReplayError::Codec(err.to_string()))?;
        let expected_object_key = snapshot_table(
            envelope.payload.namespace_id.as_str(),
            envelope.payload.checkpoint_seq.0,
            snapshot_table_family(envelope.payload.family),
            envelope.payload.segment_index,
        );

        if stored_segment.object_key != expected_object_key {
            return Err(CheckpointReplayError::SegmentObjectKeyMismatch {
                expected: expected_object_key,
                actual: stored_segment.object_key.clone(),
            });
        }

        let actual_descriptor =
            checkpoint_segment_descriptor(stored_segment.object_key.clone(), &envelope)
                .map_err(|err| CheckpointReplayError::Codec(err.to_string()))?;
        if &actual_descriptor != expected_descriptor {
            return Err(CheckpointReplayError::SegmentDescriptorMismatch {
                object_key: stored_segment.object_key.clone(),
                expected: expected_descriptor.clone(),
                actual: actual_descriptor,
            });
        }

        loaded_segments.push(LoadedCheckpointSegment {
            object_key: stored_segment.object_key.clone(),
            descriptor: expected_descriptor.clone(),
            envelope,
        });
    }

    if let Some((object_key, _)) = provided_segments.into_iter().next() {
        return Err(CheckpointReplayError::UnexpectedSegment { object_key });
    }

    let mut checked_invariants = loaded_manifest.checked_invariants.clone();
    push_invariant(
        &mut checked_invariants,
        "checkpoint_replay_requires_all_manifest_segments",
    );
    push_invariant(
        &mut checked_invariants,
        "checkpoint_segment_payload_checksum_matches_payload",
    );
    push_invariant(
        &mut checked_invariants,
        "checkpoint_segment_key_matches_family_and_index",
    );
    push_invariant(
        &mut checked_invariants,
        "checkpoint_segment_descriptor_matches_payload",
    );

    Ok(LoadedCheckpoint {
        object_key: loaded_manifest.object_key,
        manifest: loaded_manifest.manifest,
        basis_head: loaded_manifest.basis_head,
        segments: loaded_segments,
        checked_invariants,
    })
}

pub fn replay_from_checkpoint_and_wal_tail(
    expected_namespace: &NamespaceId,
    checkpoint_manifest: &StoredCheckpointManifest,
    checkpoint_segments: &[StoredCheckpointSegment],
    wal_tail: &[StoredWalObject],
) -> Result<HeadState, CheckpointReplayError> {
    let loaded = load_checkpoint(expected_namespace, checkpoint_manifest, checkpoint_segments)?;
    replay_wal_tail(&loaded.basis_head, wal_tail).map_err(CheckpointReplayError::WalReplay)
}

pub fn prepare_checkpoint_head_publish(
    current_head: &HeadState,
    checkpoint: &LoadedCheckpoint,
    request: &CheckpointHeadPublishRequest,
    writer_version: &str,
) -> Result<PreparedCheckpointHeadPublish, CheckpointPublishError> {
    if writer_version.trim().is_empty() {
        return Err(CheckpointPublishError::EmptyWriterVersion);
    }

    if !checkpoint.manifest.payload.verified
        || !checkpoint
            .checked_invariants
            .iter()
            .any(|name| name == "checkpoint_segment_descriptor_matches_payload")
    {
        return Err(CheckpointPublishError::UnverifiedCheckpoint {
            checkpoint_seq: checkpoint.manifest.payload.checkpoint_seq,
        });
    }

    if current_head.namespace_id != checkpoint.manifest.payload.namespace_id {
        return Err(CheckpointPublishError::NamespaceMismatch {
            head: current_head.namespace_id.clone(),
            checkpoint: checkpoint.manifest.payload.namespace_id.clone(),
        });
    }

    if checkpoint.manifest.payload.checkpoint_seq > current_head.seq {
        return Err(CheckpointPublishError::CheckpointAheadOfHead {
            checkpoint_seq: checkpoint.manifest.payload.checkpoint_seq,
            head_seq: current_head.seq,
        });
    }

    let resulting_snapshot_hint_seq = Some(
        current_head
            .snapshot_hint_seq
            .unwrap_or(checkpoint.manifest.payload.checkpoint_seq)
            .max(checkpoint.manifest.payload.checkpoint_seq),
    );
    let resulting_retention_floor_seq = match request.requested_retention_floor_seq {
        Some(requested) => {
            if requested < current_head.retention_floor_seq {
                return Err(CheckpointPublishError::RetentionFloorRegression {
                    current: current_head.retention_floor_seq,
                    requested,
                });
            }

            if requested > checkpoint.manifest.payload.checkpoint_seq {
                return Err(CheckpointPublishError::RetentionFloorBeyondCheckpoint {
                    checkpoint_seq: checkpoint.manifest.payload.checkpoint_seq,
                    requested,
                });
            }

            let retention_authorizers = request
                .retention_authorizers
                .as_ref()
                .ok_or(CheckpointPublishError::MissingRetentionAuthorizers { requested })?;
            for progress in &retention_authorizers.required_progress {
                if progress.envelope.state.through_seq < requested {
                    return Err(CheckpointPublishError::RequiredProgressLag {
                        work_class: progress.envelope.state.work_class.clone(),
                        requested,
                        available: progress.envelope.state.through_seq,
                    });
                }
            }

            if retention_authorizers
                .retention_policy
                .envelope
                .state
                .through_seq
                < requested
            {
                return Err(CheckpointPublishError::RetentionPolicyLag {
                    work_class: retention_authorizers
                        .retention_policy
                        .envelope
                        .state
                        .work_class
                        .clone(),
                    requested,
                    available: retention_authorizers
                        .retention_policy
                        .envelope
                        .state
                        .through_seq,
                });
            }

            requested
        }
        None => current_head.retention_floor_seq,
    };

    if current_head.snapshot_hint_seq == resulting_snapshot_hint_seq
        && current_head.retention_floor_seq == resulting_retention_floor_seq
    {
        return Err(CheckpointPublishError::NoHeadChangeRequired);
    }

    let resulting_head = HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: current_head.seq,
        active_fence_token: current_head.active_fence_token,
        next_inode_id: current_head.next_inode_id,
        snapshot_hint_seq: resulting_snapshot_hint_seq,
        retention_floor_seq: resulting_retention_floor_seq,
    };
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        writer_version,
        resulting_head.clone(),
    )
    .map_err(|err| CheckpointPublishError::Codec(err.to_string()))?;
    let encoded_bytes = serde_json::to_vec(&envelope)
        .map_err(|err| CheckpointPublishError::Codec(err.to_string()))?;

    let mut checked_invariants = checkpoint.checked_invariants.clone();
    if let Some(retention_authorizers) = request.retention_authorizers.as_ref() {
        extend_invariants(
            &mut checked_invariants,
            &retention_authorizers.checked_invariants,
        );
    }
    push_invariant(
        &mut checked_invariants,
        "checkpoint_publish_requires_verified_checkpoint",
    );
    push_invariant(
        &mut checked_invariants,
        "snapshot_hint_seq_advances_monotonically",
    );
    if request.requested_retention_floor_seq.is_some() {
        push_invariant(
            &mut checked_invariants,
            "retention_floor_seq_advances_monotonically",
        );
        push_invariant(
            &mut checked_invariants,
            "retention_floor_seq_requires_checkpoint_coverage",
        );
        push_invariant(
            &mut checked_invariants,
            "retention_floor_seq_requires_derived_progress",
        );
        push_invariant(
            &mut checked_invariants,
            "retention_floor_seq_respects_policy_gate",
        );
    }

    Ok(PreparedCheckpointHeadPublish {
        object_key: namespace_head(current_head.namespace_id.as_str()),
        resulting_head,
        envelope,
        encoded_bytes,
        checked_invariants,
    })
}

pub fn publish_checkpoint_head<S: ObjectStore>(
    store: &S,
    expected_head_etag: &str,
    prepared: &PreparedCheckpointHeadPublish,
) -> Result<ObjectMetadata, CheckpointPublishError> {
    if expected_head_etag.trim().is_empty() {
        return Err(CheckpointPublishError::EmptyExpectedHeadEtag);
    }

    store
        .compare_and_swap(
            &prepared.object_key,
            expected_head_etag,
            &prepared.encoded_bytes,
        )
        .map_err(map_object_store_error)
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

fn checkpoint_segment_descriptor(
    object_key: String,
    envelope: &CheckpointSegmentEnvelope,
) -> Result<CheckpointSegmentDescriptor, String> {
    Ok(CheckpointSegmentDescriptor {
        object_key,
        segment_index: envelope.payload.segment_index,
        row_count: envelope.payload.row_count,
        min_key: envelope.payload.min_key.clone(),
        max_key: envelope.payload.max_key.clone(),
        payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
        page_checksums_sha256: envelope
            .page_checksums_sha256()
            .map_err(|err| err.to_string())?,
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

fn push_invariant(checked_invariants: &mut Vec<String>, invariant: &str) {
    if !checked_invariants.iter().any(|value| value == invariant) {
        checked_invariants.push(invariant.to_owned());
    }
}

fn extend_invariants(checked_invariants: &mut Vec<String>, new_invariants: &[String]) {
    for invariant in new_invariants {
        push_invariant(checked_invariants, invariant);
    }
}

fn map_object_store_error(err: ObjectStoreError) -> CheckpointPublishError {
    match err {
        ObjectStoreError::PreconditionFailed => CheckpointPublishError::HeadCasPreconditionFailed,
        other => CheckpointPublishError::Store(other.to_string()),
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
        load_checkpoint, load_checkpoint_manifest, prepare_checkpoint,
        prepare_checkpoint_head_publish, publish_checkpoint_head,
        replay_from_checkpoint_and_wal_tail, CheckpointBuildError, CheckpointHeadPublishRequest,
        CheckpointPublishError, CheckpointReplayError, LoadedCheckpoint, StoredCheckpointManifest,
        StoredCheckpointSegment,
    };
    use crate::progress::{
        load_retention_authorizers, LoadedProgressObject, LoadedRetentionAuthorizers,
    };
    use crate::wal::StoredWalObject;
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{
        derived_progress, namespace_head, snapshot_manifest, snapshot_table, wal_commit,
        SnapshotTableFamily,
    };
    use loon_objectstore::ObjectStore;
    use loon_types::{
        decode_checkpoint_manifest_json, decode_checkpoint_segment_envelope_zstd,
        encode_checkpoint_manifest_json, encode_wal_commit_envelope_zstd, ChangeSeq,
        CheckpointManifestEnvelope, CheckpointManifestPayload, CheckpointSegmentDescriptor,
        CheckpointTableFamily, CheckpointTableManifest, ControlObjectKind, FenceToken, HeadState,
        HeadStateEnvelope, InodeId, NamespaceId, ProgressState, ProgressStateEnvelope,
        WalCommitEnvelope, WalCommitPayload, WalOp, WalPrecondition,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn load_checkpoint_verifies_segments_before_deriving_basis_head() {
        let prepared =
            prepare_checkpoint(&sample_head(), "loon-core-test").expect("prepare checkpoint");
        let loaded = load_checkpoint(
            &NamespaceId::from("ns-1"),
            &StoredCheckpointManifest::from(&prepared.manifest),
            &stored_checkpoint_segments(&prepared),
        )
        .expect("load prepared checkpoint");

        assert_eq!(loaded.basis_head.seq, ChangeSeq(42));
        assert_eq!(loaded.basis_head.active_fence_token, FenceToken(9));
        assert_eq!(loaded.basis_head.next_inode_id, InodeId(777));
        assert_eq!(loaded.basis_head.snapshot_hint_seq, Some(ChangeSeq(42)));
        assert_eq!(loaded.segments.len(), 4);
        assert!(loaded
            .checked_invariants
            .contains(&"checkpoint_segment_descriptor_matches_payload".to_owned()));
    }

    #[test]
    fn prepare_checkpoint_head_publish_advances_snapshot_hint_and_retention_floor() {
        let loaded_checkpoint = loaded_checkpoint_for_publish();
        let prepared = prepare_checkpoint_head_publish(
            &sample_head(),
            &loaded_checkpoint,
            &CheckpointHeadPublishRequest {
                requested_retention_floor_seq: Some(ChangeSeq(42)),
                retention_authorizers: Some(sample_retention_authorizers(ChangeSeq(42))),
            },
            "loon-core-test",
        )
        .expect("prepare checkpoint head publish");

        assert_eq!(prepared.object_key, namespace_head("ns-1"));
        assert_eq!(prepared.resulting_head.seq, ChangeSeq(42));
        assert_eq!(prepared.resulting_head.active_fence_token, FenceToken(9));
        assert_eq!(prepared.resulting_head.next_inode_id, InodeId(777));
        assert_eq!(
            prepared.resulting_head.snapshot_hint_seq,
            Some(ChangeSeq(42))
        );
        assert_eq!(prepared.resulting_head.retention_floor_seq, ChangeSeq(42));
        assert!(prepared
            .checked_invariants
            .contains(&"retention_floor_seq_requires_derived_progress".to_owned()));
    }

    #[test]
    fn prepare_checkpoint_head_publish_rejects_missing_authorizers() {
        let error = prepare_checkpoint_head_publish(
            &sample_head(),
            &loaded_checkpoint_for_publish(),
            &CheckpointHeadPublishRequest {
                requested_retention_floor_seq: Some(ChangeSeq(42)),
                retention_authorizers: None,
            },
            "loon-core-test",
        )
        .expect_err("missing authorizers should fail");

        assert_eq!(
            error,
            CheckpointPublishError::MissingRetentionAuthorizers {
                requested: ChangeSeq(42),
            }
        );
    }

    #[test]
    fn prepare_checkpoint_head_publish_rejects_lagging_required_progress() {
        let error = prepare_checkpoint_head_publish(
            &sample_head(),
            &loaded_checkpoint_for_publish(),
            &CheckpointHeadPublishRequest {
                requested_retention_floor_seq: Some(ChangeSeq(42)),
                retention_authorizers: Some(sample_retention_authorizers(ChangeSeq(41))),
            },
            "loon-core-test",
        )
        .expect_err("lagging required progress should fail");

        assert_eq!(
            error,
            CheckpointPublishError::RequiredProgressLag {
                work_class: "BuildListingIndex".to_owned(),
                requested: ChangeSeq(42),
                available: ChangeSeq(41),
            }
        );
    }

    #[test]
    fn prepare_checkpoint_head_publish_rejects_noop_publish() {
        let current_head = HeadState {
            snapshot_hint_seq: Some(ChangeSeq(42)),
            retention_floor_seq: ChangeSeq(42),
            ..sample_head()
        };
        let error = prepare_checkpoint_head_publish(
            &current_head,
            &loaded_checkpoint_for_publish(),
            &CheckpointHeadPublishRequest {
                requested_retention_floor_seq: None,
                retention_authorizers: None,
            },
            "loon-core-test",
        )
        .expect_err("noop publish should fail");

        assert_eq!(error, CheckpointPublishError::NoHeadChangeRequired);
    }

    #[test]
    fn publish_checkpoint_head_compare_and_swap_writes_new_head() {
        let temp_dir = TestDir::new("checkpoint-head-publish");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        let initial_head = sample_head();
        let initial_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "seed-head",
            initial_head,
        )
        .expect("build initial head envelope");
        let head_key = namespace_head("ns-1");
        let initial_bytes =
            serde_json::to_vec(&initial_envelope).expect("encode initial head envelope");

        store
            .put_if_absent(&head_key, &initial_bytes)
            .expect("seed initial head");
        seed_progress_object(&store, "ns-1", "BuildListingIndex", ChangeSeq(42));
        seed_progress_object(&store, "ns-1", "RetentionPolicy", ChangeSeq(42));
        let etag = store
            .head(&head_key)
            .expect("head read")
            .expect("head should exist")
            .etag
            .expect("head etag should exist");
        let retention_authorizers = load_retention_authorizers(
            &store,
            &NamespaceId::from("ns-1"),
            &["BuildListingIndex".to_owned()],
            "RetentionPolicy",
        )
        .expect("load retention authorizers");

        let prepared = prepare_checkpoint_head_publish(
            &sample_head(),
            &loaded_checkpoint_for_publish(),
            &CheckpointHeadPublishRequest {
                requested_retention_floor_seq: Some(ChangeSeq(42)),
                retention_authorizers: Some(retention_authorizers),
            },
            "loon-core-test",
        )
        .expect("prepare checkpoint head publish");

        publish_checkpoint_head(&store, &etag, &prepared).expect("head CAS should succeed");

        let stored_bytes = store
            .get(&head_key, None)
            .expect("read published head")
            .expect("published head should exist");
        let stored: HeadStateEnvelope =
            serde_json::from_slice(&stored_bytes).expect("decode published head");

        assert_eq!(stored.state.snapshot_hint_seq, Some(ChangeSeq(42)));
        assert_eq!(stored.state.retention_floor_seq, ChangeSeq(42));
        assert!(stored
            .has_valid_payload_checksum()
            .expect("recompute head payload checksum"));
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
        let prepared = prepared_checkpoint_for_replay();
        let wal_tail = vec![stored_wal_object(sample_wal_payload(
            ChangeSeq(41),
            ChangeSeq(40),
            "req-20260311-0001",
            FenceToken(9),
        ))];

        let final_head = replay_from_checkpoint_and_wal_tail(
            &NamespaceId::from("ns-1"),
            &StoredCheckpointManifest::from(&prepared.manifest),
            &stored_checkpoint_segments(&prepared),
            &wal_tail,
        )
        .expect("checkpoint plus wal tail should replay");

        assert_eq!(final_head.seq, ChangeSeq(41));
        assert_eq!(final_head.active_fence_token, FenceToken(9));
        assert_eq!(final_head.next_inode_id, InodeId(501));
        assert_eq!(final_head.snapshot_hint_seq, Some(ChangeSeq(40)));
    }

    #[test]
    fn load_checkpoint_rejects_missing_segment() {
        let prepared = prepared_checkpoint_for_replay();
        let mut segments = stored_checkpoint_segments(&prepared);
        let missing = segments
            .pop()
            .expect("prepared checkpoint should have at least one segment");

        let error = load_checkpoint(
            &NamespaceId::from("ns-1"),
            &StoredCheckpointManifest::from(&prepared.manifest),
            &segments,
        )
        .expect_err("missing segment should fail");

        assert_eq!(
            error,
            CheckpointReplayError::MissingSegment {
                object_key: missing.object_key,
            }
        );
    }

    #[test]
    fn load_checkpoint_rejects_unexpected_segment() {
        let prepared = prepared_checkpoint_for_replay();
        let mut segments = stored_checkpoint_segments(&prepared);
        let mut unexpected = segments[0].clone();
        unexpected.object_key =
            "namespaces/ns-1/snapshots/00000000000000000040/tables/extra-00000.sst.zst".to_owned();
        segments.push(unexpected.clone());

        let error = load_checkpoint(
            &NamespaceId::from("ns-1"),
            &StoredCheckpointManifest::from(&prepared.manifest),
            &segments,
        )
        .expect_err("unexpected segment should fail");

        assert_eq!(
            error,
            CheckpointReplayError::UnexpectedSegment {
                object_key: unexpected.object_key,
            }
        );
    }

    #[test]
    fn load_checkpoint_rejects_descriptor_mismatch() {
        let prepared = prepared_checkpoint_for_replay();
        let mut manifest_payload = prepared.manifest.envelope.payload.clone();
        manifest_payload.tables[0].segments[0].row_count = 1;
        let manifest_envelope =
            CheckpointManifestEnvelope::from_payload("loon-core-test", manifest_payload)
                .expect("rebuild mismatched checkpoint manifest");
        let manifest = StoredCheckpointManifest {
            object_key: prepared.manifest.object_key.clone(),
            encoded_bytes: encode_checkpoint_manifest_json(&manifest_envelope)
                .expect("encode mismatched checkpoint manifest"),
        };

        let error = load_checkpoint(
            &NamespaceId::from("ns-1"),
            &manifest,
            &stored_checkpoint_segments(&prepared),
        )
        .expect_err("descriptor mismatch should fail");

        match error {
            CheckpointReplayError::SegmentDescriptorMismatch {
                object_key,
                expected,
                actual,
            } => {
                assert!(object_key.contains("/tables/"));
                assert_eq!(expected.row_count, 1);
                assert_eq!(actual.row_count, 0);
            }
            other => panic!("unexpected error: {other:?}"),
        }
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

    fn stored_checkpoint_segments(
        prepared: &super::PreparedCheckpoint,
    ) -> Vec<StoredCheckpointSegment> {
        prepared
            .segments
            .iter()
            .map(StoredCheckpointSegment::from)
            .collect()
    }

    fn prepared_checkpoint_for_replay() -> super::PreparedCheckpoint {
        prepare_checkpoint(&sample_checkpoint_head(), "loon-core-test")
            .expect("prepare checkpoint for replay tests")
    }

    fn loaded_checkpoint_for_publish() -> LoadedCheckpoint {
        let prepared = prepare_checkpoint(&sample_head(), "loon-core-test")
            .expect("prepare checkpoint for publish tests");
        load_checkpoint(
            &NamespaceId::from("ns-1"),
            &StoredCheckpointManifest::from(&prepared.manifest),
            &stored_checkpoint_segments(&prepared),
        )
        .expect("load checkpoint for publish tests")
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

    fn sample_checkpoint_head() -> HeadState {
        HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(40),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        }
    }

    fn sample_retention_authorizers(through_seq: ChangeSeq) -> LoadedRetentionAuthorizers {
        let required_progress = loaded_progress_object("BuildListingIndex", through_seq);
        let retention_policy = loaded_progress_object("RetentionPolicy", through_seq);

        LoadedRetentionAuthorizers {
            required_progress: vec![required_progress.clone()],
            retention_policy: retention_policy.clone(),
            checked_invariants: vec![
                "progress_object_checksum_matches_payload".to_owned(),
                "progress_object_key_matches_namespace_and_work_class".to_owned(),
            ],
        }
    }

    fn loaded_progress_object(work_class: &str, through_seq: ChangeSeq) -> LoadedProgressObject {
        let envelope = ProgressStateEnvelope::from_state(
            ControlObjectKind::NamespaceProgress,
            "loon-core-test",
            ProgressState {
                namespace_id: NamespaceId::from("ns-1"),
                work_class: work_class.to_owned(),
                through_seq,
            },
        )
        .expect("build progress envelope");

        LoadedProgressObject {
            object_key: derived_progress("ns-1", work_class),
            envelope,
            checked_invariants: vec![
                "progress_object_checksum_matches_payload".to_owned(),
                "progress_object_key_matches_namespace_and_work_class".to_owned(),
            ],
        }
    }

    fn seed_progress_object(
        store: &LocalFsStore,
        namespace_id: &str,
        work_class: &str,
        through_seq: ChangeSeq,
    ) {
        let envelope = ProgressStateEnvelope::from_state(
            ControlObjectKind::NamespaceProgress,
            "loon-core-test",
            ProgressState {
                namespace_id: NamespaceId::from(namespace_id),
                work_class: work_class.to_owned(),
                through_seq,
            },
        )
        .expect("build progress envelope");

        store
            .put_if_absent(
                &derived_progress(namespace_id, work_class),
                &serde_json::to_vec(&envelope).expect("encode progress envelope"),
            )
            .expect("seed progress object");
    }

    #[derive(Debug)]
    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "loondb-core-{label}-{}-{stamp}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
