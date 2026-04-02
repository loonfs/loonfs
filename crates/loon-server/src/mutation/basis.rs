use crate::core::checkpoint::{
    replay_from_checkpoint_and_wal_tail_with_metadata, CheckpointReplayError,
    StoredCheckpointManifest, StoredCheckpointSegment,
};
use crate::core::metadata::MetadataState;
use crate::core::wal::{replay_wal_tail_with_metadata, StoredWalObject, WalReplayError};
use crate::genesis::bootstrap_basis_metadata_state;
use crate::mutation::loading::{
    read_head_object, read_lease_object, ControlObjectLoadError, LoadedHeadObject,
};
use crate::objectstore::keys::snapshot_manifest;
use crate::objectstore::ObjectStore;
use crate::core::control_types::HeadStateEnvelope;
use loon_types::{HeadState, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VerifiedNamespaceBasis {
    pub(crate) head: HeadState,
    pub(crate) head_etag: String,
    pub(crate) lease: loon_types::LeaseState,
    pub(crate) metadata_state: MetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum BasisLoadError {
    #[error(transparent)]
    LoadHead(#[from] ControlObjectLoadError),
    #[error("failed to load lease object: {0}")]
    LoadLease(ControlObjectLoadError),
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("missing checkpoint manifest `{object_key}`")]
    MissingCheckpointManifest { object_key: String },
    #[error("failed to list checkpoint segments under `{prefix}`: {message}")]
    ListCheckpointSegments { prefix: String, message: String },
    #[error("failed to read checkpoint segment `{object_key}`: {message}")]
    ReadCheckpointSegment { object_key: String, message: String },
    #[error("missing checkpoint segment after list `{object_key}`")]
    MissingCheckpointSegmentAfterList { object_key: String },
    #[error("failed to list WAL objects under `{prefix}`: {message}")]
    ListWal { prefix: String, message: String },
    #[error("invalid WAL object key `{object_key}`")]
    InvalidWalObjectKey { object_key: String },
    #[error("duplicate WAL seq `{seq:?}` with keys `{first}` and `{second}`")]
    DuplicateWalSeq {
        seq: loon_types::ChangeSeq,
        first: String,
        second: String,
    },
    #[error("missing WAL object for seq `{seq:?}` under `{prefix}`")]
    MissingWalObject {
        prefix: String,
        seq: loon_types::ChangeSeq,
    },
    #[error("failed to read WAL object `{object_key}`: {message}")]
    ReadWal { object_key: String, message: String },
    #[error("missing WAL object after list `{object_key}`")]
    MissingWalObjectAfterList { object_key: String },
    #[error("checkpoint replay failed: {0:?}")]
    CheckpointReplay(Box<CheckpointReplayError>),
    #[error("wal replay failed: {0:?}")]
    WalReplay(WalReplayError),
    #[error(
        "verified basis mismatch: expected current head `{expected:?}`, reconstructed `{actual:?}`"
    )]
    ReconstructedHeadMismatch {
        expected: Box<HeadState>,
        actual: Box<HeadState>,
    },
}

pub(crate) fn load_verified_namespace_basis<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<VerifiedNamespaceBasis, BasisLoadError> {
    let loaded_head = read_head_object(store, expected_namespace)?;
    let loaded_lease =
        read_lease_object(store, expected_namespace).map_err(BasisLoadError::LoadLease)?;
    let head_etag =
        loaded_head
            .metadata
            .etag
            .clone()
            .ok_or_else(|| BasisLoadError::MissingHeadEtag {
                object_key: loaded_head.object_key.clone(),
            })?;

    let metadata_state = match loaded_head.envelope.state.snapshot_hint_seq {
        Some(snapshot_seq) => load_metadata_from_checkpoint_basis(
            store,
            &loaded_head,
            expected_namespace,
            snapshot_seq,
        )?,
        None => load_metadata_from_genesis_basis(store, &loaded_head, expected_namespace)?,
    };

    Ok(VerifiedNamespaceBasis {
        head: loaded_head.envelope.state,
        head_etag,
        lease: loaded_lease.envelope.state,
        metadata_state,
    })
}

fn load_metadata_from_checkpoint_basis<S: ObjectStore>(
    store: &S,
    loaded_head: &LoadedHeadObject,
    expected_namespace: &NamespaceId,
    snapshot_seq: loon_types::ChangeSeq,
) -> Result<MetadataState, BasisLoadError> {
    let manifest = load_stored_checkpoint_manifest(store, expected_namespace, snapshot_seq)?;
    let segments = load_stored_checkpoint_segments(store, expected_namespace, snapshot_seq)?;
    let wal_tail = load_stored_wal_tail(
        store,
        expected_namespace,
        snapshot_seq,
        loaded_head.envelope.state.seq,
    )?;
    let replayed = replay_from_checkpoint_and_wal_tail_with_metadata(
        expected_namespace,
        &manifest,
        &segments,
        &wal_tail,
    )
    .map_err(|error| BasisLoadError::CheckpointReplay(Box::new(error)))?;
    ensure_reconstructed_head_matches(&loaded_head.envelope, &replayed.resulting_head)?;
    Ok(replayed.resulting_metadata_state)
}

fn load_metadata_from_genesis_basis<S: ObjectStore>(
    store: &S,
    loaded_head: &LoadedHeadObject,
    expected_namespace: &NamespaceId,
) -> Result<MetadataState, BasisLoadError> {
    let initial_head = HeadState::initial(expected_namespace.clone());
    let initial_metadata_state = bootstrap_basis_metadata_state();
    let wal_tail = load_stored_wal_tail(
        store,
        expected_namespace,
        initial_head.seq,
        loaded_head.envelope.state.seq,
    )?;
    let replayed = replay_wal_tail_with_metadata(&initial_head, &initial_metadata_state, &wal_tail)
        .map_err(BasisLoadError::WalReplay)?;
    ensure_reconstructed_head_matches(&loaded_head.envelope, &replayed.resulting_head)?;
    Ok(replayed.resulting_metadata_state)
}

fn ensure_reconstructed_head_matches(
    current_head: &HeadStateEnvelope,
    reconstructed: &HeadState,
) -> Result<(), BasisLoadError> {
    if current_head.state.namespace_id != reconstructed.namespace_id
        || current_head.state.seq != reconstructed.seq
        || current_head.state.next_inode_id != reconstructed.next_inode_id
        || current_head.state.snapshot_hint_seq != reconstructed.snapshot_hint_seq
        || current_head.state.retention_floor_seq != reconstructed.retention_floor_seq
    {
        return Err(BasisLoadError::ReconstructedHeadMismatch {
            expected: Box::new(current_head.state.clone()),
            actual: Box::new(reconstructed.clone()),
        });
    }
    Ok(())
}

fn load_stored_checkpoint_manifest<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
    snapshot_seq: loon_types::ChangeSeq,
) -> Result<StoredCheckpointManifest, BasisLoadError> {
    let object_key = snapshot_manifest(expected_namespace.as_str(), snapshot_seq.0);
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(|err| BasisLoadError::ListCheckpointSegments {
            prefix: object_key.clone(),
            message: err.to_string(),
        })?
        .ok_or_else(|| BasisLoadError::MissingCheckpointManifest {
            object_key: object_key.clone(),
        })?;
    Ok(StoredCheckpointManifest {
        object_key,
        encoded_bytes,
    })
}

fn load_stored_checkpoint_segments<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
    snapshot_seq: loon_types::ChangeSeq,
) -> Result<Vec<StoredCheckpointSegment>, BasisLoadError> {
    let prefix = format!(
        "namespaces/{}/snapshots/{:020}/tables/",
        expected_namespace.as_str(),
        snapshot_seq.0
    );
    let mut keys =
        store
            .list_prefix(&prefix)
            .map_err(|err| BasisLoadError::ListCheckpointSegments {
                prefix: prefix.clone(),
                message: err.to_string(),
            })?;
    keys.sort();

    let mut segments = Vec::with_capacity(keys.len());
    for object_key in keys {
        let encoded_bytes = store
            .get(&object_key, None)
            .map_err(|err| BasisLoadError::ReadCheckpointSegment {
                object_key: object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| BasisLoadError::MissingCheckpointSegmentAfterList {
                object_key: object_key.clone(),
            })?;
        segments.push(StoredCheckpointSegment {
            object_key,
            encoded_bytes,
        });
    }

    Ok(segments)
}

fn load_stored_wal_tail<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
    from_seq_exclusive: loon_types::ChangeSeq,
    through_seq_inclusive: loon_types::ChangeSeq,
) -> Result<Vec<StoredWalObject>, BasisLoadError> {
    let prefix = format!("namespaces/{}/wal/", expected_namespace.as_str());
    let listed = store
        .list_prefix(&prefix)
        .map_err(|err| BasisLoadError::ListWal {
            prefix: prefix.clone(),
            message: err.to_string(),
        })?;

    let mut wal_by_seq = std::collections::BTreeMap::new();
    for object_key in listed {
        let Some(seq) = wal_seq_from_key(&prefix, &object_key) else {
            return Err(BasisLoadError::InvalidWalObjectKey { object_key });
        };
        if seq <= from_seq_exclusive || seq > through_seq_inclusive {
            continue;
        }
        if let Some(existing) = wal_by_seq.insert(seq, object_key.clone()) {
            return Err(BasisLoadError::DuplicateWalSeq {
                seq,
                first: existing,
                second: object_key,
            });
        }
    }

    let mut wal_tail = Vec::new();
    let mut next_seq = from_seq_exclusive.0 + 1;
    while next_seq <= through_seq_inclusive.0 {
        let seq = loon_types::ChangeSeq(next_seq);
        let object_key =
            wal_by_seq
                .get(&seq)
                .cloned()
                .ok_or_else(|| BasisLoadError::MissingWalObject {
                    prefix: prefix.clone(),
                    seq,
                })?;
        let encoded_bytes = store
            .get(&object_key, None)
            .map_err(|err| BasisLoadError::ReadWal {
                object_key: object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| BasisLoadError::MissingWalObjectAfterList {
                object_key: object_key.clone(),
            })?;
        wal_tail.push(StoredWalObject {
            object_key,
            encoded_bytes,
        });
        next_seq += 1;
    }

    Ok(wal_tail)
}

fn wal_seq_from_key(prefix: &str, object_key: &str) -> Option<loon_types::ChangeSeq> {
    let remainder = object_key.strip_prefix(prefix)?;
    let (seq_text, _) = remainder.split_once('-')?;
    let seq = seq_text.parse::<u64>().ok()?;
    Some(loon_types::ChangeSeq(seq))
}
