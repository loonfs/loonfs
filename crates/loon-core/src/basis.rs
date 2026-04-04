use crate::genesis::bootstrap_basis_metadata_state;
use crate::loading::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::metadata::MetadataState;
use crate::wal::{replay_wal_tail_with_metadata, StoredWalObject, WalReplayError};
use loon_api::{HeadState, NamespaceId};
use loon_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedNamespaceBasis {
    pub head: HeadState,
    pub head_etag: String,
    pub lease: loon_api::LeaseState,
    pub metadata_state: MetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum BasisLoadError {
    #[error(transparent)]
    LoadHead(#[from] ControlObjectLoadError),
    #[error("failed to load lease object: {0}")]
    LoadLease(ControlObjectLoadError),
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("failed to list WAL objects under `{prefix}`: {message}")]
    ListWal { prefix: String, message: String },
    #[error("invalid WAL object key `{object_key}`")]
    InvalidWalObjectKey { object_key: String },
    #[error("duplicate WAL seq `{seq:?}` with keys `{first}` and `{second}`")]
    DuplicateWalSeq {
        seq: loon_api::ChangeSeq,
        first: String,
        second: String,
    },
    #[error("missing WAL object for seq `{seq:?}` under `{prefix}`")]
    MissingWalObject {
        prefix: String,
        seq: loon_api::ChangeSeq,
    },
    #[error("failed to read WAL object `{object_key}`: {message}")]
    ReadWal { object_key: String, message: String },
    #[error("missing WAL object after list `{object_key}`")]
    MissingWalObjectAfterList { object_key: String },
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

pub fn load_verified_namespace_basis<S: ObjectStore + ?Sized>(
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
    ensure_reconstructed_head_matches(&loaded_head.envelope.state, &replayed.resulting_head)?;

    Ok(VerifiedNamespaceBasis {
        head: loaded_head.envelope.state,
        head_etag,
        lease: loaded_lease.envelope.state,
        metadata_state: replayed.resulting_metadata_state,
    })
}

fn ensure_reconstructed_head_matches(
    current_head: &HeadState,
    reconstructed: &HeadState,
) -> Result<(), BasisLoadError> {
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.next_inode_id != reconstructed.next_inode_id
        || current_head.snapshot_hint_seq != reconstructed.snapshot_hint_seq
        || current_head.retention_floor_seq != reconstructed.retention_floor_seq
    {
        return Err(BasisLoadError::ReconstructedHeadMismatch {
            expected: Box::new(current_head.clone()),
            actual: Box::new(reconstructed.clone()),
        });
    }
    Ok(())
}

fn load_stored_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
    from_seq_exclusive: loon_api::ChangeSeq,
    through_seq_inclusive: loon_api::ChangeSeq,
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
    let mut expected = from_seq_exclusive.0.saturating_add(1);
    while expected <= through_seq_inclusive.0 {
        let seq = loon_api::ChangeSeq(expected);
        let object_key =
            wal_by_seq
                .remove(&seq)
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
        expected = expected.saturating_add(1);
    }

    Ok(wal_tail)
}

fn wal_seq_from_key(prefix: &str, object_key: &str) -> Option<loon_api::ChangeSeq> {
    let suffix = object_key.strip_prefix(prefix)?;
    let (seq_part, _) = suffix.split_once('-')?;
    let seq = seq_part.parse::<u64>().ok()?;
    Some(loon_api::ChangeSeq(seq))
}
