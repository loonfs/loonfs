use crate::checkpoint::{
    checkpoint_basis_head, load_verified_checkpoint_materialization, CheckpointLoadError,
};
use crate::error::CoreError;
use crate::genesis::bootstrap_basis_metadata_state;
use crate::loading::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::metadata::MetadataState;
use crate::namespace::catalog::{load_namespace_catalog_entry, NamespaceCatalogLoadError};
use crate::wal::{replay_wal_tail_with_metadata, StoredWalObject, WalReplayError};
use loon_api::{
    decode_wal_segment_envelope_zstd, ChangeSeq, ContentStoreId, HeadState,
    NamespaceDescriptorState, NamespaceId, WalSegmentPointer,
};
use loon_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedNamespaceBasis {
    pub namespace_descriptor: NamespaceDescriptorState,
    pub content_store_id: ContentStoreId,
    pub head: HeadState,
    pub head_etag: String,
    pub lease: loon_api::LeaseState,
    pub metadata_state: MetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadSummary {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub checkpoint_hint_seq: Option<ChangeSeq>,
    pub retention_floor_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum BasisLoadError {
    #[error("failed to load namespace descriptor: {0}")]
    LoadNamespaceDescriptor(ControlObjectLoadError),
    #[error("failed to load content store descriptor: {0}")]
    LoadContentStoreDescriptor(ControlObjectLoadError),
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
    #[error(transparent)]
    CheckpointLoad(#[from] CheckpointLoadError),
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

impl From<NamespaceCatalogLoadError> for BasisLoadError {
    fn from(value: NamespaceCatalogLoadError) -> Self {
        match value {
            NamespaceCatalogLoadError::LoadNamespaceDescriptor(error) => {
                Self::LoadNamespaceDescriptor(error)
            }
            NamespaceCatalogLoadError::LoadContentStoreDescriptor(error) => {
                Self::LoadContentStoreDescriptor(error)
            }
        }
    }
}

pub fn load_verified_namespace_basis<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<VerifiedNamespaceBasis, BasisLoadError> {
    let catalog_entry = load_namespace_catalog_entry(store, expected_namespace)?;

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

    let (initial_head, initial_metadata_state) = if let Some(checkpoint_seq) =
        loaded_head.envelope.state.checkpoint_hint_seq
    {
        let materialized =
            load_verified_checkpoint_materialization(store, expected_namespace, checkpoint_seq)?;
        (
            checkpoint_basis_head(&loaded_head.envelope.state, &materialized.manifest),
            materialized.metadata_state,
        )
    } else {
        (
            HeadState::initial(expected_namespace.clone()),
            bootstrap_basis_metadata_state(),
        )
    };
    let wal_tail = load_stored_wal_tail(
        store,
        expected_namespace,
        initial_head.seq,
        loaded_head.envelope.state.seq,
        loaded_head.envelope.state.visible_wal_tip.clone(),
    )?;
    let replayed = replay_wal_tail_with_metadata(&initial_head, &initial_metadata_state, &wal_tail)
        .map_err(BasisLoadError::WalReplay)?;
    ensure_reconstructed_head_matches(&loaded_head.envelope.state, &replayed.resulting_head)?;

    Ok(VerifiedNamespaceBasis {
        namespace_descriptor: catalog_entry.namespace_descriptor,
        content_store_id: catalog_entry.content_store_id,
        head: loaded_head.envelope.state,
        head_etag,
        lease: loaded_lease.envelope.state,
        metadata_state: replayed.resulting_metadata_state,
    })
}

pub fn load_namespace_head_summary<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<NamespaceHeadSummary, CoreError> {
    let loaded_head = read_head_object(store, expected_namespace)
        .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
    let head = loaded_head.envelope.state;
    Ok(NamespaceHeadSummary {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        checkpoint_hint_seq: head.checkpoint_hint_seq,
        retention_floor_seq: head.retention_floor_seq,
    })
}

fn ensure_reconstructed_head_matches(
    current_head: &HeadState,
    reconstructed: &HeadState,
) -> Result<(), BasisLoadError> {
    // `active_fence_token` is intentionally excluded. Lease takeover can bump
    // the fence token in the control plane without any WAL replay.
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.next_inode_id != reconstructed.next_inode_id
        || current_head.name_policy != reconstructed.name_policy
        || current_head.checkpoint_hint_seq != reconstructed.checkpoint_hint_seq
        || current_head.retention_floor_seq != reconstructed.retention_floor_seq
        || (reconstructed.visible_wal_tip.is_some()
            && current_head.visible_wal_tip != reconstructed.visible_wal_tip)
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
    from_seq_exclusive: ChangeSeq,
    through_seq_inclusive: ChangeSeq,
    visible_wal_tip: Option<WalSegmentPointer>,
) -> Result<Vec<StoredWalObject>, BasisLoadError> {
    if through_seq_inclusive <= from_seq_exclusive {
        return Ok(Vec::new());
    }

    let prefix = format!("namespaces/{}/wal/", expected_namespace.as_str());
    let mut pointer = visible_wal_tip.ok_or_else(|| BasisLoadError::MissingWalObject {
        prefix: prefix.clone(),
        seq: through_seq_inclusive,
    })?;
    let mut reversed = Vec::new();

    loop {
        if pointer.end_seq <= from_seq_exclusive {
            break;
        }
        let encoded_bytes = store
            .get(&pointer.object_key, None)
            .map_err(|err| BasisLoadError::ReadWal {
                object_key: pointer.object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| BasisLoadError::MissingWalObjectAfterList {
                object_key: pointer.object_key.clone(),
            })?;
        let envelope = decode_wal_segment_envelope_zstd(&encoded_bytes)
            .map_err(|err| BasisLoadError::WalReplay(WalReplayError::Codec(err.to_string())))?;
        if envelope.payload.namespace_id != *expected_namespace {
            return Err(BasisLoadError::WalReplay(
                WalReplayError::NamespaceMismatch {
                    expected: expected_namespace.clone(),
                    actual: envelope.payload.namespace_id.clone(),
                },
            ));
        }
        if envelope.payload.segment_id != pointer.segment_id
            || envelope.payload.start_seq != pointer.start_seq
            || envelope.payload.end_seq != pointer.end_seq
            || envelope.payload_checksum_sha256 != pointer.payload_checksum_sha256
        {
            return Err(BasisLoadError::WalReplay(
                WalReplayError::SegmentSummaryMismatch,
            ));
        }
        let prev = envelope.payload.prev_visible_segment.clone();
        reversed.push(StoredWalObject {
            object_key: pointer.object_key.clone(),
            encoded_bytes,
        });
        let Some(prev) = prev else {
            break;
        };
        pointer = prev;
    }

    reversed.reverse();
    Ok(reversed)
}
