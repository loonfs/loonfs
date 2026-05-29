use crate::checkpoint::{
    checkpoint_basis_head, load_verified_checkpoint_materialization, CheckpointLoadError,
};
use crate::error::CoreError;
use crate::genesis::bootstrap_basis_metadata_state;
use crate::loading::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::metadata::MetadataState;
use crate::namespace::catalog::{
    load_namespace_catalog_entry, namespace_initialization_state, NamespaceCatalogLoadError,
    NamespaceInitializationError, NamespaceInitializationState,
};
use crate::wal::{
    load_validated_wal_chain, replay_validated_wal_tail_with_metadata, WalChainLoadError,
    WalChainLoadRequest, WalReplayError,
};
use loon_api::{ChangeSeq, ContentStoreId, HeadState, NamespaceDescriptorState, NamespaceId};
use loon_objectstore::{
    keys::{namespace_descriptor, namespace_head},
    ObjectStore,
};
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
    pub wal_tail_segments: u64,
    pub retention_floor_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceHeadEtagProbe {
    pub head_etag: String,
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
    #[error(transparent)]
    WalChainLoad(#[from] WalChainLoadError),
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
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id: expected_namespace,
            chain_base_seq: initial_head.seq,
            head_seq: loaded_head.envelope.state.seq,
            visible_tip: loaded_head.envelope.state.visible_wal_tip.clone(),
            stop_after_seq: None,
        },
    )?;
    let replayed = replay_validated_wal_tail_with_metadata(
        &initial_head,
        &initial_metadata_state,
        wal_chain.segments(),
    )
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
    match namespace_initialization_state(store, expected_namespace) {
        Ok(NamespaceInitializationState::Complete) => {}
        Ok(NamespaceInitializationState::Absent) => {
            return Err(CoreError::Basis(BasisLoadError::LoadNamespaceDescriptor(
                ControlObjectLoadError::MissingObject {
                    object_key: namespace_descriptor(expected_namespace.as_str()),
                },
            )));
        }
        Ok(NamespaceInitializationState::Partial) => {
            return Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: expected_namespace.clone(),
            });
        }
        Err(error) => return Err(map_namespace_initialization_error_to_core(error)),
    }

    let loaded_head = read_head_object(store, expected_namespace)
        .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
    let head = loaded_head.envelope.state;
    let checkpoint_basis_seq = head.checkpoint_hint_seq.unwrap_or(ChangeSeq(0));
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id: expected_namespace,
            chain_base_seq: checkpoint_basis_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
        },
    )
    .map_err(|error| CoreError::Basis(BasisLoadError::WalChainLoad(error)))?;
    let wal_tail_segments = u64::try_from(wal_chain.segments().len())
        .map_err(|_| CoreError::Store("WAL tail segment count overflow".to_owned()))?;
    Ok(NamespaceHeadSummary {
        namespace_id: head.namespace_id,
        head_seq: head.seq,
        checkpoint_hint_seq: head.checkpoint_hint_seq,
        wal_tail_segments,
        retention_floor_seq: head.retention_floor_seq,
    })
}

pub fn probe_namespace_head_etag<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<NamespaceHeadEtagProbe, CoreError> {
    NamespaceId::parse(expected_namespace.as_str())?;
    let object_key = namespace_head(expected_namespace.as_str());
    let metadata = store
        .head(&object_key)
        .map_err(|err| {
            CoreError::Basis(BasisLoadError::LoadHead(ControlObjectLoadError::Store(
                err.to_string(),
            )))
        })?
        .ok_or_else(|| {
            CoreError::Basis(BasisLoadError::LoadHead(
                ControlObjectLoadError::MissingObject {
                    object_key: object_key.clone(),
                },
            ))
        })?;
    let head_etag = metadata
        .etag
        .ok_or(CoreError::Basis(BasisLoadError::MissingHeadEtag {
            object_key,
        }))?;
    Ok(NamespaceHeadEtagProbe { head_etag })
}

fn map_namespace_initialization_error_to_core(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        NamespaceInitializationError::LoadNamespaceDescriptor(error) => {
            CoreError::Basis(BasisLoadError::LoadNamespaceDescriptor(error))
        }
        NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
            CoreError::Basis(BasisLoadError::LoadContentStoreDescriptor(error))
        }
        NamespaceInitializationError::InspectNamespaceDescriptor(_)
        | NamespaceInitializationError::InspectNamespaceHead(_)
        | NamespaceInitializationError::InspectNamespaceLease(_) => {
            CoreError::Store(error.to_string())
        }
    }
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
