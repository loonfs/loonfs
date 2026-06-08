use super::bootstrap::BootstrapNamespaceError;
use crate::checkpoint::{
    create_checkpoint, load_verified_checkpoint_materialization,
    write_verified_checkpoint_from_metadata, CheckpointMetadataWriteRequest,
};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::namespace::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::namespace::catalog::{
    namespace_initialization_state, NamespaceInitializationError, NamespaceInitializationState,
};
use loon_api::wire::control::{
    ControlObjectKind, HeadState, HeadStateEnvelope, LeaseState, LeaseStateEnvelope,
    NamespaceDescriptorEnvelope, NamespaceDescriptorState, NamespaceForkState,
    NamespaceForkStateEnvelope,
};
use loon_api::{FenceToken, NamespaceId, NamespaceSummary};
use loon_objectstore::keys::{
    namespace_descriptor, namespace_fork_state, namespace_head, namespace_lease,
};
use loon_objectstore::{ObjectStore, ObjectStoreError};

pub(crate) fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<NamespaceSummary, CoreError> {
    match namespace_initialization_state(store, new_namespace_id)
        .map_err(map_namespace_initialization_error_to_core)?
    {
        NamespaceInitializationState::Absent => {}
        NamespaceInitializationState::Complete => {
            return Err(CoreError::NamespaceAlreadyExists {
                namespace_id: new_namespace_id.clone(),
            });
        }
        NamespaceInitializationState::Partial => {
            return Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: new_namespace_id.clone(),
            });
        }
    }

    let checkpoint = create_checkpoint(store, source_namespace_id, context)?;
    let fork_seq = checkpoint.checkpoint_seq;
    let source_basis = load_verified_namespace_basis(store, source_namespace_id)?;
    let source_checkpoint =
        load_verified_checkpoint_materialization(store, source_namespace_id, fork_seq)
            .map_err(|err| CoreError::Basis(BasisLoadError::CheckpointLoad(err)))?;

    let initial_head = HeadState {
        namespace_id: new_namespace_id.clone(),
        seq: fork_seq,
        active_fence_token: FenceToken(0),
        next_inode_id: source_checkpoint.manifest.payload.next_inode_id,
        name_policy: source_basis.head.name_policy,
        checkpoint_hint_seq: Some(fork_seq),
        retention_floor_seq: fork_seq,
        visible_wal_tip: None,
    };
    let initial_lease = LeaseState {
        namespace_id: new_namespace_id.clone(),
        holder_id: context.writer_id.clone(),
        fence_token: initial_head.active_fence_token,
        lease_expires_at_ms: context.now_ms.saturating_add(context.lease_duration_ms),
    };
    let namespace_descriptor_envelope = NamespaceDescriptorEnvelope::from_state(
        ControlObjectKind::NamespaceDescriptor,
        &context.writer_version,
        NamespaceDescriptorState {
            namespace_id: new_namespace_id.clone(),
            content_store_id: source_basis.content_store_id,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let head = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        &context.writer_version,
        initial_head,
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let lease = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        &context.writer_version,
        initial_lease,
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let fork_state = NamespaceForkStateEnvelope::from_state(
        ControlObjectKind::NamespaceForkState,
        &context.writer_version,
        NamespaceForkState {
            namespace_id: new_namespace_id.clone(),
            source_namespace_id: source_namespace_id.clone(),
            fork_seq,
            source_checkpoint_seq: fork_seq,
            source_head_seq: fork_seq,
            created_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;

    let head_key = namespace_head(new_namespace_id.as_str());
    let fork_state_key = namespace_fork_state(new_namespace_id.as_str());
    let lease_key = namespace_lease(new_namespace_id.as_str());
    let descriptor_key = namespace_descriptor(new_namespace_id.as_str());
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &head_key,
        &serde_json::to_vec(&head).map_err(|err| CoreError::Store(err.to_string()))?,
    )?;
    write_verified_checkpoint_from_metadata(
        store,
        CheckpointMetadataWriteRequest {
            namespace_id: new_namespace_id,
            checkpoint_seq: fork_seq,
            active_fence_token: FenceToken(0),
            next_inode_id: source_checkpoint.manifest.payload.next_inode_id,
            retention_floor_seq: fork_seq,
            metadata_state: &source_checkpoint.metadata_state,
            writer_version: &context.writer_version,
        },
    )?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &fork_state_key,
        &serde_json::to_vec(&fork_state).map_err(|err| CoreError::Store(err.to_string()))?,
    )?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &lease_key,
        &serde_json::to_vec(&lease).map_err(|err| CoreError::Store(err.to_string()))?,
    )?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &descriptor_key,
        &serde_json::to_vec(&namespace_descriptor_envelope)
            .map_err(|err| CoreError::Store(err.to_string()))?,
    )?;

    Ok(NamespaceSummary {
        namespace_id: new_namespace_id.clone(),
    })
}

fn put_target_namespace_control_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), CoreError> {
    match store.put_if_absent(object_key, bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
            match namespace_initialization_state(store, namespace_id)
                .map_err(map_namespace_initialization_error_to_core)?
            {
                NamespaceInitializationState::Complete => Err(CoreError::NamespaceAlreadyExists {
                    namespace_id: namespace_id.clone(),
                }),
                NamespaceInitializationState::Partial => {
                    Err(CoreError::NamespacePartiallyInitialized {
                        namespace_id: namespace_id.clone(),
                    })
                }
                NamespaceInitializationState::Absent => Err(CoreError::Store(format!(
                    "target namespace control object `{object_key}` write failed, but namespace remains absent"
                ))),
            }
        }
        Err(err) => Err(CoreError::Store(err.to_string())),
    }
}

fn map_namespace_initialization_error_to_core(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        other => CoreError::Store(BootstrapNamespaceError::from(other).to_string()),
    }
}
