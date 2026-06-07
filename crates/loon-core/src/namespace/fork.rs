use super::bootstrap::BootstrapNamespaceError;
use crate::checkpoint::{
    create_checkpoint, load_verified_manifest_materialization, write_namespace_manifest,
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
    NamespaceForkStateEnvelope, NamespaceGcPinState, NamespaceGcPinStateEnvelope,
};
use loon_api::wire::manifest::{
    NamespaceCheckpointRecord, NamespaceManifestEnvelope, NamespaceManifestFork,
    NamespaceManifestPayload,
};
use loon_api::{
    generate_checkpoint_id, generate_gc_pin_id, FenceToken, ManifestId, NamespaceId,
    NamespaceSummary,
};
use loon_objectstore::keys::{
    gc_pin, namespace_descriptor, namespace_fork_state, namespace_head, namespace_lease,
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
    let source_manifest =
        load_verified_manifest_materialization(store, source_namespace_id, checkpoint.manifest_id)
            .map_err(|err| CoreError::Basis(BasisLoadError::ManifestLoad(err)))?;
    let target_manifest_id = ManifestId(fork_seq.0);
    let target_checkpoint_id = generate_checkpoint_id();

    let initial_head = HeadState {
        namespace_id: new_namespace_id.clone(),
        seq: fork_seq,
        head_commit_id: source_basis.head.head_commit_id.clone(),
        active_fence_token: FenceToken(0),
        next_inode_id: source_manifest.manifest.payload.next_inode_id,
        name_policy: source_basis.head.name_policy,
        current_manifest_id: Some(target_manifest_id),
        latest_checkpoint_id: Some(target_checkpoint_id.clone()),
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
            source_checkpoint_id: checkpoint.checkpoint_id.clone(),
            source_manifest_id: checkpoint.manifest_id,
            source_head_seq: source_basis.head.seq,
            created_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;

    let head_key = namespace_head(new_namespace_id.as_str());
    let fork_state_key = namespace_fork_state(new_namespace_id.as_str());
    let lease_key = namespace_lease(new_namespace_id.as_str());
    let descriptor_key = namespace_descriptor(new_namespace_id.as_str());
    let target_manifest = NamespaceManifestEnvelope::from_payload(
        &context.writer_version,
        NamespaceManifestPayload {
            namespace_id: new_namespace_id.clone(),
            manifest_id: target_manifest_id,
            head_seq: fork_seq,
            base_seq: source_manifest.manifest.payload.base_seq,
            active_fence_token: FenceToken(0),
            next_inode_id: source_manifest.manifest.payload.next_inode_id,
            retention_floor_seq: fork_seq,
            initialized: true,
            verified: true,
            fork: Some(NamespaceManifestFork {
                source_namespace_id: source_namespace_id.clone(),
                fork_seq,
                source_checkpoint_id: checkpoint.checkpoint_id.clone(),
                source_manifest_id: checkpoint.manifest_id,
                source_head_seq: source_basis.head.seq,
            }),
            checkpoints: vec![NamespaceCheckpointRecord {
                checkpoint_id: target_checkpoint_id.clone(),
                manifest_id: target_manifest_id,
                head_seq: fork_seq,
                head_commit_id: source_basis.head.head_commit_id.clone(),
                created_at_ms: context.now_ms,
                expires_at_ms: None,
                name: None,
            }],
            metadata_files: source_manifest.manifest.payload.metadata_files.clone(),
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let referenced_metadata_files_debug = target_manifest
        .payload
        .metadata_files
        .iter()
        .filter(|metadata_file| metadata_file.owner_namespace_id == *source_namespace_id)
        .map(|metadata_file| metadata_file.object_key.clone())
        .collect::<Vec<_>>();
    let gc_pin_envelope = NamespaceGcPinStateEnvelope::from_state(
        ControlObjectKind::NamespaceGcPinState,
        &context.writer_version,
        NamespaceGcPinState {
            pin_id: generate_gc_pin_id(),
            source_namespace_id: source_namespace_id.clone(),
            target_namespace_id: new_namespace_id.clone(),
            source_checkpoint_id: checkpoint.checkpoint_id,
            source_manifest_id: checkpoint.manifest_id,
            source_head_seq: source_basis.head.seq,
            referenced_metadata_files_debug,
            created_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let gc_pin_key = gc_pin(source_namespace_id.as_str(), &gc_pin_envelope.state.pin_id);
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &head_key,
        &serde_json::to_vec(&head).map_err(|err| CoreError::Store(err.to_string()))?,
    )?;
    write_namespace_manifest(store, &target_manifest).map_err(CoreError::Basis)?;
    write_source_gc_pin(
        store,
        &gc_pin_key,
        &serde_json::to_vec(&gc_pin_envelope).map_err(|err| CoreError::Store(err.to_string()))?,
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

fn write_source_gc_pin<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), CoreError> {
    match store.put_if_absent(object_key, bytes) {
        Ok(_) | Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => Ok(()),
        Err(err) => Err(CoreError::Store(err.to_string())),
    }
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
