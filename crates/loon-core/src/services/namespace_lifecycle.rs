use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::checkpoint::{
    create_checkpoint, load_verified_checkpoint_materialization,
    write_verified_checkpoint_from_metadata, CheckpointMetadataWriteRequest,
};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::genesis::bootstrap_basis_metadata_state;
use crate::loading::ControlObjectLoadError;
use crate::namespace::catalog::{
    load_namespace_descriptor, namespace_initialization_state, NamespaceInitializationError,
    NamespaceInitializationState,
};
use loon_api::{
    ContentStoreDescriptorEnvelope, ContentStoreDescriptorState, ContentStoreId, ControlObjectKind,
    FenceToken, HeadState, HeadStateEnvelope, LeaseState, LeaseStateEnvelope,
    NamespaceDescriptorEnvelope, NamespaceDescriptorState, NamespaceId, NamespaceIdValidationError,
    NamespaceSummary,
};
use loon_objectstore::keys::{
    content_store_descriptor, namespace_descriptor, namespace_head, namespace_lease,
};
use loon_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BootstrapNamespaceError {
    #[error(transparent)]
    InvalidNamespaceId(#[from] NamespaceIdValidationError),
    #[error("holder id must not be empty")]
    EmptyHolderId,
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is partially initialized")]
    NamespacePartiallyInitialized { namespace_id: NamespaceId },
    #[error(transparent)]
    Descriptor(ControlObjectLoadError),
    #[error("failed to write namespace descriptor object: {0}")]
    DescriptorWrite(String),
    #[error("failed to write content store descriptor object: {0}")]
    ContentStoreWrite(String),
    #[error(transparent)]
    Head(ControlObjectLoadError),
    #[error("failed to write head object: {0}")]
    HeadWrite(String),
    #[error("failed to write lease object: {0}")]
    LeaseWrite(String),
}

impl From<NamespaceInitializationError> for BootstrapNamespaceError {
    fn from(value: NamespaceInitializationError) -> Self {
        match value {
            NamespaceInitializationError::InvalidNamespaceId(error) => {
                Self::InvalidNamespaceId(error)
            }
            NamespaceInitializationError::InspectNamespaceDescriptor(message) => {
                Self::DescriptorWrite(message)
            }
            NamespaceInitializationError::InspectNamespaceHead(message) => Self::HeadWrite(message),
            NamespaceInitializationError::InspectNamespaceLease(message) => {
                Self::LeaseWrite(message)
            }
            NamespaceInitializationError::LoadNamespaceDescriptor(error)
            | NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
                Self::Descriptor(error)
            }
        }
    }
}

pub fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    allow_existing: bool,
) -> Result<NamespaceSummary, BootstrapNamespaceError> {
    if context.writer_id.trim().is_empty() {
        return Err(BootstrapNamespaceError::EmptyHolderId);
    }
    if context.writer_version.trim().is_empty() {
        return Err(BootstrapNamespaceError::EmptyWriterVersion);
    }

    match namespace_initialization_state(store, namespace_id)? {
        NamespaceInitializationState::Complete if allow_existing => {
            return Ok(NamespaceSummary {
                namespace_id: namespace_id.clone(),
            });
        }
        NamespaceInitializationState::Complete => {
            return Err(BootstrapNamespaceError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            });
        }
        NamespaceInitializationState::Partial => {
            return Err(BootstrapNamespaceError::NamespacePartiallyInitialized {
                namespace_id: namespace_id.clone(),
            });
        }
        NamespaceInitializationState::Absent => {}
    }

    let initial_head = HeadState::initial(namespace_id.clone());
    let initial_lease = LeaseState {
        namespace_id: namespace_id.clone(),
        holder_id: context.writer_id.clone(),
        fence_token: initial_head.active_fence_token,
        lease_expires_at_ms: context.now_ms.saturating_add(context.lease_duration_ms),
    };
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        &context.writer_version,
        initial_head,
    )
    .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?;
    let lease_envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        &context.writer_version,
        initial_lease,
    )
    .map_err(|err| BootstrapNamespaceError::LeaseWrite(err.to_string()))?;
    let head_bytes = serde_json::to_vec(&head_envelope)
        .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?;
    let lease_bytes = serde_json::to_vec(&lease_envelope)
        .map_err(|err| BootstrapNamespaceError::LeaseWrite(err.to_string()))?;

    let head_key = namespace_head(namespace_id.as_str());
    let lease_key = namespace_lease(namespace_id.as_str());
    store
        .put_if_absent(&head_key, &head_bytes)
        .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?;
    store
        .put_if_absent(&lease_key, &lease_bytes)
        .map_err(|err| BootstrapNamespaceError::LeaseWrite(err.to_string()))?;

    let content_store_id = create_new_content_store(store, context)?;
    let namespace_descriptor_envelope = NamespaceDescriptorEnvelope::from_state(
        ControlObjectKind::NamespaceDescriptor,
        &context.writer_version,
        NamespaceDescriptorState {
            namespace_id: namespace_id.clone(),
            content_store_id,
        },
    )
    .map_err(|err| BootstrapNamespaceError::DescriptorWrite(err.to_string()))?;
    let namespace_descriptor_bytes = serde_json::to_vec(&namespace_descriptor_envelope)
        .map_err(|err| BootstrapNamespaceError::DescriptorWrite(err.to_string()))?;

    let descriptor_key = namespace_descriptor(namespace_id.as_str());
    store
        .put_if_absent(&descriptor_key, &namespace_descriptor_bytes)
        .map_err(|err| BootstrapNamespaceError::DescriptorWrite(err.to_string()))?;

    let _ = bootstrap_basis_metadata_state();

    Ok(NamespaceSummary {
        namespace_id: namespace_id.clone(),
    })
}

const CONTENT_STORE_ID_RETRY_LIMIT: usize = 8;

fn create_new_content_store<S: ObjectStore + ?Sized>(
    store: &S,
    context: &MutationContext,
) -> Result<ContentStoreId, BootstrapNamespaceError> {
    for _attempt in 0..CONTENT_STORE_ID_RETRY_LIMIT {
        let content_store_id = ContentStoreId::generate();
        let descriptor = ContentStoreDescriptorEnvelope::from_state(
            ControlObjectKind::ContentStoreDescriptor,
            &context.writer_version,
            ContentStoreDescriptorState {
                content_store_id: content_store_id.clone(),
            },
        )
        .map_err(|err| BootstrapNamespaceError::ContentStoreWrite(err.to_string()))?;
        let bytes = serde_json::to_vec(&descriptor)
            .map_err(|err| BootstrapNamespaceError::ContentStoreWrite(err.to_string()))?;
        let key = content_store_descriptor(content_store_id.as_str());
        match store.put_if_absent(&key, &bytes) {
            Ok(_) => return Ok(content_store_id),
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(err) => return Err(BootstrapNamespaceError::ContentStoreWrite(err.to_string())),
        }
    }

    Err(BootstrapNamespaceError::ContentStoreWrite(
        "content store id generation collided repeatedly".to_owned(),
    ))
}

pub fn fork_namespace<S: ObjectStore + ?Sized>(
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

    let head_key = namespace_head(new_namespace_id.as_str());
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

pub fn list_namespaces<S: ObjectStore + ?Sized>(
    store: &S,
) -> Result<Vec<NamespaceSummary>, CoreError> {
    let keys = store
        .list_prefix("namespaces/")
        .map_err(|err| CoreError::Store(err.to_string()))?;
    let mut names = std::collections::BTreeSet::new();
    for key in keys {
        let Some(rest) = key.strip_prefix("namespaces/") else {
            continue;
        };
        let Some((namespace, leaf)) = rest.split_once('/') else {
            continue;
        };
        if leaf != "descriptor.json" {
            continue;
        }
        let namespace_id = NamespaceId::from(namespace.to_owned());
        load_namespace_descriptor(store, &namespace_id)?;
        names.insert(namespace_id);
    }
    Ok(names
        .into_iter()
        .map(|namespace_id| NamespaceSummary { namespace_id })
        .collect())
}
