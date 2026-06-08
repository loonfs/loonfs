use crate::context::MutationContext;
use crate::metadata::{InodeRecord, MetadataState};
use crate::namespace::catalog::{
    namespace_initialization_state, NamespaceInitializationError, NamespaceInitializationState,
};
use crate::namespace::control::ControlObjectLoadError;
use loon_api::wire::control::{
    ContentStoreDescriptorEnvelope, ContentStoreDescriptorState, ControlObjectKind, HeadState,
    HeadStateEnvelope, LeaseState, LeaseStateEnvelope, NamespaceDescriptorEnvelope,
    NamespaceDescriptorState,
};
use loon_api::{
    ChangeSeq, ContentStoreId, InodeId, InodeKind, NamespaceId, NamespaceIdValidationError,
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

pub(crate) fn bootstrap_namespace<S: ObjectStore + ?Sized>(
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

pub(crate) fn bootstrap_basis_metadata_state() -> MetadataState {
    MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(0),
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}
