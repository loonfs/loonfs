use crate::checkpoint::{build_initial_namespace_manifest, write_namespace_manifest};
use crate::context::MutationContext;
use crate::metadata::{InodeRecord, MetadataState};
use crate::namespace::catalog::{
    namespace_initialization_state, NamespaceInitializationError, NamespaceInitializationState,
};
use crate::namespace::control::read_head_object;
use crate::namespace::control::ControlObjectLoadError;
use bytes::Bytes;
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::wire::control::{
    encode_control_object, ContentStoreDescriptorEnvelope, ContentStoreDescriptorState,
    ControlObjectKind, HeadState, HeadStateEnvelope, MetadataRootEnvelope, MetadataRootState,
    NamespaceConfigEnvelope, NamespaceConfigState, WalFloorBasis, WalFloorEnvelope, WalFloorState,
    WriterBlock,
};
use loonfs_api::{
    ChangeSeq, ContentStoreId, ErrorCode, InodeId, InodeKind, NamePolicy, NamespaceId,
    NamespaceIdValidationError, NamespaceSummary,
};
use loonfs_objectstore::keys::{
    content_store_descriptor, metadata_root, namespace_config, wal_floor, wal_head,
};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
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
    #[error("namespace `{namespace_id}` is deleted and its id is retired")]
    NamespaceDeleted { namespace_id: NamespaceId },
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
    #[error("failed to write initial namespace manifest: {0}")]
    ManifestWrite(String),
}

impl BootstrapNamespaceError {
    /// Returns the stable machine-readable reason for this error.
    ///
    /// This is the single source of truth for the wire code every surface
    /// (HTTP server, CLI) reports for a bootstrap failure, mirroring
    /// [`CoreError::code`](crate::Error::code).
    pub fn code(&self) -> ErrorCode {
        match self {
            BootstrapNamespaceError::InvalidNamespaceId(_)
            | BootstrapNamespaceError::EmptyHolderId
            | BootstrapNamespaceError::EmptyWriterVersion => ErrorCode::InvalidRequest,
            BootstrapNamespaceError::NamespaceAlreadyExists { .. } => ErrorCode::NamespaceExists,
            BootstrapNamespaceError::NamespacePartiallyInitialized { .. } => {
                ErrorCode::NamespacePartial
            }
            BootstrapNamespaceError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            BootstrapNamespaceError::Descriptor(_)
            | BootstrapNamespaceError::DescriptorWrite(_)
            | BootstrapNamespaceError::ContentStoreWrite(_)
            | BootstrapNamespaceError::Head(_)
            | BootstrapNamespaceError::HeadWrite(_)
            | BootstrapNamespaceError::ManifestWrite(_) => ErrorCode::ServerError,
        }
    }
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
            NamespaceInitializationError::LoadNamespaceDescriptor(error)
            | NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
                Self::Descriptor(error)
            }
        }
    }
}

pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
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

    match namespace_initialization_state(store, namespace_id).await? {
        NamespaceInitializationState::Complete => {
            let head = read_head_object(store, namespace_id)
                .await
                .map_err(BootstrapNamespaceError::Head)?;
            // A deleted namespace retires its id permanently; re-creation is
            // refused as deleted, not as existing.
            if head.envelope.state.state == NamespaceState::Deleted {
                return Err(BootstrapNamespaceError::NamespaceDeleted {
                    namespace_id: namespace_id.clone(),
                });
            }
            if allow_existing {
                return Ok(NamespaceSummary {
                    namespace_id: namespace_id.clone(),
                });
            }
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

    let mut initial_head = HeadState::initial(namespace_id.clone());
    initial_head.writer = Some(WriterBlock {
        writer_id: context.writer_id.clone(),
        writer_session_id: context.writer_session_id.clone(),
        acquired_at_ms: context.now_ms,
    });
    let initial_manifest = build_initial_namespace_manifest(
        store,
        namespace_id,
        &initial_head,
        &context.writer_version,
    )
    .await
    .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;
    write_namespace_manifest(store, &initial_manifest)
        .await
        .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;

    let root_envelope = MetadataRootEnvelope::from_state(
        ControlObjectKind::MetadataRoot,
        &context.writer_version,
        MetadataRootState {
            namespace_id: namespace_id.clone(),
            manifest_id: initial_manifest.payload.manifest_id,
            manifest_head_seq: initial_head.seq,
            manifest_payload_checksum: initial_manifest.payload_checksum.clone(),
            updated_at_ms: context.now_ms,
        },
    )
    .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;
    let root_bytes = encode_control_object(&root_envelope)
        .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;
    store
        .put_if_absent(
            &metadata_root(namespace_id.as_str()),
            Bytes::from(root_bytes),
        )
        .await
        .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;

    let floor_envelope = WalFloorEnvelope::from_state(
        ControlObjectKind::WalFloor,
        &context.writer_version,
        WalFloorState {
            namespace_id: namespace_id.clone(),
            floor_seq: initial_head.seq,
            basis: WalFloorBasis {
                manifest_id: initial_manifest.payload.manifest_id,
                manifest_head_seq: initial_head.seq,
                manifest_payload_checksum: initial_manifest.payload_checksum.clone(),
            },
            verified_at_ms: context.now_ms,
            updated_at_ms: context.now_ms,
        },
    )
    .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;
    let floor_bytes = encode_control_object(&floor_envelope)
        .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;
    store
        .put_if_absent(&wal_floor(namespace_id.as_str()), Bytes::from(floor_bytes))
        .await
        .map_err(|err| BootstrapNamespaceError::ManifestWrite(err.to_string()))?;

    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::WalHead,
        &context.writer_version,
        initial_head,
    )
    .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?;
    let head_bytes = encode_control_object(&head_envelope)
        .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?;

    let head_key = wal_head(namespace_id.as_str());
    store
        .put_if_absent(&head_key, Bytes::from(head_bytes))
        .await
        .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?;

    let content_store_id = create_new_content_store(store, context).await?;
    let namespace_descriptor_envelope = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: namespace_id.clone(),
            content_store_id,
            name_policy: NamePolicy::default(),
        },
    )
    .map_err(|err| BootstrapNamespaceError::DescriptorWrite(err.to_string()))?;
    let namespace_descriptor_bytes = encode_control_object(&namespace_descriptor_envelope)
        .map_err(|err| BootstrapNamespaceError::DescriptorWrite(err.to_string()))?;

    let descriptor_key = namespace_config(namespace_id.as_str());
    store
        .put_if_absent(&descriptor_key, Bytes::from(namespace_descriptor_bytes))
        .await
        .map_err(|err| BootstrapNamespaceError::DescriptorWrite(err.to_string()))?;

    Ok(NamespaceSummary {
        namespace_id: namespace_id.clone(),
    })
}

const CONTENT_STORE_ID_RETRY_LIMIT: usize = 8;

async fn create_new_content_store<S: ObjectStore + ?Sized>(
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
        let bytes = encode_control_object(&descriptor)
            .map_err(|err| BootstrapNamespaceError::ContentStoreWrite(err.to_string()))?;
        let key = content_store_descriptor(content_store_id.as_str());
        match store.put_if_absent(&key, Bytes::from(bytes)).await {
            Ok(_) => return Ok(content_store_id),
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(err) => return Err(BootstrapNamespaceError::ContentStoreWrite(err.to_string())),
        }
    }

    Err(BootstrapNamespaceError::ContentStoreWrite(
        "content store id generation collided repeatedly".to_owned(),
    ))
}

pub(crate) fn bootstrap_metadata_state() -> MetadataState {
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
