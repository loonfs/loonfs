use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::checkpoint::{
    create_checkpoint, load_verified_checkpoint_materialization,
    write_verified_checkpoint_from_metadata, CheckpointMetadataWriteRequest,
};
use crate::commit::SemanticCommitFingerprint;
use crate::content::{
    read_durable_content_bytes, validate_durable_content_reference, write_immutable_object,
};
use crate::genesis::bootstrap_basis_metadata_state;
use crate::loading::ControlObjectLoadError;
use crate::metadata::{MetadataState, ResolvedVisiblePath, VisiblePathError};
use crate::namespace::catalog::{
    load_namespace_content_store_id, load_namespace_descriptor, namespace_initialization_state,
    NamespaceInitializationError, NamespaceInitializationState,
};
use crate::publisher::{
    DirectObjectStorePublisher, PathMutationIntent, PlannedPathMutation, PublishOptions,
};
use loon_api::{
    name_key_for_display_name, payload_checksum_sha256,
    v0::{
        CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
        CommitRequest as ApiCommitRequest, RenameMode,
    },
    AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, CommitId, ContentRef,
    ContentStoreDescriptorEnvelope, ContentStoreDescriptorState, ContentStoreId, ControlObjectKind,
    FenceToken, HeadState, HeadStateEnvelope, InodeId, InodeKind, LeaseState, LeaseStateEnvelope,
    MutationResult, NamespaceDescriptorEnvelope, NamespaceDescriptorState, NamespaceId,
    NamespaceIdValidationError, NamespaceSummary,
};
use loon_objectstore::keys::{
    content_blob, content_store_descriptor, namespace_descriptor, namespace_head, namespace_lease,
};
use loon_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use crate::context::MutationContext;
pub use crate::error::{CoreError, CoreErrorKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredContent {
    pub content_store_id: ContentStoreId,
    pub object_key: String,
    pub content_ref: ContentRef,
    pub file_digest_sha256: String,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutFileBehavior {
    CreateOnly,
    ReplaceExisting,
}

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

pub fn resolve_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let resolved = basis
        .metadata_state
        .resolve_visible_path(absolute_path, basis.head.seq)?;
    build_authoritative_path_entry(
        store,
        namespace_id,
        &basis.content_store_id,
        basis.head.seq,
        &basis.metadata_state,
        &resolved,
    )
}

pub fn list_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let resolved = basis
        .metadata_state
        .resolve_visible_path(absolute_path, basis.head.seq)?;
    if resolved.inode_kind == InodeKind::File {
        return Ok(vec![build_authoritative_path_entry(
            store,
            namespace_id,
            &basis.content_store_id,
            basis.head.seq,
            &basis.metadata_state,
            &resolved,
        )?]);
    }
    if resolved.inode_kind != InodeKind::Dir {
        return Err(CoreError::ExpectedDirectory {
            path: resolved.absolute_path,
            kind: resolved.inode_kind,
        });
    }

    basis
        .metadata_state
        .visible_children(resolved.inode_id, basis.head.seq)
        .into_iter()
        .map(|direntry| {
            let child = basis
                .metadata_state
                .visible_inode(direntry.child_inode_id, basis.head.seq)
                .expect("visible child listing should resolve inode");
            build_authoritative_path_entry(
                store,
                namespace_id,
                &basis.content_store_id,
                basis.head.seq,
                &basis.metadata_state,
                &ResolvedVisiblePath {
                    absolute_path: join_absolute_path(
                        &resolved.absolute_path,
                        &direntry.display_name,
                    ),
                    inode_id: direntry.child_inode_id,
                    inode_kind: child.inode_kind,
                    parent_inode_id: Some(direntry.parent_inode_id),
                    display_name: direntry.display_name,
                },
            )
        })
        .collect()
}

pub fn read_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let entry = resolve_path(store, namespace_id, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    let content_ref = entry
        .content_ref
        .clone()
        .ok_or_else(|| CoreError::MissingPath(absolute_path.to_owned()))?;
    let content_store_id = load_namespace_content_store_id(store, namespace_id)?;
    let read = read_durable_content_bytes(store, &content_store_id, &content_ref)?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

pub fn store_bytes_as_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id)?;
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Store(err.to_string()))?;
    write_immutable_object(store, &object_key, bytes)?;

    Ok(StoredContent {
        content_store_id,
        object_key,
        file_digest_sha256: content_ref.digest.clone(),
        file_size_bytes: content_ref.size_bytes,
        content_ref,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PathFingerprintInput {
    CreateDir {
        namespace_id: NamespaceId,
        absolute_path: String,
    },
    PutFile {
        namespace_id: NamespaceId,
        absolute_path: String,
        behavior: PutFileBehavior,
        content_ref: ContentRef,
    },
    DeletePath {
        namespace_id: NamespaceId,
        absolute_path: String,
        recursive: bool,
    },
    MovePath {
        namespace_id: NamespaceId,
        from_path: String,
        to_path: String,
        mode: RenameMode,
    },
    CopyFilePath {
        namespace_id: NamespaceId,
        from_path: String,
        to_path: String,
    },
}

fn generated_commit_id() -> CommitId {
    CommitId::generate()
}

fn normalized_commit_id(commit_id: Option<&str>) -> Result<CommitId, CoreError> {
    let commit_id = commit_id
        .filter(|value| !value.trim().is_empty())
        .map(CommitId::parse)
        .transpose()?
        .unwrap_or_else(generated_commit_id);
    Ok(commit_id)
}

fn path_semantic_commit_fingerprint(
    identity: &PathFingerprintInput,
) -> Result<SemanticCommitFingerprint, CoreError> {
    payload_checksum_sha256(identity)
        .map(SemanticCommitFingerprint::new_unchecked)
        .map_err(|err| CoreError::Store(err.to_string()))
}

pub(crate) fn semantic_commit_fingerprint_for_path_intent(
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
) -> Result<SemanticCommitFingerprint, CoreError> {
    let identity = match intent {
        PathMutationIntent::CreateDir { absolute_path, .. } => PathFingerprintInput::CreateDir {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.clone(),
        },
        PathMutationIntent::PutFile {
            absolute_path,
            behavior,
            content_ref,
            ..
        } => PathFingerprintInput::PutFile {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.clone(),
            behavior: *behavior,
            content_ref: content_ref.clone(),
        },
        PathMutationIntent::DeletePath {
            absolute_path,
            recursive,
            ..
        } => PathFingerprintInput::DeletePath {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.clone(),
            recursive: *recursive,
        },
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            mode,
            ..
        } => PathFingerprintInput::MovePath {
            namespace_id: namespace_id.clone(),
            from_path: from_path.clone(),
            to_path: to_path.clone(),
            mode: *mode,
        },
        PathMutationIntent::CopyFilePath {
            from_path, to_path, ..
        } => PathFingerprintInput::CopyFilePath {
            namespace_id: namespace_id.clone(),
            from_path: from_path.clone(),
            to_path: to_path.clone(),
        },
    };
    path_semantic_commit_fingerprint(&identity)
}

pub fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: PutFileBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let stored = store_bytes_as_content(store, namespace_id, bytes)?;
    put_file_content_ref(
        store,
        namespace_id,
        absolute_path,
        stored.content_ref,
        behavior,
        context,
        commit_id,
    )
}

pub fn write_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    put_file_bytes(
        store,
        namespace_id,
        absolute_path,
        bytes,
        PutFileBehavior::ReplaceExisting,
        context,
        commit_id,
    )
}

pub fn create_dir_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::CreateDir {
        commit_id,
        absolute_path: absolute_path.to_owned(),
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn put_file_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    content_ref: ContentRef,
    behavior: PutFileBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::PutFile {
        commit_id,
        absolute_path: absolute_path.to_owned(),
        content_ref,
        behavior,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub(crate) fn plan_path_mutation<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
) -> Result<PlannedPathMutation, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    plan_path_mutation_against_state(
        store,
        namespace_id,
        intent,
        &basis.head,
        &basis.metadata_state,
        &basis.content_store_id,
    )
}

pub(crate) fn plan_path_mutation_against_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: &PathMutationIntent,
    head: &HeadState,
    metadata_state: &MetadataState,
    content_store_id: &ContentStoreId,
) -> Result<PlannedPathMutation, CoreError> {
    let commit_id = intent.commit_id().clone();
    let semantic_fingerprint = semantic_commit_fingerprint_for_path_intent(namespace_id, intent)?;
    let view = PathPlanningView {
        head,
        metadata_state,
        content_store_id,
    };
    let commit_request = match intent {
        PathMutationIntent::CreateDir { absolute_path, .. } => {
            plan_create_dir(absolute_path, &commit_id, &view)?
        }
        PathMutationIntent::PutFile {
            absolute_path,
            content_ref,
            behavior,
            ..
        } => plan_put_file_content_ref(
            store,
            absolute_path,
            content_ref.clone(),
            *behavior,
            &commit_id,
            &view,
        )?,
        PathMutationIntent::DeletePath {
            absolute_path,
            recursive,
            ..
        } => plan_delete_path(absolute_path, *recursive, &commit_id, &view)?,
        PathMutationIntent::MovePath {
            from_path,
            to_path,
            mode,
            ..
        } => plan_move_path(from_path, to_path, *mode, &commit_id, &view)?,
        PathMutationIntent::CopyFilePath {
            from_path, to_path, ..
        } => plan_copy_file_path(store, from_path, to_path, &commit_id, &view)?,
    };
    Ok(PlannedPathMutation {
        commit_id,
        semantic_commit_fingerprint: semantic_fingerprint,
        commit_request,
    })
}

struct PathPlanningView<'a> {
    head: &'a HeadState,
    metadata_state: &'a MetadataState,
    content_store_id: &'a ContentStoreId,
}

fn child_name_is_precondition(
    view: &PathPlanningView<'_>,
    resolved: &ResolvedVisiblePath,
) -> Result<ApiCommitPrecondition, CoreError> {
    let parent_inode = resolved
        .parent_inode_id
        .ok_or(CoreError::RootMutationForbidden)?;
    let binding = view
        .metadata_state
        .current_parent_binding_for_child(resolved.inode_id, view.head.seq)
        .ok_or_else(|| CoreError::MissingPath(resolved.absolute_path.clone()))?;
    if binding.parent_inode_id != parent_inode {
        return Err(CoreError::MissingPath(resolved.absolute_path.clone()));
    }
    Ok(ApiCommitPrecondition::BindingIs {
        parent_inode,
        name_key: binding.name_key,
        child_inode: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

fn child_name_absent_precondition(
    view: &PathPlanningView<'_>,
    parent_inode: InodeId,
    display_name: &str,
) -> ApiCommitPrecondition {
    ApiCommitPrecondition::ChildNameAbsent {
        parent_inode,
        name_key: name_key_for_display_name(view.head.name_policy, display_name),
    }
}

fn plan_create_dir(
    absolute_path: &str,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    reject_tombstoned_path_ancestor(view.metadata_state, absolute_path, view.head.seq)?;
    if lookup_path(view.metadata_state, absolute_path, view.head.seq).is_ok() {
        return Err(CoreError::DestinationExists(absolute_path.to_owned()));
    }
    let parent_inode = resolve_parent_directory(view.metadata_state, absolute_path, view.head.seq)?;
    let display_name = final_component(absolute_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::CreateDir {
            parent_inode,
            display_name: display_name.clone(),
        }],
        preconditions: vec![
            child_name_absent_precondition(view, parent_inode, &display_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode,
            },
        ],
        message: None,
        annotations: None,
    })
}

fn plan_put_file_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    absolute_path: &str,
    content_ref: ContentRef,
    behavior: PutFileBehavior,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let _validated =
        validate_durable_content_reference(store, view.content_store_id, &content_ref)?;
    reject_tombstoned_path_ancestor(view.metadata_state, absolute_path, view.head.seq)?;
    let target = lookup_path(view.metadata_state, absolute_path, view.head.seq);

    let mut ops = Vec::new();
    let mut working = view.metadata_state.clone();
    let mut next_inode_id = view.head.next_inode_id;
    let mut op_index = 0u32;
    let final_parent_inode = ensure_parent_directories(
        absolute_path,
        view.head.seq,
        &mut working,
        &mut ops,
        &mut next_inode_id,
        &mut op_index,
    )?;
    let final_name = final_component(absolute_path)?;
    let mut preconditions = Vec::new();

    match target {
        Ok(existing) => {
            if behavior == PutFileBehavior::CreateOnly {
                return Err(CoreError::DestinationExists(absolute_path.to_owned()));
            }
            if existing.inode_kind != InodeKind::File {
                return Err(CoreError::ExpectedFile {
                    path: absolute_path.to_owned(),
                    kind: existing.inode_kind,
                });
            }
            let revision = view
                .metadata_state
                .latest_revision_head_at_seq(existing.inode_id, view.head.seq)
                .ok_or_else(|| CoreError::MissingPath(absolute_path.to_owned()))?;
            preconditions.push(child_name_is_precondition(view, &existing)?);
            ops.push(ApiCommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision_no: revision.revision_no,
                content_ref: content_ref.clone(),
            });
            preconditions.push(ApiCommitPrecondition::InodeRevisionIs {
                inode_id: existing.inode_id,
                revision_no: revision.revision_no,
            });
            preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        Err(VisiblePathError::PathNotFound { .. }) => {
            ops.push(ApiCommitOp::CreateFile {
                parent_inode: final_parent_inode,
                display_name: final_name.clone(),
                content_ref,
            });
            if view
                .metadata_state
                .visible_inode(final_parent_inode, view.head.seq)
                .is_some()
            {
                preconditions.push(child_name_absent_precondition(
                    view,
                    final_parent_inode,
                    &final_name,
                ));
                preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                    inode_id: final_parent_inode,
                });
            }
        }
        Err(other) => return Err(other.into()),
    }

    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
        annotations: None,
    })
}

pub fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::DeletePath {
        commit_id,
        absolute_path: absolute_path.to_owned(),
        recursive: true,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::DeletePath {
        commit_id,
        absolute_path: absolute_path.to_owned(),
        recursive: false,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::MovePath {
        commit_id,
        from_path: from_path.to_owned(),
        to_path: to_path.to_owned(),
        mode: RenameMode::NoReplace,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::CopyFilePath {
        commit_id,
        from_path: from_path.to_owned(),
        to_path: to_path.to_owned(),
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

fn plan_delete_path(
    absolute_path: &str,
    recursive: bool,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let resolved = view
        .metadata_state
        .resolve_visible_path(absolute_path, view.head.seq)?;
    let op = match resolved.inode_kind {
        InodeKind::File => ApiCommitOp::DeleteFile {
            inode_id: resolved.inode_id,
        },
        InodeKind::Dir if recursive => ApiCommitOp::DeleteSubtree {
            root_inode: resolved.inode_id,
        },
        InodeKind::Dir => {
            let children = view
                .metadata_state
                .visible_children(resolved.inode_id, view.head.seq);
            if !children.is_empty() {
                return Err(CoreError::DirectoryNotEmpty(absolute_path.to_owned()));
            }
            ApiCommitOp::DeleteSubtree {
                root_inode: resolved.inode_id,
            }
        }
        kind => {
            return Err(CoreError::ExpectedFile {
                path: absolute_path.to_owned(),
                kind,
            });
        }
    };
    let mut preconditions = vec![child_name_is_precondition(view, &resolved)?];
    if !recursive && resolved.inode_kind == InodeKind::Dir {
        preconditions.push(ApiCommitPrecondition::DirectoryEmpty {
            inode_id: resolved.inode_id,
        });
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![op],
        preconditions,
        message: None,
        annotations: None,
    })
}

fn plan_move_path(
    from_path: &str,
    to_path: &str,
    mode: RenameMode,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    if mode != RenameMode::NoReplace {
        return Err(CoreError::CommitValidation(
            crate::commit::CommitValidationError::UnsupportedRenameMode { mode },
        ));
    }
    validate_path_for_mutation(from_path)?;
    validate_path_for_mutation(to_path)?;
    reject_tombstoned_path_ancestor(view.metadata_state, from_path, view.head.seq)?;
    reject_tombstoned_path_ancestor(view.metadata_state, to_path, view.head.seq)?;
    let source = view
        .metadata_state
        .resolve_visible_path(from_path, view.head.seq)?;
    let target_parent = resolve_parent_directory(view.metadata_state, to_path, view.head.seq)?;
    let target_name = final_component(to_path)?;
    if lookup_path(view.metadata_state, to_path, view.head.seq).is_ok() {
        return Err(CoreError::DestinationExists(to_path.to_owned()));
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::Rename {
            inode_id: source.inode_id,
            new_parent_inode: target_parent,
            new_display_name: target_name.clone(),
            mode,
        }],
        preconditions: vec![
            child_name_is_precondition(view, &source)?,
            child_name_absent_precondition(view, target_parent, &target_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: source.inode_id,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target_parent,
            },
        ],
        message: None,
        annotations: None,
    })
}

fn plan_copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    from_path: &str,
    to_path: &str,
    commit_id: &CommitId,
    view: &PathPlanningView<'_>,
) -> Result<ApiCommitRequest, CoreError> {
    validate_path_for_mutation(from_path)?;
    validate_path_for_mutation(to_path)?;
    reject_tombstoned_path_ancestor(view.metadata_state, from_path, view.head.seq)?;
    reject_tombstoned_path_ancestor(view.metadata_state, to_path, view.head.seq)?;

    let source = view
        .metadata_state
        .resolve_visible_path(from_path, view.head.seq)?;
    if source.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: from_path.to_owned(),
            kind: source.inode_kind,
        });
    }

    if lookup_path(view.metadata_state, to_path, view.head.seq).is_ok() {
        return Err(CoreError::DestinationExists(to_path.to_owned()));
    }

    let revision = view
        .metadata_state
        .latest_revision_head_at_seq(source.inode_id, view.head.seq)
        .ok_or_else(|| CoreError::MissingPath(from_path.to_owned()))?;
    let _validated =
        validate_durable_content_reference(store, view.content_store_id, &revision.content_ref)?;

    let target_parent = resolve_parent_directory(view.metadata_state, to_path, view.head.seq)?;
    let target_name = final_component(to_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::CreateFile {
            parent_inode: target_parent,
            display_name: target_name.clone(),
            content_ref: revision.content_ref,
        }],
        preconditions: vec![
            child_name_is_precondition(view, &source)?,
            ApiCommitPrecondition::InodeRevisionIs {
                inode_id: source.inode_id,
                revision_no: revision.revision_no,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: source.inode_id,
            },
            child_name_absent_precondition(view, target_parent, &target_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target_parent,
            },
        ],
        message: None,
        annotations: None,
    })
}

fn ensure_parent_directories(
    absolute_path: &str,
    committed_seq: ChangeSeq,
    working: &mut MetadataState,
    ops: &mut Vec<ApiCommitOp>,
    next_inode_id: &mut InodeId,
    op_index: &mut u32,
) -> Result<InodeId, CoreError> {
    let components = path_components(absolute_path)?;
    if components.len() <= 1 {
        return Ok(InodeId(1));
    }

    let mut current_inode = InodeId(1);
    for component in &components[..components.len() - 1] {
        if let Some(child) = working.visible_child(current_inode, component, committed_seq) {
            let inode = working
                .visible_inode(child.child_inode_id, committed_seq)
                .ok_or_else(|| CoreError::MissingPath(component.clone()))?;
            if inode.inode_kind != InodeKind::Dir {
                return Err(CoreError::NonDirectoryPathComponent(component.clone()));
            }
            current_inode = child.child_inode_id;
            continue;
        }

        ops.push(ApiCommitOp::CreateDir {
            parent_inode: current_inode,
            display_name: component.clone(),
        });
        let allocated = *next_inode_id;
        *next_inode_id = InodeId(next_inode_id.0.saturating_add(1));
        let delta_index = op_index.saturating_mul(2);
        let applied = working.apply_committed_wal_ops(
            committed_seq,
            &[
                loon_api::WalOp::CreateInode {
                    delta_index,
                    inode_id: allocated,
                    inode_kind: InodeKind::Dir,
                },
                loon_api::WalOp::BindDirentry {
                    delta_index: delta_index.saturating_add(1),
                    parent_inode: current_inode,
                    display_name: component.clone(),
                    child_inode: allocated,
                },
            ],
        )?;
        *working = applied.metadata_state;
        *op_index = op_index.saturating_add(1);
        current_inode = allocated;
    }
    Ok(current_inode)
}

fn resolve_parent_directory(
    metadata_state: &MetadataState,
    absolute_path: &str,
    seq: ChangeSeq,
) -> Result<InodeId, CoreError> {
    let components = path_components(absolute_path)?;
    if components.len() <= 1 {
        return Ok(InodeId(1));
    }
    let parent_path = format!("/{}", components[..components.len() - 1].join("/"));
    let resolved = metadata_state.resolve_visible_path(&parent_path, seq)?;
    if resolved.inode_kind != InodeKind::Dir {
        return Err(CoreError::ExpectedDirectory {
            path: parent_path,
            kind: resolved.inode_kind,
        });
    }
    Ok(resolved.inode_id)
}

fn build_authoritative_path_entry<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    resolved: &ResolvedVisiblePath,
) -> Result<AuthoritativePathEntry, CoreError> {
    let revision = metadata_state.latest_revision_head_at_seq(resolved.inode_id, head_seq);
    let content_ref = revision
        .as_ref()
        .map(|revision| revision.content_ref.clone());
    let size_bytes = match content_ref.as_ref() {
        Some(content_ref) => {
            let validated =
                validate_durable_content_reference(store, content_store_id, content_ref)?;
            Some(validated.file_size_bytes)
        }
        None => None,
    };

    Ok(AuthoritativePathEntry {
        namespace_id: namespace_id.clone(),
        absolute_path: resolved.absolute_path.clone(),
        inode_id: resolved.inode_id,
        inode_kind: resolved.inode_kind.clone(),
        authoritative_head_seq: head_seq,
        parent_inode_id: resolved.parent_inode_id,
        display_name: resolved.display_name.clone(),
        revision_no: revision.as_ref().map(|revision| revision.revision_no),
        size_bytes,
        content_ref,
    })
}

fn lookup_path(
    metadata_state: &MetadataState,
    absolute_path: &str,
    seq: ChangeSeq,
) -> Result<ResolvedVisiblePath, VisiblePathError> {
    metadata_state.resolve_visible_path(absolute_path, seq)
}

fn validate_path_for_mutation(absolute_path: &str) -> Result<(), CoreError> {
    if absolute_path == "/" {
        return Err(CoreError::RootMutationForbidden);
    }
    path_components(absolute_path).map(|_| ())
}

fn path_components(absolute_path: &str) -> Result<Vec<String>, CoreError> {
    if !absolute_path.starts_with('/') {
        return Err(CoreError::InvalidPath(absolute_path.to_owned()));
    }
    let mut out = Vec::new();
    for component in absolute_path.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(CoreError::InvalidPath(absolute_path.to_owned()));
        }
        out.push(component.to_owned());
    }
    if out.is_empty() {
        return Err(CoreError::InvalidPath(absolute_path.to_owned()));
    }
    Ok(out)
}

fn final_component(absolute_path: &str) -> Result<String, CoreError> {
    let components = path_components(absolute_path)?;
    components
        .last()
        .cloned()
        .ok_or_else(|| CoreError::InvalidPath(absolute_path.to_owned()))
}

fn join_absolute_path(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
    }
}

fn reject_tombstoned_path_ancestor(
    metadata_state: &MetadataState,
    absolute_path: &str,
    seq: ChangeSeq,
) -> Result<(), CoreError> {
    let Some((path, root_inode, tombstone_seq)) =
        tombstoned_path_ancestor(metadata_state, absolute_path, seq)?
    else {
        return Ok(());
    };
    Err(CoreError::TombstoneConflict {
        path,
        root_inode,
        tombstone_seq,
    })
}

fn tombstoned_path_ancestor(
    metadata_state: &MetadataState,
    absolute_path: &str,
    seq: ChangeSeq,
) -> Result<Option<(String, InodeId, ChangeSeq)>, CoreError> {
    let components = path_components(absolute_path)?;
    let mut current_inode = InodeId(1);
    let mut current_path = "/".to_owned();

    for component in components {
        let Some(bound_child) = metadata_state.bound_child_at_seq(current_inode, &component, seq)
        else {
            return Ok(None);
        };
        if let Some(tombstone) =
            metadata_state.covering_subtree_tombstone(bound_child.child_inode_id, seq)
        {
            return Ok(Some((
                join_absolute_path(&current_path, &bound_child.display_name),
                tombstone.root_inode_id,
                tombstone.tombstone_seq,
            )));
        }
        let Some(latest_binding) =
            metadata_state.current_parent_binding_for_child(bound_child.child_inode_id, seq)
        else {
            return Ok(None);
        };
        if latest_binding.parent_inode_id != bound_child.parent_inode_id
            || latest_binding.name_key != bound_child.name_key
            || latest_binding.bind_seq != bound_child.bind_seq
            || latest_binding.bind_delta_index != bound_child.bind_delta_index
        {
            return Ok(None);
        }

        if metadata_state
            .visible_inode(bound_child.child_inode_id, seq)
            .is_none()
        {
            return Ok(None);
        }
        current_path = join_absolute_path(&current_path, &bound_child.display_name);
        current_inode = bound_child.child_inode_id;
    }

    Ok(None)
}
