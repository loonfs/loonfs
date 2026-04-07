use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::commit::{CommitHeadPublishError, CommitOp, CommitValidationError};
use crate::content::{
    read_durable_content_bytes, validate_durable_content_reference, DurableContentValidationError,
};
use crate::genesis::bootstrap_basis_metadata_state;
use crate::lease::LeaseAcquireError;
use crate::loading::ControlObjectLoadError;
use crate::metadata::{MetadataApplyError, MetadataState, ResolvedVisiblePath, VisiblePathError};
use crate::wal::WalBuildError;
use loon_api::{
    content_manifest_digest_sha256, encode_content_manifest_json, name_key_for_display_name,
    payload_checksum_sha256,
    v0::{
        CommitOp as V0CommitOp, CommitOpResult, CommitPrecondition as V0CommitPrecondition,
        CommitRequest as V0CommitRequest, CommitResponse as V0CommitResponse,
    },
    AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, ContentBlockDescriptor,
    ContentManifestEnvelope, ContentManifestPayload, ControlObjectKind, HeadState,
    HeadStateEnvelope, InodeId, InodeKind, LeaseState, LeaseStateEnvelope, MutationResult,
    NamespaceId, NamespaceSummary, CONTENT_BLOCK_SIZE_BYTES,
};
use loon_objectstore::keys::{blob, content_manifest, namespace_head, namespace_lease};
use loon_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationContext {
    pub writer_id: String,
    pub writer_version: String,
    pub now_ms: u64,
    pub lease_duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredContent {
    pub content_manifest_digest: String,
    pub file_digest_sha256: String,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutFileBehavior {
    CreateOnly,
    ReplaceExisting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreErrorKind {
    InvalidPath,
    NamespaceNotFound,
    PathNotFound,
    RevisionNotFound,
    PathConflict,
    StaleHead,
    StaleRevision,
    TombstoneConflict,
    LeaseConflict,
    WouldCycle,
    RequestIdConflict,
    CheckpointUnavailable,
    UploadNotFound,
    UploadAlreadyCompleted,
    UploadBlockConflict,
    InvalidUploadBlock,
    RebootstrapRequired,
    NamespaceCorrupt,
    ServerError,
}

#[derive(Debug, Error)]
pub enum BootstrapNamespaceError {
    #[error("holder id must not be empty")]
    EmptyHolderId,
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is partially initialized")]
    NamespacePartiallyInitialized { namespace_id: NamespaceId },
    #[error(transparent)]
    Head(#[from] ControlObjectLoadError),
    #[error("failed to write head object: {0}")]
    HeadWrite(String),
    #[error("failed to write lease object: {0}")]
    LeaseWrite(String),
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error(transparent)]
    Basis(#[from] BasisLoadError),
    #[error(transparent)]
    VisiblePath(#[from] VisiblePathError),
    #[error(transparent)]
    DurableContent(#[from] DurableContentValidationError),
    #[error(transparent)]
    Lease(#[from] LeaseAcquireError),
    #[error("commit validation failed: {0:?}")]
    CommitValidation(CommitValidationError),
    #[error("wal build failed: {0:?}")]
    WalBuild(WalBuildError),
    #[error("metadata apply failed: {0:?}")]
    MetadataApply(MetadataApplyError),
    #[error("head publish failed: {0:?}")]
    HeadPublish(CommitHeadPublishError),
    #[error("failed to write wal object: {0}")]
    WalWrite(String),
    #[error("invalid absolute path `{0}`")]
    InvalidPath(String),
    #[error("path not found `{0}`")]
    MissingPath(String),
    #[error("expected file at `{path}` but found `{kind:?}`")]
    ExpectedFile { path: String, kind: InodeKind },
    #[error("expected directory at `{path}` but found `{kind:?}`")]
    ExpectedDirectory { path: String, kind: InodeKind },
    #[error("directory not empty `{0}`")]
    DirectoryNotEmpty(String),
    #[error("cannot mutate root path")]
    RootMutationForbidden,
    #[error("destination already exists at `{0}`")]
    DestinationExists(String),
    #[error("request id conflict for `{0}`")]
    RequestIdConflict(String),
    #[error("{0}")]
    CheckpointUnavailable(String),
    #[error("upload session `{upload_id}` was not found")]
    UploadNotFound { upload_id: String },
    #[error("upload session `{upload_id}` is already completed")]
    UploadAlreadyCompleted { upload_id: String },
    #[error("upload session `{upload_id}` block `{block_index}` conflicts with prior content")]
    UploadBlockConflict { upload_id: String, block_index: u32 },
    #[error("invalid upload block: {0}")]
    InvalidUploadBlock(String),
    #[error(
        "change feed cursor `{after_seq:?}` is older than retention floor `{retention_floor_seq:?}`"
    )]
    RebootstrapRequired {
        after_seq: ChangeSeq,
        retention_floor_seq: ChangeSeq,
    },
    #[error(
        "path `{path}` is covered by subtree tombstone rooted at inode `{root_inode}` from seq `{tombstone_seq:?}`"
    )]
    TombstoneConflict {
        path: String,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("path component `{0}` is not a directory")]
    NonDirectoryPathComponent(String),
    #[error("object store error: {0}")]
    Store(String),
}

impl From<CommitValidationError> for CoreError {
    fn from(value: CommitValidationError) -> Self {
        Self::CommitValidation(value)
    }
}

impl From<WalBuildError> for CoreError {
    fn from(value: WalBuildError) -> Self {
        Self::WalBuild(value)
    }
}

impl From<MetadataApplyError> for CoreError {
    fn from(value: MetadataApplyError) -> Self {
        Self::MetadataApply(value)
    }
}

impl From<CommitHeadPublishError> for CoreError {
    fn from(value: CommitHeadPublishError) -> Self {
        Self::HeadPublish(value)
    }
}

impl CoreError {
    pub fn kind(&self) -> CoreErrorKind {
        match self {
            CoreError::Basis(error) => classify_basis_load_error(error),
            CoreError::VisiblePath(error) => classify_visible_path_error(error),
            CoreError::DurableContent(error) => classify_durable_content_error(error),
            CoreError::Lease(error) => classify_lease_acquire_error(error),
            CoreError::CommitValidation(error) => classify_commit_validation_error(error),
            CoreError::WalBuild(_)
            | CoreError::MetadataApply(_)
            | CoreError::WalWrite(_)
            | CoreError::Store(_) => CoreErrorKind::ServerError,
            CoreError::HeadPublish(error) => classify_head_publish_error(error),
            CoreError::InvalidPath(_) | CoreError::RootMutationForbidden => {
                CoreErrorKind::InvalidPath
            }
            CoreError::MissingPath(_) => CoreErrorKind::PathNotFound,
            CoreError::RequestIdConflict(_) => CoreErrorKind::RequestIdConflict,
            CoreError::CheckpointUnavailable(_) => CoreErrorKind::CheckpointUnavailable,
            CoreError::UploadNotFound { .. } => CoreErrorKind::UploadNotFound,
            CoreError::UploadAlreadyCompleted { .. } => CoreErrorKind::UploadAlreadyCompleted,
            CoreError::UploadBlockConflict { .. } => CoreErrorKind::UploadBlockConflict,
            CoreError::InvalidUploadBlock(_) => CoreErrorKind::InvalidUploadBlock,
            CoreError::RebootstrapRequired { .. } => CoreErrorKind::RebootstrapRequired,
            CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::DirectoryNotEmpty(_)
            | CoreError::DestinationExists(_) => CoreErrorKind::PathConflict,
            CoreError::TombstoneConflict { .. } => CoreErrorKind::TombstoneConflict,
            CoreError::NonDirectoryPathComponent(_) => CoreErrorKind::InvalidPath,
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

    let head_key = namespace_head(namespace_id.as_str());
    let lease_key = namespace_lease(namespace_id.as_str());
    let existing_head = store
        .head(&head_key)
        .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?
        .is_some();
    let existing_lease = store
        .head(&lease_key)
        .map_err(|err| BootstrapNamespaceError::LeaseWrite(err.to_string()))?
        .is_some();

    match (existing_head, existing_lease) {
        (true, true) if allow_existing => {
            return Ok(NamespaceSummary {
                name: namespace_id.clone(),
            });
        }
        (true, true) => {
            return Err(BootstrapNamespaceError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            });
        }
        (true, false) | (false, true) => {
            return Err(BootstrapNamespaceError::NamespacePartiallyInitialized {
                namespace_id: namespace_id.clone(),
            });
        }
        (false, false) => {}
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

    store
        .put_if_absent(&head_key, &head_bytes)
        .map_err(|err| BootstrapNamespaceError::HeadWrite(err.to_string()))?;
    store
        .put_if_absent(&lease_key, &lease_bytes)
        .map_err(|err| BootstrapNamespaceError::LeaseWrite(err.to_string()))?;

    let _ = bootstrap_basis_metadata_state();

    Ok(NamespaceSummary {
        name: namespace_id.clone(),
    })
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
        let Some((namespace, _)) = rest.split_once('/') else {
            continue;
        };
        names.insert(NamespaceId::from(namespace.to_owned()));
    }
    Ok(names
        .into_iter()
        .map(|name| NamespaceSummary { name })
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
    let manifest_digest = entry
        .content_manifest_digest
        .clone()
        .ok_or_else(|| CoreError::MissingPath(absolute_path.to_owned()))?;
    let read = read_durable_content_bytes(store, namespace_id, &manifest_digest)?;
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
    let mut blocks = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let end = std::cmp::min(offset + CONTENT_BLOCK_SIZE_BYTES as usize, bytes.len());
        let block = &bytes[offset..end];
        let digest = loon_api::sha256_digest(block);
        write_immutable_object(store, &blob(namespace_id.as_str(), &digest), block)?;
        blocks.push(ContentBlockDescriptor {
            content_digest_sha256: digest,
            plaintext_size_bytes: block.len() as u64,
        });
        offset = end;
    }

    let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
        namespace_id: namespace_id.clone(),
        file_size_bytes: bytes.len() as u64,
        file_digest_sha256: loon_api::sha256_digest(bytes),
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        blocks,
    })
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let manifest_digest = content_manifest_digest_sha256(&manifest)
        .map_err(|err| CoreError::Store(err.to_string()))?;
    let manifest_key = content_manifest(namespace_id.as_str(), &manifest_digest);
    let manifest_bytes =
        encode_content_manifest_json(&manifest).map_err(|err| CoreError::Store(err.to_string()))?;
    write_immutable_object(store, &manifest_key, &manifest_bytes)?;

    Ok(StoredContent {
        content_manifest_digest: manifest_digest,
        file_digest_sha256: loon_api::sha256_digest(bytes),
        file_size_bytes: bytes.len() as u64,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PathRequestIdentity {
    PutFile {
        namespace_id: NamespaceId,
        absolute_path: String,
        behavior: PutFileBehavior,
        content_manifest_digest: String,
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
    },
    CopyFilePath {
        namespace_id: NamespaceId,
        from_path: String,
        to_path: String,
    },
}

fn normalized_request_id(request_id: Option<&str>) -> String {
    request_id
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn source_request_checksum_sha256(identity: &PathRequestIdentity) -> Result<String, CoreError> {
    payload_checksum_sha256(identity).map_err(|err| CoreError::Store(err.to_string()))
}

fn maybe_retry_existing_path_request<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request_id: &str,
    source_request_checksum_sha256: &str,
    context: &MutationContext,
) -> Result<Option<MutationResult>, CoreError> {
    crate::protocol::retry_existing_path_request(
        store,
        namespace_id,
        request_id,
        source_request_checksum_sha256,
        context,
    )
    .map(|response| response.map(mutation_result_from_commit_response))
}

pub fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: PutFileBehavior,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let stored = store_bytes_as_content(store, namespace_id, bytes)?;
    let _validated =
        validate_durable_content_reference(store, namespace_id, &stored.content_manifest_digest)?;
    put_file_manifest(
        store,
        namespace_id,
        absolute_path,
        &stored.content_manifest_digest,
        behavior,
        context,
        request_id,
    )
}

pub fn write_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    put_file_bytes(
        store,
        namespace_id,
        absolute_path,
        bytes,
        PutFileBehavior::ReplaceExisting,
        context,
        request_id,
    )
}

pub fn put_file_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    content_manifest_digest: &str,
    behavior: PutFileBehavior,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let _validated =
        validate_durable_content_reference(store, namespace_id, content_manifest_digest)?;
    commit_file_manifest(
        store,
        namespace_id,
        absolute_path,
        content_manifest_digest,
        behavior,
        context,
        request_id,
    )
}

fn commit_file_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    content_manifest_digest: &str,
    behavior: PutFileBehavior,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let request_id = normalized_request_id(request_id);
    let source_request_checksum = source_request_checksum_sha256(&PathRequestIdentity::PutFile {
        namespace_id: namespace_id.clone(),
        absolute_path: absolute_path.to_owned(),
        behavior,
        content_manifest_digest: content_manifest_digest.to_owned(),
    })?;
    if let Some(existing) = maybe_retry_existing_path_request(
        store,
        namespace_id,
        &request_id,
        &source_request_checksum,
        context,
    )? {
        return Ok(existing);
    }

    let basis = load_verified_namespace_basis(store, namespace_id)?;
    reject_tombstoned_path_ancestor(&basis.metadata_state, absolute_path, basis.head.seq)?;
    let target = lookup_path(&basis.metadata_state, absolute_path, basis.head.seq);

    let mut ops = Vec::new();
    let mut working = basis.metadata_state.clone();
    let mut next_inode_id = basis.head.next_inode_id;
    let mut op_index = 0u32;
    let final_parent_inode = ensure_parent_directories(
        absolute_path,
        basis.head.seq,
        &mut working,
        &mut ops,
        &mut next_inode_id,
        &mut op_index,
    )?;
    let final_name = final_component(absolute_path)?;
    let mut preconditions = vec![V0CommitPrecondition::HeadSeqIs {
        expected_seq: basis.head.seq,
    }];

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
            let revision = basis
                .metadata_state
                .latest_revision_head_at_seq(existing.inode_id, basis.head.seq)
                .ok_or_else(|| CoreError::MissingPath(absolute_path.to_owned()))?;
            ops.push(V0CommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision_no: revision.revision_no,
                content_manifest_digest: content_manifest_digest.to_owned(),
            });
            preconditions.push(V0CommitPrecondition::InodeRevisionIs {
                inode_id: existing.inode_id,
                revision_no: revision.revision_no,
            });
            preconditions.push(V0CommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        Err(VisiblePathError::PathNotFound { .. }) => {
            ops.push(V0CommitOp::CreateFile {
                parent_inode: final_parent_inode,
                display_name: final_name.clone(),
                content_manifest_digest: content_manifest_digest.to_owned(),
            });
            preconditions.push(V0CommitPrecondition::ChildNameAbsent {
                parent_inode: final_parent_inode,
                name_key: name_key_for_display_name(basis.head.name_policy, &final_name),
            });
            preconditions.push(V0CommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: final_parent_inode,
            });
        }
        Err(other) => return Err(other.into()),
    }

    crate::protocol::commit_path_operations(
        store,
        namespace_id,
        V0CommitRequest {
            request_id,
            planned_head_seq: basis.head.seq,
            ops,
            preconditions,
            message: None,
            annotations: None,
        },
        source_request_checksum,
        context,
    )
    .map(mutation_result_from_commit_response)
}

pub fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let request_id = normalized_request_id(request_id);
    let source_request_checksum =
        source_request_checksum_sha256(&PathRequestIdentity::DeletePath {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.to_owned(),
            recursive: true,
        })?;
    if let Some(existing) = maybe_retry_existing_path_request(
        store,
        namespace_id,
        &request_id,
        &source_request_checksum,
        context,
    )? {
        return Ok(existing);
    }

    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let resolved = basis
        .metadata_state
        .resolve_visible_path(absolute_path, basis.head.seq)?;
    let op = match resolved.inode_kind {
        InodeKind::File => V0CommitOp::DeleteFile {
            inode_id: resolved.inode_id,
        },
        InodeKind::Dir => V0CommitOp::DeleteSubtree {
            root_inode: resolved.inode_id,
        },
        kind => {
            return Err(CoreError::ExpectedFile {
                path: absolute_path.to_owned(),
                kind,
            });
        }
    };
    crate::protocol::commit_path_operations(
        store,
        namespace_id,
        V0CommitRequest {
            request_id,
            planned_head_seq: basis.head.seq,
            ops: vec![op],
            preconditions: vec![V0CommitPrecondition::HeadSeqIs {
                expected_seq: basis.head.seq,
            }],
            message: None,
            annotations: None,
        },
        source_request_checksum,
        context,
    )
    .map(mutation_result_from_commit_response)
}

pub fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let request_id = normalized_request_id(request_id);
    let source_request_checksum =
        source_request_checksum_sha256(&PathRequestIdentity::DeletePath {
            namespace_id: namespace_id.clone(),
            absolute_path: absolute_path.to_owned(),
            recursive: false,
        })?;
    if let Some(existing) = maybe_retry_existing_path_request(
        store,
        namespace_id,
        &request_id,
        &source_request_checksum,
        context,
    )? {
        return Ok(existing);
    }

    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let resolved = basis
        .metadata_state
        .resolve_visible_path(absolute_path, basis.head.seq)?;

    let op = match resolved.inode_kind {
        InodeKind::File => V0CommitOp::DeleteFile {
            inode_id: resolved.inode_id,
        },
        InodeKind::Dir => {
            let children = basis
                .metadata_state
                .visible_children(resolved.inode_id, basis.head.seq);
            if !children.is_empty() {
                return Err(CoreError::DirectoryNotEmpty(absolute_path.to_owned()));
            }
            V0CommitOp::DeleteSubtree {
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
    crate::protocol::commit_path_operations(
        store,
        namespace_id,
        V0CommitRequest {
            request_id,
            planned_head_seq: basis.head.seq,
            ops: vec![op],
            preconditions: vec![V0CommitPrecondition::HeadSeqIs {
                expected_seq: basis.head.seq,
            }],
            message: None,
            annotations: None,
        },
        source_request_checksum,
        context,
    )
    .map(mutation_result_from_commit_response)
}

pub fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(from_path)?;
    validate_path_for_mutation(to_path)?;
    let request_id = normalized_request_id(request_id);
    let source_request_checksum = source_request_checksum_sha256(&PathRequestIdentity::MovePath {
        namespace_id: namespace_id.clone(),
        from_path: from_path.to_owned(),
        to_path: to_path.to_owned(),
    })?;
    if let Some(existing) = maybe_retry_existing_path_request(
        store,
        namespace_id,
        &request_id,
        &source_request_checksum,
        context,
    )? {
        return Ok(existing);
    }

    let basis = load_verified_namespace_basis(store, namespace_id)?;
    reject_tombstoned_path_ancestor(&basis.metadata_state, from_path, basis.head.seq)?;
    reject_tombstoned_path_ancestor(&basis.metadata_state, to_path, basis.head.seq)?;
    let source = basis
        .metadata_state
        .resolve_visible_path(from_path, basis.head.seq)?;
    let target_parent = resolve_parent_directory(&basis.metadata_state, to_path, basis.head.seq)?;
    let target_name = final_component(to_path)?;
    if lookup_path(&basis.metadata_state, to_path, basis.head.seq).is_ok() {
        return Err(CoreError::DestinationExists(to_path.to_owned()));
    }
    crate::protocol::commit_path_operations(
        store,
        namespace_id,
        V0CommitRequest {
            request_id,
            planned_head_seq: basis.head.seq,
            ops: vec![V0CommitOp::Rename {
                inode_id: source.inode_id,
                new_parent_inode: target_parent,
                new_display_name: target_name,
            }],
            preconditions: vec![V0CommitPrecondition::HeadSeqIs {
                expected_seq: basis.head.seq,
            }],
            message: None,
            annotations: None,
        },
        source_request_checksum,
        context,
    )
    .map(mutation_result_from_commit_response)
}

pub fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(from_path)?;
    validate_path_for_mutation(to_path)?;
    let request_id = normalized_request_id(request_id);
    let source_request_checksum =
        source_request_checksum_sha256(&PathRequestIdentity::CopyFilePath {
            namespace_id: namespace_id.clone(),
            from_path: from_path.to_owned(),
            to_path: to_path.to_owned(),
        })?;
    if let Some(existing) = maybe_retry_existing_path_request(
        store,
        namespace_id,
        &request_id,
        &source_request_checksum,
        context,
    )? {
        return Ok(existing);
    }

    let basis = load_verified_namespace_basis(store, namespace_id)?;
    reject_tombstoned_path_ancestor(&basis.metadata_state, from_path, basis.head.seq)?;
    reject_tombstoned_path_ancestor(&basis.metadata_state, to_path, basis.head.seq)?;

    let source = basis
        .metadata_state
        .resolve_visible_path(from_path, basis.head.seq)?;
    if source.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: from_path.to_owned(),
            kind: source.inode_kind,
        });
    }

    if lookup_path(&basis.metadata_state, to_path, basis.head.seq).is_ok() {
        return Err(CoreError::DestinationExists(to_path.to_owned()));
    }

    let revision = basis
        .metadata_state
        .latest_revision_head_at_seq(source.inode_id, basis.head.seq)
        .ok_or_else(|| CoreError::MissingPath(from_path.to_owned()))?;
    let _validated =
        validate_durable_content_reference(store, namespace_id, &revision.content_manifest_digest)?;

    let target_parent = resolve_parent_directory(&basis.metadata_state, to_path, basis.head.seq)?;
    let target_name = final_component(to_path)?;
    let target_name_key = name_key_for_display_name(basis.head.name_policy, &target_name);
    crate::protocol::commit_path_operations(
        store,
        namespace_id,
        V0CommitRequest {
            request_id,
            planned_head_seq: basis.head.seq,
            ops: vec![V0CommitOp::CreateFile {
                parent_inode: target_parent,
                display_name: target_name.clone(),
                content_manifest_digest: revision.content_manifest_digest,
            }],
            preconditions: vec![
                V0CommitPrecondition::HeadSeqIs {
                    expected_seq: basis.head.seq,
                },
                V0CommitPrecondition::ChildNameAbsent {
                    parent_inode: target_parent,
                    name_key: target_name_key,
                },
                V0CommitPrecondition::AncestorsNotSubtreeDeleted {
                    inode_id: target_parent,
                },
            ],
            message: None,
            annotations: None,
        },
        source_request_checksum,
        context,
    )
    .map(mutation_result_from_commit_response)
}

fn mutation_result_from_commit_response(response: V0CommitResponse) -> MutationResult {
    MutationResult {
        namespace_id: response.namespace_id,
        committed_seq: response.committed_seq,
    }
}

pub(crate) fn derive_commit_results(
    ops: &[CommitOp],
    allocated_inode_ids: &[InodeId],
    resolved_restore_content_manifest_digests: &[Option<String>],
) -> Vec<CommitOpResult> {
    let mut allocated = allocated_inode_ids.iter().copied();
    ops.iter()
        .enumerate()
        .map(|(index, op)| {
            let op_index = u32::try_from(index).expect("commit op index should fit in u32");
            match op {
                CommitOp::CreateDir { .. } => CommitOpResult::CreateDir {
                    op_index,
                    inode_id: allocated
                        .next()
                        .expect("allocated inode ids should cover create ops"),
                },
                CommitOp::CreateFile {
                    content_manifest_digest,
                    ..
                } => CommitOpResult::CreateFile {
                    op_index,
                    inode_id: allocated
                        .next()
                        .expect("allocated inode ids should cover create ops"),
                    revision_no: loon_api::RevisionNo(1),
                    content_manifest_digest: content_manifest_digest.clone(),
                },
                CommitOp::ReplaceFile {
                    inode_id,
                    base_revision,
                    content_manifest_digest,
                } => CommitOpResult::ReplaceFile {
                    op_index,
                    inode_id: *inode_id,
                    revision_no: loon_api::RevisionNo(
                        base_revision
                            .0
                            .checked_add(1)
                            .expect("replace_file revision increment validated"),
                    ),
                    content_manifest_digest: content_manifest_digest.clone(),
                },
                CommitOp::RestoreRevision {
                    inode_id,
                    source_revision,
                    base_revision,
                } => CommitOpResult::RestoreRevision {
                    op_index,
                    inode_id: *inode_id,
                    source_revision_no: *source_revision,
                    revision_no: loon_api::RevisionNo(
                        base_revision
                            .0
                            .checked_add(1)
                            .expect("restore_revision increment validated"),
                    ),
                    content_manifest_digest: resolved_restore_content_manifest_digests[index]
                        .as_ref()
                        .expect("resolved restore manifest digest should be present")
                        .clone(),
                },
                CommitOp::DeleteFile { inode_id } => CommitOpResult::DeleteFile {
                    op_index,
                    inode_id: *inode_id,
                },
                CommitOp::Rename { inode_id, .. } => CommitOpResult::Rename {
                    op_index,
                    inode_id: *inode_id,
                },
                CommitOp::DeleteSubtree { root_inode } => CommitOpResult::DeleteSubtree {
                    op_index,
                    root_inode: *root_inode,
                },
            }
        })
        .collect()
}

fn ensure_parent_directories(
    absolute_path: &str,
    committed_seq: ChangeSeq,
    working: &mut MetadataState,
    ops: &mut Vec<V0CommitOp>,
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

        ops.push(V0CommitOp::CreateDir {
            parent_inode: current_inode,
            display_name: component.clone(),
        });
        let allocated = *next_inode_id;
        *next_inode_id = InodeId(next_inode_id.0.saturating_add(1));
        let applied = working.apply_committed_wal_ops(
            committed_seq,
            &[loon_api::WalOp::CreateDir {
                op_index: *op_index,
                inode_id: allocated,
                parent_inode: current_inode,
                display_name: component.clone(),
            }],
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
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    resolved: &ResolvedVisiblePath,
) -> Result<AuthoritativePathEntry, CoreError> {
    let revision = metadata_state.latest_revision_head_at_seq(resolved.inode_id, head_seq);
    let content_manifest_digest = revision
        .as_ref()
        .map(|revision| revision.content_manifest_digest.clone());
    let (size_bytes, content_digest) = match content_manifest_digest.as_deref() {
        Some(manifest_digest) => {
            let validated =
                validate_durable_content_reference(store, namespace_id, manifest_digest)?;
            (
                Some(validated.file_size_bytes),
                Some(validated.file_digest_sha256),
            )
        }
        None => (None, None),
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
        content_digest,
        content_manifest_digest,
    })
}

pub(crate) fn write_immutable_object<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_bytes: &[u8],
) -> Result<(), CoreError> {
    match store.put_if_absent(object_key, expected_bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed) => {
            let existing = store
                .get(object_key, None)
                .map_err(|err| CoreError::Store(err.to_string()))?
                .ok_or_else(|| {
                    CoreError::Store(format!(
                        "missing immutable object `{object_key}` after precondition failure"
                    ))
                })?;
            if existing == expected_bytes {
                Ok(())
            } else {
                Err(CoreError::Store(format!(
                    "immutable object `{object_key}` already exists with different bytes"
                )))
            }
        }
        Err(err) => Err(CoreError::Store(err.to_string())),
    }
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
        let Some(latest_binding) =
            metadata_state.current_parent_binding_for_child(bound_child.child_inode_id, seq)
        else {
            return Ok(None);
        };
        if latest_binding.parent_inode_id != bound_child.parent_inode_id
            || latest_binding.name_key != bound_child.name_key
            || latest_binding.bind_seq != bound_child.bind_seq
            || latest_binding.bind_op_index != bound_child.bind_op_index
        {
            return Ok(None);
        }

        if let Some(tombstone) =
            metadata_state.covering_subtree_tombstone(bound_child.child_inode_id, seq)
        {
            return Ok(Some((
                join_absolute_path(&current_path, &bound_child.display_name),
                tombstone.root_inode_id,
                tombstone.tombstone_seq,
            )));
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

fn classify_control_object_load_error(error: &ControlObjectLoadError) -> CoreErrorKind {
    match error {
        ControlObjectLoadError::MissingObject { .. }
        | ControlObjectLoadError::MissingObjectAfterHead { .. } => CoreErrorKind::NamespaceNotFound,
        ControlObjectLoadError::KindMismatch { .. }
        | ControlObjectLoadError::NamespaceMismatch { .. }
        | ControlObjectLoadError::ChecksumMismatch { .. }
        | ControlObjectLoadError::Codec { .. } => CoreErrorKind::NamespaceCorrupt,
        ControlObjectLoadError::Store(_) => CoreErrorKind::ServerError,
    }
}

fn classify_basis_load_error(error: &BasisLoadError) -> CoreErrorKind {
    match error {
        BasisLoadError::LoadHead(error) | BasisLoadError::LoadLease(error) => {
            classify_control_object_load_error(error)
        }
        BasisLoadError::InvalidWalObjectKey { .. }
        | BasisLoadError::DuplicateWalSeq { .. }
        | BasisLoadError::MissingWalObject { .. }
        | BasisLoadError::MissingWalObjectAfterList { .. }
        | BasisLoadError::WalReplay(_)
        | BasisLoadError::ReconstructedHeadMismatch { .. } => CoreErrorKind::NamespaceCorrupt,
        BasisLoadError::CheckpointLoad(error) => match error.kind() {
            crate::CheckpointLoadErrorKind::Corrupt => CoreErrorKind::NamespaceCorrupt,
            crate::CheckpointLoadErrorKind::Store => CoreErrorKind::ServerError,
        },
        BasisLoadError::MissingHeadEtag { .. }
        | BasisLoadError::ListWal { .. }
        | BasisLoadError::ReadWal { .. } => CoreErrorKind::ServerError,
    }
}

fn classify_visible_path_error(error: &VisiblePathError) -> CoreErrorKind {
    match error {
        VisiblePathError::InvalidAbsolutePath { .. } => CoreErrorKind::InvalidPath,
        VisiblePathError::RootMissing => CoreErrorKind::NamespaceCorrupt,
        VisiblePathError::PathNotFound { .. } => CoreErrorKind::PathNotFound,
        VisiblePathError::PathComponentNotDirectory { .. } => CoreErrorKind::PathConflict,
    }
}

fn classify_durable_content_error(error: &DurableContentValidationError) -> CoreErrorKind {
    match error {
        DurableContentValidationError::MissingManifestObject { .. }
        | DurableContentValidationError::ManifestCodec { .. }
        | DurableContentValidationError::ManifestDigestMismatch { .. }
        | DurableContentValidationError::ManifestNamespaceMismatch { .. }
        | DurableContentValidationError::MissingBlockObject { .. }
        | DurableContentValidationError::BlockLengthMismatch { .. }
        | DurableContentValidationError::BlockDigestMismatch { .. }
        | DurableContentValidationError::FileSizeMismatch { .. }
        | DurableContentValidationError::FileDigestMismatch { .. } => {
            CoreErrorKind::NamespaceCorrupt
        }
        DurableContentValidationError::Store { .. } => CoreErrorKind::ServerError,
    }
}

fn classify_lease_acquire_error(error: &LeaseAcquireError) -> CoreErrorKind {
    match error {
        LeaseAcquireError::LoadHead(error) | LeaseAcquireError::LoadLease(error) => {
            classify_control_object_load_error(error)
        }
        LeaseAcquireError::HeldByOtherWriter { .. } => CoreErrorKind::LeaseConflict,
        LeaseAcquireError::UnexpectedControlState { .. } => CoreErrorKind::NamespaceCorrupt,
        LeaseAcquireError::EmptyWriterId
        | LeaseAcquireError::ZeroLeaseDuration
        | LeaseAcquireError::MissingHeadEtag { .. }
        | LeaseAcquireError::MissingLeaseEtag { .. }
        | LeaseAcquireError::HeadFenceTakeover(_)
        | LeaseAcquireError::HeadWrite(_)
        | LeaseAcquireError::LeaseWrite(_)
        | LeaseAcquireError::RetryExhausted { .. } => CoreErrorKind::ServerError,
    }
}

fn classify_commit_validation_error(error: &CommitValidationError) -> CoreErrorKind {
    match error {
        CommitValidationError::PlannedHeadSeqMismatch { .. }
        | CommitValidationError::MissingHeadSeqPrecondition { .. }
        | CommitValidationError::ConflictingHeadSeqPrecondition { .. } => CoreErrorKind::StaleHead,
        CommitValidationError::ReplaceFileBaseRevisionMismatch { .. }
        | CommitValidationError::RestoreRevisionBaseRevisionMismatch { .. } => {
            CoreErrorKind::StaleRevision
        }
        CommitValidationError::RestoreRevisionSourceRevisionMissing { .. } => {
            CoreErrorKind::RevisionNotFound
        }
        CommitValidationError::CreateUnderSubtreeTombstone { .. }
        | CommitValidationError::ReplaceFileUnderSubtreeTombstone { .. }
        | CommitValidationError::RestoreRevisionUnderSubtreeTombstone { .. }
        | CommitValidationError::DeleteFileCoveredByTombstone { .. }
        | CommitValidationError::RenameInodeUnderSubtreeTombstone { .. }
        | CommitValidationError::RenameTargetParentUnderSubtreeTombstone { .. }
        | CommitValidationError::DeleteSubtreeRootCoveredByTombstone { .. } => {
            CoreErrorKind::TombstoneConflict
        }
        CommitValidationError::CreateChildNameCollision { .. }
        | CommitValidationError::CreateParentNotDirectory { .. }
        | CommitValidationError::ReplaceFileInodeNotFile { .. }
        | CommitValidationError::RestoreRevisionInodeNotFile { .. }
        | CommitValidationError::DeleteFileInodeNotFile { .. }
        | CommitValidationError::RenameTargetParentNotDirectory { .. }
        | CommitValidationError::RenameTargetNameCollision { .. }
        | CommitValidationError::DeleteSubtreeRootNotDirectory { .. } => {
            CoreErrorKind::PathConflict
        }
        CommitValidationError::CreateParentMissing { .. }
        | CommitValidationError::ReplaceFileInodeMissing { .. }
        | CommitValidationError::RestoreRevisionInodeMissing { .. }
        | CommitValidationError::DeleteFileInodeMissing { .. }
        | CommitValidationError::RenameInodeMissing { .. }
        | CommitValidationError::RenameSourceBindingMissing { .. }
        | CommitValidationError::RenameTargetParentMissing { .. }
        | CommitValidationError::DeleteSubtreeRootMissing { .. } => CoreErrorKind::PathNotFound,
        CommitValidationError::RenameWouldCycleDirectory { .. } => CoreErrorKind::WouldCycle,
        CommitValidationError::StaleWriterFenceToken { .. }
        | CommitValidationError::LeaseHolderMismatch { .. }
        | CommitValidationError::LeaseExpired { .. } => CoreErrorKind::LeaseConflict,
        CommitValidationError::EmptyCommit
        | CommitValidationError::NamespaceMismatch
        | CommitValidationError::HeadLeaseNamespaceMismatch
        | CommitValidationError::HeadLeaseFenceMismatch { .. }
        | CommitValidationError::RestoreRevisionOverflow { .. }
        | CommitValidationError::ReplaceFileRevisionOverflow { .. }
        | CommitValidationError::SeqOverflow
        | CommitValidationError::NextInodeOverflow
        | CommitValidationError::OpIndexOverflow => CoreErrorKind::ServerError,
    }
}

fn classify_head_publish_error(error: &CommitHeadPublishError) -> CoreErrorKind {
    match error {
        CommitHeadPublishError::StaleHead => CoreErrorKind::StaleHead,
        CommitHeadPublishError::EmptyWriterVersion
        | CommitHeadPublishError::EmptyExpectedHeadEtag
        | CommitHeadPublishError::NamespaceMismatch { .. }
        | CommitHeadPublishError::PlanBaseHeadSeqMismatch { .. }
        | CommitHeadPublishError::PlanNextSeqMismatch { .. }
        | CommitHeadPublishError::Codec(_)
        | CommitHeadPublishError::Store(_) => CoreErrorKind::ServerError,
    }
}
