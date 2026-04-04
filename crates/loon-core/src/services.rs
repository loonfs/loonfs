use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::commit::{
    build_commit_plan, prepare_commit_head_publish, publish_commit_head, CommitHeadPublishError,
    CommitOp, CommitRequest, CommitValidationContext, CommitValidationError, Precondition,
};
use crate::content::{
    read_durable_content_bytes, validate_durable_content_reference, DurableContentValidationError,
};
use crate::genesis::bootstrap_basis_metadata_state;
use crate::lease::{acquire_or_renew_namespace_lease, LeaseAcquireError};
use crate::loading::ControlObjectLoadError;
use crate::metadata::{MetadataApplyError, MetadataState, ResolvedVisiblePath, VisiblePathError};
use crate::wal::{prepare_wal_commit, WalBuildError};
use loon_api::{
    content_manifest_digest_sha256, encode_content_manifest_json, AuthoritativeFileBytes,
    AuthoritativePathEntry, ChangeSeq, ContentBlockDescriptor, ContentManifestEnvelope,
    ContentManifestPayload, ControlObjectKind, HeadState, HeadStateEnvelope, InodeId, InodeKind,
    LeaseState, LeaseStateEnvelope, MutationResult, NamespaceId, NamespaceSummary,
    CONTENT_BLOCK_SIZE_BYTES,
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
    #[error("cannot mutate root path")]
    RootMutationForbidden,
    #[error("destination already exists at `{0}`")]
    DestinationExists(String),
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

pub fn bootstrap_namespace<S: ObjectStore>(
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

pub fn list_namespaces<S: ObjectStore>(store: &S) -> Result<Vec<NamespaceSummary>, CoreError> {
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

pub fn resolve_path<S: ObjectStore>(
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

pub fn list_path<S: ObjectStore>(
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

pub fn read_file_bytes<S: ObjectStore>(
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

pub fn store_bytes_as_content<S: ObjectStore>(
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

pub fn write_file_bytes<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let stored = store_bytes_as_content(store, namespace_id, bytes)?;
    let _validated =
        validate_durable_content_reference(store, namespace_id, &stored.content_manifest_digest)?;
    let request_id = request_id
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    acquire_or_renew_namespace_lease(store, namespace_id, context)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
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
    let mut preconditions = vec![Precondition::HeadSeqIs(basis.head.seq)];

    match target {
        Ok(existing) => {
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
            ops.push(CommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision: revision.revision_no,
                content_manifest_digest: stored.content_manifest_digest,
            });
            preconditions.push(Precondition::InodeRevisionIs {
                inode_id: existing.inode_id,
                revision: revision.revision_no,
            });
            preconditions.push(Precondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        Err(VisiblePathError::PathNotFound { .. }) => {
            ops.push(CommitOp::CreateFile {
                parent_inode: final_parent_inode,
                display_name: final_name.clone(),
                content_manifest_digest: stored.content_manifest_digest,
            });
            preconditions.push(Precondition::ChildNameAbsent {
                parent_inode: final_parent_inode,
                name_key: final_name,
            });
            preconditions.push(Precondition::AncestorsNotSubtreeDeleted {
                inode_id: final_parent_inode,
            });
        }
        Err(other) => return Err(other.into()),
    }

    let writer_fence_token = basis.head.active_fence_token;
    let planned_head_seq = basis.head.seq;
    execute_commit(
        store,
        basis,
        context,
        CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id,
            writer_id: context.writer_id.clone(),
            writer_fence_token,
            planned_head_seq,
            ops,
            preconditions,
        },
    )
}

pub fn delete_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    acquire_or_renew_namespace_lease(store, namespace_id, context)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let resolved = basis
        .metadata_state
        .resolve_visible_path(absolute_path, basis.head.seq)?;
    let op = match resolved.inode_kind {
        InodeKind::File => CommitOp::DeleteFile {
            inode_id: resolved.inode_id,
        },
        InodeKind::Dir => CommitOp::DeleteSubtree {
            root_inode: resolved.inode_id,
        },
        kind => {
            return Err(CoreError::ExpectedFile {
                path: absolute_path.to_owned(),
                kind,
            });
        }
    };
    let writer_fence_token = basis.head.active_fence_token;
    let planned_head_seq = basis.head.seq;
    execute_commit(
        store,
        basis,
        context,
        CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: request_id
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            writer_id: context.writer_id.clone(),
            writer_fence_token,
            planned_head_seq,
            ops: vec![op],
            preconditions: vec![Precondition::HeadSeqIs(planned_head_seq)],
        },
    )
}

pub fn move_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    request_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(from_path)?;
    validate_path_for_mutation(to_path)?;
    acquire_or_renew_namespace_lease(store, namespace_id, context)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let source = basis
        .metadata_state
        .resolve_visible_path(from_path, basis.head.seq)?;
    let target_parent = resolve_parent_directory(&basis.metadata_state, to_path, basis.head.seq)?;
    let target_name = final_component(to_path)?;
    if lookup_path(&basis.metadata_state, to_path, basis.head.seq).is_ok() {
        return Err(CoreError::DestinationExists(to_path.to_owned()));
    }
    let writer_fence_token = basis.head.active_fence_token;
    let planned_head_seq = basis.head.seq;
    execute_commit(
        store,
        basis,
        context,
        CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: request_id
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            writer_id: context.writer_id.clone(),
            writer_fence_token,
            planned_head_seq,
            ops: vec![CommitOp::Rename {
                inode_id: source.inode_id,
                new_parent_inode: target_parent,
                new_display_name: target_name,
            }],
            preconditions: vec![Precondition::HeadSeqIs(planned_head_seq)],
        },
    )
}

fn execute_commit<S: ObjectStore>(
    store: &S,
    basis: crate::VerifiedNamespaceBasis,
    context: &MutationContext,
    request: CommitRequest,
) -> Result<MutationResult, CoreError> {
    let validation = CommitValidationContext {
        head: basis.head.clone(),
        lease: basis.lease.clone(),
        now_ms: context.now_ms,
        metadata_state: basis.metadata_state.clone(),
    };
    let plan = build_commit_plan(&request, &validation)?;
    let wal = prepare_wal_commit(&request, &plan, &context.writer_version)?;
    let applied = basis
        .metadata_state
        .apply_committed_wal_ops(plan.next_seq, &wal.envelope.payload.ops)?;
    let head_publish = prepare_commit_head_publish(&basis.head, &plan, &context.writer_version)?;
    store
        .put_if_absent(&wal.object_key, &wal.encoded_bytes)
        .map_err(|err| CoreError::WalWrite(err.to_string()))?;
    publish_commit_head(store, &basis.head_etag, &head_publish)?;
    let _ = applied;
    Ok(MutationResult {
        namespace_id: request.namespace_id,
        committed_seq: head_publish.resulting_head.seq,
    })
}

fn ensure_parent_directories(
    absolute_path: &str,
    committed_seq: ChangeSeq,
    working: &mut MetadataState,
    ops: &mut Vec<CommitOp>,
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

        ops.push(CommitOp::CreateDir {
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

fn build_authoritative_path_entry<S: ObjectStore>(
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

fn write_immutable_object<S: ObjectStore>(
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
