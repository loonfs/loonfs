use crate::genesis::bootstrap_basis_metadata_state;
use crate::mutation::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::mutation::lease::LeaseAcquireError;
use crate::mutation::loading::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::mutation::{
    execute_client_mutation_against_basis, execute_commit_request_against_basis,
};
use crate::mutation::{ClientMutationExecutionError, ClientMutationExecutionParams};
use loon_core::checkpoint::{
    load_checkpoint, load_checkpoint_manifest, prepare_checkpoint, prepare_checkpoint_head_publish,
    publish_checkpoint_head, CheckpointBuildError, CheckpointHeadPublishRequest,
    CheckpointPublishError, CheckpointReplayError, StoredCheckpointManifest,
    StoredCheckpointSegment,
};
use loon_core::commit::{CommitRequest, Precondition};
use loon_core::content::{
    read_durable_content_bytes, upload_small_file_from_path, validate_durable_content_reference,
    DurableContentValidationError, UploadError,
};
use loon_core::metadata::{
    MetadataState, RecursiveCopyPlanError, RecursivePutLocalEntry, RecursivePutPlanError,
    ResolvedVisiblePath, VisibleDirectorySubtree, VisiblePathError, VisiblePathMutationError,
    VisibleSubtreeError,
};
use loon_objectstore::keys::{
    content_manifest, namespace_head, namespace_lease, snapshot_manifest,
};
use loon_objectstore::ObjectStore;
use loon_types::{
    decode_content_manifest_json, ChangeSeq, ClientMutationOp, ClientMutationRequest,
    ControlObjectKind, HeadState, HeadStateEnvelope, InodeId, InodeKind, LeaseState,
    LeaseStateEnvelope, NamespaceId, ObservedRemoteInode, RevisionNo,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceBootstrapParams {
    pub holder_id: String,
    pub writer_version: String,
    pub now_ms: u64,
    pub lease_duration_ms: u64,
    pub allow_existing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrappedNamespace {
    pub namespace_id: NamespaceId,
    pub created: bool,
    pub head: HeadState,
    pub lease: LeaseState,
    pub checkpoint_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceCheckpointBasisSummary {
    pub checkpoint_seq: ChangeSeq,
    pub manifest_object_key: String,
    pub table_object_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceWalTailSummary {
    pub object_count: usize,
    pub first_seq: Option<ChangeSeq>,
    pub last_seq: Option<ChangeSeq>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceMetadataSummary {
    pub inode_count: usize,
    pub visible_inode_count: usize,
    pub direntry_count: usize,
    pub revision_count: usize,
    pub subtree_tombstone_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceStateSummary {
    pub namespace_id: NamespaceId,
    pub head: HeadState,
    pub lease: LeaseState,
    pub checkpoint_basis: Option<NamespaceCheckpointBasisSummary>,
    pub wal_tail: NamespaceWalTailSummary,
    pub metadata: NamespaceMetadataSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum NamespaceBootstrapError {
    #[error("holder id must not be empty")]
    EmptyHolderId,
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
    #[error("namespace control objects already exist for `{namespace_id}`")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is partially initialized")]
    NamespacePartiallyInitialized { namespace_id: NamespaceId },
    #[error(transparent)]
    LoadHead(#[from] ControlObjectLoadError),
    #[error("failed to load lease object: {0}")]
    LoadLease(ControlObjectLoadError),
    #[error("failed to validate existing namespace state: {0}")]
    ExistingNamespaceState(Box<NamespaceStateSummaryError>),
    #[error("failed to write head object: {0}")]
    HeadWrite(String),
    #[error("failed to write lease object: {0}")]
    LeaseWrite(String),
    #[error("failed to write checkpoint manifest `{object_key}`: {message}")]
    CheckpointManifestWrite { object_key: String, message: String },
    #[error("failed to write checkpoint segment `{object_key}`: {message}")]
    CheckpointSegmentWrite { object_key: String, message: String },
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("failed to load stored checkpoint for bootstrap: {0:?}")]
    CheckpointReplay(Box<CheckpointReplayError>),
    #[error("failed to prepare checkpoint: {0:?}")]
    CheckpointBuild(CheckpointBuildError),
    #[error("failed to prepare checkpoint head publish: {0:?}")]
    CheckpointPublish(CheckpointPublishError),
}

impl From<NamespaceStateSummaryError> for NamespaceBootstrapError {
    fn from(value: NamespaceStateSummaryError) -> Self {
        Self::ExistingNamespaceState(Box::new(value))
    }
}

impl From<CheckpointReplayError> for NamespaceBootstrapError {
    fn from(value: CheckpointReplayError) -> Self {
        Self::CheckpointReplay(Box::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum NamespaceStateSummaryError {
    #[error(transparent)]
    Head(#[from] ControlObjectLoadError),
    #[error("failed to load lease object: {0}")]
    Lease(ControlObjectLoadError),
    #[error(transparent)]
    Basis(Box<BasisLoadError>),
    #[error("failed to list checkpoint tables under `{prefix}`: {message}")]
    ListCheckpointTables { prefix: String, message: String },
    #[error("failed to read checkpoint manifest `{object_key}`: {message}")]
    ReadCheckpointManifest { object_key: String, message: String },
    #[error("missing checkpoint manifest `{object_key}`")]
    MissingCheckpointManifest { object_key: String },
    #[error("failed to decode checkpoint manifest: {0:?}")]
    CheckpointManifest(Box<CheckpointReplayError>),
    #[error("failed to list WAL objects under `{prefix}`: {message}")]
    ListWal { prefix: String, message: String },
    #[error("invalid WAL object key `{object_key}`")]
    InvalidWalObjectKey { object_key: String },
}

impl From<BasisLoadError> for NamespaceStateSummaryError {
    fn from(value: BasisLoadError) -> Self {
        Self::Basis(Box::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum RemoteObservationTranslationError {
    #[error(transparent)]
    Basis(Box<BasisLoadError>),
    #[error("failed to read content manifest `{object_key}`: {message}")]
    ReadManifest { object_key: String, message: String },
    #[error("missing content manifest `{object_key}`")]
    MissingManifest { object_key: String },
    #[error("failed to decode content manifest `{object_key}`: {message}")]
    DecodeManifest { object_key: String, message: String },
}

impl From<BasisLoadError> for RemoteObservationTranslationError {
    fn from(value: BasisLoadError) -> Self {
        Self::Basis(Box::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativePathEntry {
    pub namespace_id: NamespaceId,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub authoritative_head_seq: ChangeSeq,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub revision_no: Option<RevisionNo>,
    pub size_bytes: Option<u64>,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileBytes {
    pub entry: AuthoritativePathEntry,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativePutResult {
    pub entry: AuthoritativePathEntry,
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeRecursivePutResult {
    pub root_entry: AuthoritativePathEntry,
    pub committed_seq: ChangeSeq,
    pub directory_count: u64,
    pub file_count: u64,
    pub total_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeRecursiveCpResult {
    pub root_entry: AuthoritativePathEntry,
    pub committed_seq: ChangeSeq,
    pub directory_count: u64,
    pub file_count: u64,
    pub total_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeCpResult {
    pub entry: AuthoritativePathEntry,
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeSubtreeDirectoryEntry {
    pub relative_path: String,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub parent_inode_id: InodeId,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeSubtreeFileEntry {
    pub relative_path: String,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub parent_inode_id: InodeId,
    pub display_name: String,
    pub revision_no: RevisionNo,
    pub content_manifest_digest: String,
    pub size_bytes: u64,
    pub content_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeSubtreeDownload {
    pub root: AuthoritativePathEntry,
    #[serde(default)]
    pub directories: Vec<AuthoritativeSubtreeDirectoryEntry>,
    #[serde(default)]
    pub files: Vec<AuthoritativeSubtreeFileEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeMkdirResult {
    pub entry: AuthoritativePathEntry,
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeRmResult {
    pub namespace_id: NamespaceId,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeMvResult {
    pub namespace_id: NamespaceId,
    pub from_absolute_path: String,
    pub to_absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum AuthoritativePathReadError {
    #[error(transparent)]
    Basis(Box<BasisLoadError>),
    #[error(transparent)]
    VisiblePath(VisiblePathError),
    #[error("file command requires file at `{absolute_path}` (inode `{inode_id:?}` kind `{inode_kind:?}`)")]
    FileCommandRequiresFile {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[error(
        "directory command requires directory at `{absolute_path}` (inode `{inode_id:?}` kind `{inode_kind:?}`)"
    )]
    DirectoryCommandRequiresDirectory {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[error("visible file at `{absolute_path}` is missing its current revision head")]
    FileRevisionMissing {
        absolute_path: String,
        inode_id: InodeId,
    },
    #[error(
        "recursive subtree download does not support descendant kind `{inode_kind:?}` at `{absolute_path}` (inode `{inode_id:?}`)"
    )]
    UnsupportedSubtreeDescendant {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[error(transparent)]
    DurableContent(Box<DurableContentValidationError>),
    #[error("failed to read content manifest `{object_key}`: {message}")]
    ReadManifest { object_key: String, message: String },
    #[error("missing content manifest `{object_key}`")]
    MissingManifest { object_key: String },
    #[error("failed to decode content manifest `{object_key}`: {message}")]
    DecodeManifest { object_key: String, message: String },
}

impl From<BasisLoadError> for AuthoritativePathReadError {
    fn from(value: BasisLoadError) -> Self {
        Self::Basis(Box::new(value))
    }
}

impl From<VisiblePathError> for AuthoritativePathReadError {
    fn from(value: VisiblePathError) -> Self {
        Self::VisiblePath(value)
    }
}

impl From<DurableContentValidationError> for AuthoritativePathReadError {
    fn from(value: DurableContentValidationError) -> Self {
        Self::DurableContent(Box::new(value))
    }
}

#[derive(Debug, Error)]
pub enum AuthoritativePathWriteError {
    #[error(transparent)]
    Basis(Box<BasisLoadError>),
    #[error(transparent)]
    LeaseAcquire(Box<LeaseAcquireError>),
    #[error(transparent)]
    VisiblePath(VisiblePathError),
    #[error(transparent)]
    VisiblePathMutation(VisiblePathMutationError),
    #[error("authoritative remove requires `--recursive` for directory `{absolute_path}`")]
    DirectoryRequiresRecursive {
        absolute_path: String,
        inode_id: InodeId,
    },
    #[error(transparent)]
    Upload(Box<UploadError>),
    #[error(transparent)]
    DurableContent(Box<DurableContentValidationError>),
    #[error(transparent)]
    RecursivePutSource(Box<RecursivePutSourceError>),
    #[error(transparent)]
    RecursivePutPlan(Box<RecursivePutPlanError>),
    #[error(transparent)]
    RecursiveCopyPlan(Box<RecursiveCopyPlanError>),
    #[error(transparent)]
    PathRead(Box<AuthoritativePathReadError>),
    #[error(transparent)]
    Mutation(Box<ClientMutationExecutionError>),
}

impl From<BasisLoadError> for AuthoritativePathWriteError {
    fn from(value: BasisLoadError) -> Self {
        Self::Basis(Box::new(value))
    }
}

impl From<LeaseAcquireError> for AuthoritativePathWriteError {
    fn from(value: LeaseAcquireError) -> Self {
        Self::LeaseAcquire(Box::new(value))
    }
}

impl From<VisiblePathError> for AuthoritativePathWriteError {
    fn from(value: VisiblePathError) -> Self {
        Self::VisiblePath(value)
    }
}

impl From<VisiblePathMutationError> for AuthoritativePathWriteError {
    fn from(value: VisiblePathMutationError) -> Self {
        Self::VisiblePathMutation(value)
    }
}

impl From<UploadError> for AuthoritativePathWriteError {
    fn from(value: UploadError) -> Self {
        Self::Upload(Box::new(value))
    }
}

impl From<DurableContentValidationError> for AuthoritativePathWriteError {
    fn from(value: DurableContentValidationError) -> Self {
        Self::DurableContent(Box::new(value))
    }
}

impl From<RecursivePutSourceError> for AuthoritativePathWriteError {
    fn from(value: RecursivePutSourceError) -> Self {
        Self::RecursivePutSource(Box::new(value))
    }
}

impl From<RecursivePutPlanError> for AuthoritativePathWriteError {
    fn from(value: RecursivePutPlanError) -> Self {
        Self::RecursivePutPlan(Box::new(value))
    }
}

impl From<RecursiveCopyPlanError> for AuthoritativePathWriteError {
    fn from(value: RecursiveCopyPlanError) -> Self {
        Self::RecursiveCopyPlan(Box::new(value))
    }
}

impl From<AuthoritativePathReadError> for AuthoritativePathWriteError {
    fn from(value: AuthoritativePathReadError) -> Self {
        Self::PathRead(Box::new(value))
    }
}

impl From<ClientMutationExecutionError> for AuthoritativePathWriteError {
    fn from(value: ClientMutationExecutionError) -> Self {
        Self::Mutation(Box::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecursivePutSourceError {
    #[error("recursive upload source path does not exist: `{path}`")]
    MissingSource { path: String },
    #[error("recursive upload source must be a directory: `{path}`")]
    SourceNotDirectory { path: String },
    #[error("failed to inspect recursive upload source `{path}`: {message}")]
    ReadMetadata { path: String, message: String },
    #[error("failed to read recursive upload directory `{path}`: {message}")]
    ReadDirectory { path: String, message: String },
    #[error("recursive upload path component is not valid UTF-8: `{path}`")]
    InvalidUtf8PathComponent { path: String },
    #[error("recursive upload does not support local descendant kind `{kind}` at `{path}`")]
    UnsupportedLocalKind { path: String, kind: String },
    #[error("recursive upload local subtree contains duplicate relative path `{relative_path}`")]
    DuplicateRelativePath { relative_path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecursivePutSourceDirectory {
    relative_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecursivePutSourceFile {
    relative_path: String,
    source_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecursivePutSourceTree {
    directories: Vec<RecursivePutSourceDirectory>,
    files: Vec<RecursivePutSourceFile>,
}

pub fn bootstrap_namespace<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    params: &NamespaceBootstrapParams,
) -> Result<BootstrappedNamespace, NamespaceBootstrapError> {
    if params.holder_id.trim().is_empty() {
        return Err(NamespaceBootstrapError::EmptyHolderId);
    }
    if params.writer_version.trim().is_empty() {
        return Err(NamespaceBootstrapError::EmptyWriterVersion);
    }

    let head_key = namespace_head(namespace_id.as_str());
    let lease_key = namespace_lease(namespace_id.as_str());
    let existing_head = store
        .head(&head_key)
        .map_err(|err| NamespaceBootstrapError::HeadWrite(err.to_string()))?
        .is_some();
    let existing_lease = store
        .head(&lease_key)
        .map_err(|err| NamespaceBootstrapError::LeaseWrite(err.to_string()))?
        .is_some();

    match (existing_head, existing_lease) {
        (true, true) if params.allow_existing => {
            let summary = load_namespace_state_summary(store, namespace_id)?;
            let checkpoint_seq = summary.head.snapshot_hint_seq.unwrap_or(summary.head.seq);
            return Ok(BootstrappedNamespace {
                namespace_id: namespace_id.clone(),
                created: false,
                head: summary.head,
                lease: summary.lease,
                checkpoint_seq,
            });
        }
        (true, true) => {
            return Err(NamespaceBootstrapError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            });
        }
        (true, false) | (false, true) => {
            return Err(NamespaceBootstrapError::NamespacePartiallyInitialized {
                namespace_id: namespace_id.clone(),
            });
        }
        (false, false) => {}
    }

    let initial_head = HeadState::initial(namespace_id.clone());
    let initial_lease = LeaseState {
        namespace_id: namespace_id.clone(),
        holder_id: params.holder_id.clone(),
        fence_token: initial_head.active_fence_token,
        lease_expires_at_ms: params.now_ms.saturating_add(params.lease_duration_ms),
    };
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        &params.writer_version,
        initial_head.clone(),
    )
    .map_err(|err| NamespaceBootstrapError::HeadWrite(err.to_string()))?;
    let lease_envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        &params.writer_version,
        initial_lease.clone(),
    )
    .map_err(|err| NamespaceBootstrapError::LeaseWrite(err.to_string()))?;
    let head_bytes = serde_json::to_vec(&head_envelope)
        .map_err(|err| NamespaceBootstrapError::HeadWrite(err.to_string()))?;
    let lease_bytes = serde_json::to_vec(&lease_envelope)
        .map_err(|err| NamespaceBootstrapError::LeaseWrite(err.to_string()))?;

    let head_metadata = store
        .put_if_absent(&head_key, &head_bytes)
        .map_err(|err| NamespaceBootstrapError::HeadWrite(err.to_string()))?;
    store
        .put_if_absent(&lease_key, &lease_bytes)
        .map_err(|err| NamespaceBootstrapError::LeaseWrite(err.to_string()))?;

    let bootstrap_metadata_state = bootstrap_basis_metadata_state();
    let checkpoint = prepare_checkpoint(
        &initial_head,
        &bootstrap_metadata_state,
        &params.writer_version,
    )
    .map_err(NamespaceBootstrapError::CheckpointBuild)?;
    for segment in &checkpoint.segments {
        store
            .put_if_absent(&segment.object_key, &segment.encoded_bytes)
            .map_err(|err| NamespaceBootstrapError::CheckpointSegmentWrite {
                object_key: segment.object_key.clone(),
                message: err.to_string(),
            })?;
    }
    store
        .put_if_absent(
            &checkpoint.manifest.object_key,
            &checkpoint.manifest.encoded_bytes,
        )
        .map_err(|err| NamespaceBootstrapError::CheckpointManifestWrite {
            object_key: checkpoint.manifest.object_key.clone(),
            message: err.to_string(),
        })?;

    let loaded_checkpoint = load_checkpoint(
        namespace_id,
        &StoredCheckpointManifest {
            object_key: checkpoint.manifest.object_key.clone(),
            encoded_bytes: checkpoint.manifest.encoded_bytes.clone(),
        },
        &checkpoint
            .segments
            .iter()
            .map(|segment| StoredCheckpointSegment {
                object_key: segment.object_key.clone(),
                encoded_bytes: segment.encoded_bytes.clone(),
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| NamespaceBootstrapError::CheckpointReplay(Box::new(error)))?;
    let head_etag = head_metadata
        .etag
        .ok_or_else(|| NamespaceBootstrapError::MissingHeadEtag {
            object_key: head_key.clone(),
        })?;
    let prepared_head = prepare_checkpoint_head_publish(
        &initial_head,
        &loaded_checkpoint,
        &CheckpointHeadPublishRequest {
            requested_retention_floor_seq: None,
            retention_authorizers: None,
        },
        &params.writer_version,
    )
    .map_err(NamespaceBootstrapError::CheckpointPublish)?;
    publish_checkpoint_head(store, &head_etag, &prepared_head)
        .map_err(NamespaceBootstrapError::CheckpointPublish)?;

    Ok(BootstrappedNamespace {
        namespace_id: namespace_id.clone(),
        created: true,
        head: prepared_head.resulting_head,
        lease: initial_lease,
        checkpoint_seq: initial_head.seq,
    })
}

pub fn load_namespace_state_summary<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceStateSummary, NamespaceStateSummaryError> {
    let loaded_head = read_head_object(store, namespace_id)?;
    let loaded_lease =
        read_lease_object(store, namespace_id).map_err(NamespaceStateSummaryError::Lease)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;

    let checkpoint_basis = match loaded_head.envelope.state.snapshot_hint_seq {
        Some(snapshot_seq) => {
            let manifest = load_namespace_checkpoint_manifest(store, namespace_id, snapshot_seq)?;
            let table_prefix = format!(
                "namespaces/{}/snapshots/{:020}/tables/",
                namespace_id.as_str(),
                snapshot_seq.0
            );
            let table_object_count = store
                .list_prefix(&table_prefix)
                .map_err(|err| NamespaceStateSummaryError::ListCheckpointTables {
                    prefix: table_prefix.clone(),
                    message: err.to_string(),
                })?
                .len();
            Some(NamespaceCheckpointBasisSummary {
                checkpoint_seq: snapshot_seq,
                manifest_object_key: manifest.object_key,
                table_object_count,
            })
        }
        None => None,
    };

    let wal_prefix = format!("namespaces/{}/wal/", namespace_id.as_str());
    let wal_keys =
        store
            .list_prefix(&wal_prefix)
            .map_err(|err| NamespaceStateSummaryError::ListWal {
                prefix: wal_prefix.clone(),
                message: err.to_string(),
            })?;
    let wal_seqs = wal_keys
        .iter()
        .map(|key| wal_seq_from_key(&wal_prefix, key))
        .collect::<Result<Vec<_>, _>>()?;
    let cutoff = loaded_head
        .envelope
        .state
        .snapshot_hint_seq
        .unwrap_or(ChangeSeq(0));
    let tail_seqs = wal_seqs
        .into_iter()
        .filter(|seq| *seq > cutoff)
        .collect::<Vec<_>>();

    Ok(NamespaceStateSummary {
        namespace_id: namespace_id.clone(),
        head: basis.head.clone(),
        lease: loaded_lease.envelope.state,
        checkpoint_basis,
        wal_tail: NamespaceWalTailSummary {
            object_count: tail_seqs.len(),
            first_seq: tail_seqs.first().copied(),
            last_seq: tail_seqs.last().copied(),
        },
        metadata: NamespaceMetadataSummary {
            inode_count: basis.metadata_state.inodes.len(),
            visible_inode_count: basis
                .metadata_state
                .inodes
                .iter()
                .filter(|inode| {
                    basis
                        .metadata_state
                        .visible_inode(inode.inode_id, basis.head.seq)
                        .is_some()
                })
                .count(),
            direntry_count: basis.metadata_state.direntries.len(),
            revision_count: basis.metadata_state.revisions.len(),
            subtree_tombstone_count: basis.metadata_state.subtree_tombstones.len(),
        },
    })
}

pub fn translate_authoritative_state_to_remote_observations<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Vec<ObservedRemoteInode>, RemoteObservationTranslationError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let mut inode_ids = basis
        .metadata_state
        .inodes
        .iter()
        .map(|inode| inode.inode_id)
        .collect::<Vec<_>>();
    inode_ids.sort_by_key(|inode_id| inode_id.0);
    inode_ids.dedup_by_key(|inode_id| inode_id.0);

    let mut observed = Vec::new();
    for inode_id in inode_ids {
        let inode = basis
            .metadata_state
            .inode_at_seq(inode_id, basis.head.seq)
            .expect("metadata inode ids should resolve at head seq");
        let visible = basis
            .metadata_state
            .visible_inode(inode_id, basis.head.seq)
            .is_some();
        let current_binding = basis
            .metadata_state
            .current_parent_binding_for_child(inode_id, basis.head.seq);
        let revision = basis
            .metadata_state
            .latest_revision_head_at_seq(inode_id, basis.head.seq);
        let content_manifest_digest = revision
            .as_ref()
            .map(|revision| revision.content_manifest_digest.clone());
        let content_digest = match content_manifest_digest.as_deref() {
            Some(manifest_digest) => Some(
                load_file_content_summary_from_manifest(store, namespace_id, manifest_digest)?
                    .file_digest_sha256,
            ),
            None => None,
        };
        let revision_no = revision
            .as_ref()
            .map(|revision| revision.revision_no)
            .unwrap_or(RevisionNo(1));

        observed.push(ObservedRemoteInode {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: inode.inode_kind,
            observed_seq: basis.head.seq,
            revision_no,
            content_digest,
            content_manifest_digest,
            parent_inode_id: current_binding
                .as_ref()
                .map(|binding| binding.parent_inode_id),
            display_name: current_binding
                .as_ref()
                .map(|binding| binding.display_name.clone())
                .unwrap_or_default(),
            is_deleted: !visible,
        });
    }

    Ok(observed)
}

pub fn resolve_authoritative_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, AuthoritativePathReadError> {
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

pub fn list_authoritative_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<AuthoritativePathEntry>, AuthoritativePathReadError> {
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
        return Err(
            AuthoritativePathReadError::DirectoryCommandRequiresDirectory {
                absolute_path: resolved.absolute_path,
                inode_id: resolved.inode_id,
                inode_kind: resolved.inode_kind,
            },
        );
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

pub fn read_authoritative_file_bytes<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativeFileBytes, AuthoritativePathReadError> {
    let entry = resolve_authoritative_path(store, namespace_id, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(AuthoritativePathReadError::FileCommandRequiresFile {
            absolute_path: entry.absolute_path,
            inode_id: entry.inode_id,
            inode_kind: entry.inode_kind,
        });
    }
    let content_manifest_digest = entry.content_manifest_digest.clone().ok_or(
        AuthoritativePathReadError::FileRevisionMissing {
            absolute_path: entry.absolute_path.clone(),
            inode_id: entry.inode_id,
        },
    )?;
    let read = read_durable_content_bytes(store, namespace_id, &content_manifest_digest)?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

pub fn put_authoritative_file_from_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    source_path: &Path,
    absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativePutResult, AuthoritativePathWriteError> {
    let uploaded = upload_small_file_from_path(store, namespace_id, source_path)?;
    let validated =
        validate_durable_content_reference(store, namespace_id, &uploaded.content_manifest_digest)?;
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let target = basis
        .metadata_state
        .resolve_visible_create_target(absolute_path, basis.head.seq)?;
    let request = ClientMutationRequest {
        namespace_id: namespace_id.clone(),
        client_request_id: build_authoritative_request_id("put", &target.absolute_path, params),
        op: ClientMutationOp::CreateFile {
            parent_inode_id: target.parent_inode_id,
            display_name: target.display_name.clone(),
            content_manifest_digest: uploaded.content_manifest_digest.clone(),
        },
    };
    let executed =
        execute_client_mutation_against_basis(store, &request, params, basis, Some(validated))?;
    let entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &target.absolute_path,
    )?;
    Ok(AuthoritativePutResult {
        entry,
        committed_seq: executed.head_publish.resulting_head.seq,
    })
}

pub fn put_authoritative_subtree_from_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    source_path: &Path,
    absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativeRecursivePutResult, AuthoritativePathWriteError> {
    let source_tree = scan_recursive_put_source_tree(source_path)?;
    let mut local_entries = source_tree
        .directories
        .iter()
        .map(|directory| RecursivePutLocalEntry {
            relative_path: directory.relative_path.clone(),
            inode_kind: InodeKind::Dir,
            content_manifest_digest: None,
            size_bytes: None,
        })
        .collect::<Vec<_>>();
    let mut validated_invariants = Vec::new();
    let mut total_size_bytes = 0u64;
    for file in &source_tree.files {
        let uploaded = upload_small_file_from_path(store, namespace_id, &file.source_path)?;
        let validated = validate_durable_content_reference(
            store,
            namespace_id,
            &uploaded.content_manifest_digest,
        )?;
        total_size_bytes = total_size_bytes
            .checked_add(uploaded.file_size_bytes)
            .expect("recursive put total size should fit in u64");
        extend_unique_strings(&mut validated_invariants, &validated.checked_invariants);
        local_entries.push(RecursivePutLocalEntry {
            relative_path: file.relative_path.clone(),
            inode_kind: InodeKind::File,
            content_manifest_digest: Some(uploaded.content_manifest_digest),
            size_bytes: Some(uploaded.file_size_bytes),
        });
    }

    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let plan = basis.metadata_state.plan_recursive_put_subtree(
        absolute_path,
        basis.head.seq,
        &local_entries,
    )?;
    let commit_request = CommitRequest {
        namespace_id: namespace_id.clone(),
        request_id: build_authoritative_request_id(
            "put-recursive",
            &plan.target.absolute_path,
            params,
        ),
        writer_id: params.writer_id.clone(),
        writer_fence_token: basis.head.active_fence_token,
        planned_head_seq: basis.head.seq,
        ops: basis
            .metadata_state
            .build_recursive_put_commit_ops(&plan, basis.head.next_inode_id),
        preconditions: vec![Precondition::HeadSeqIs(basis.head.seq)],
    };
    let executed = execute_commit_request_against_basis(
        store,
        &commit_request,
        params,
        basis,
        &validated_invariants,
    )?;
    let root_entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &plan.target.absolute_path,
    )?;
    Ok(AuthoritativeRecursivePutResult {
        root_entry,
        committed_seq: executed.head_publish.resulting_head.seq,
        directory_count: u64::try_from(source_tree.directories.len())
            .expect("directory count should fit in u64"),
        file_count: u64::try_from(source_tree.files.len()).expect("file count should fit in u64"),
        total_size_bytes,
    })
}

pub fn replace_authoritative_file_from_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    source_path: &Path,
    absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativePutResult, AuthoritativePathWriteError> {
    let uploaded = upload_small_file_from_path(store, namespace_id, source_path)?;
    let validated =
        validate_durable_content_reference(store, namespace_id, &uploaded.content_manifest_digest)?;
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let target = basis
        .metadata_state
        .resolve_visible_file_target(absolute_path, basis.head.seq)?;
    let current_revision = basis
        .metadata_state
        .latest_revision_head_at_seq(target.inode_id, basis.head.seq)
        .ok_or_else(|| AuthoritativePathReadError::FileRevisionMissing {
            absolute_path: target.absolute_path.clone(),
            inode_id: target.inode_id,
        })?;
    let request = ClientMutationRequest {
        namespace_id: namespace_id.clone(),
        client_request_id: build_authoritative_request_id(
            "put-replace",
            &target.absolute_path,
            params,
        ),
        op: ClientMutationOp::ReplaceFile {
            inode_id: target.inode_id,
            base_revision_no: current_revision.revision_no,
            content_manifest_digest: uploaded.content_manifest_digest.clone(),
        },
    };
    let executed =
        execute_client_mutation_against_basis(store, &request, params, basis, Some(validated))?;
    let entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &target.absolute_path,
    )?;
    Ok(AuthoritativePutResult {
        entry,
        committed_seq: executed.head_publish.resulting_head.seq,
    })
}

pub fn cp_authoritative_subtree_within_namespace<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    from_absolute_path: &str,
    to_absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativeRecursiveCpResult, AuthoritativePathWriteError> {
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let plan = basis.metadata_state.plan_recursive_copy_subtree(
        from_absolute_path,
        to_absolute_path,
        basis.head.seq,
    )?;

    let mut validated_invariants = Vec::new();
    let mut validated_manifest_digests = BTreeSet::new();
    let mut total_size_bytes = 0u64;
    for file in &plan.files {
        let summary = load_file_content_summary_from_manifest(
            store,
            namespace_id,
            &file.content_manifest_digest,
        )
        .map_err(AuthoritativePathReadError::from)?;
        total_size_bytes = total_size_bytes
            .checked_add(summary.file_size_bytes)
            .expect("recursive copy total size should fit in u64");
        if validated_manifest_digests.insert(file.content_manifest_digest.clone()) {
            let validated = validate_durable_content_reference(
                store,
                namespace_id,
                &file.content_manifest_digest,
            )?;
            extend_unique_strings(&mut validated_invariants, &validated.checked_invariants);
        }
    }

    let commit_request = CommitRequest {
        namespace_id: namespace_id.clone(),
        request_id: build_authoritative_request_id(
            "cp-recursive",
            &plan.target.absolute_path,
            params,
        ),
        writer_id: params.writer_id.clone(),
        writer_fence_token: basis.head.active_fence_token,
        planned_head_seq: basis.head.seq,
        ops: basis
            .metadata_state
            .build_recursive_put_commit_ops(&plan, basis.head.next_inode_id),
        preconditions: vec![Precondition::HeadSeqIs(basis.head.seq)],
    };
    let executed = execute_commit_request_against_basis(
        store,
        &commit_request,
        params,
        basis,
        &validated_invariants,
    )?;
    let root_entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &plan.target.absolute_path,
    )?;

    Ok(AuthoritativeRecursiveCpResult {
        root_entry,
        committed_seq: executed.head_publish.resulting_head.seq,
        directory_count: u64::try_from(plan.directories.len())
            .expect("directory count should fit in u64"),
        file_count: u64::try_from(plan.files.len()).expect("file count should fit in u64"),
        total_size_bytes,
    })
}

pub fn cp_authoritative_file_within_namespace<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    from_absolute_path: &str,
    to_absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativeCpResult, AuthoritativePathWriteError> {
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    basis
        .metadata_state
        .ensure_distinct_visible_paths(from_absolute_path, to_absolute_path)?;
    let source = basis
        .metadata_state
        .resolve_visible_file_target(from_absolute_path, basis.head.seq)?;
    let target = basis
        .metadata_state
        .resolve_visible_create_target(to_absolute_path, basis.head.seq)?;
    let current_revision = basis
        .metadata_state
        .latest_revision_head_at_seq(source.inode_id, basis.head.seq)
        .ok_or_else(|| AuthoritativePathReadError::FileRevisionMissing {
            absolute_path: source.absolute_path.clone(),
            inode_id: source.inode_id,
        })?;
    let validated = validate_durable_content_reference(
        store,
        namespace_id,
        &current_revision.content_manifest_digest,
    )?;
    let request = ClientMutationRequest {
        namespace_id: namespace_id.clone(),
        client_request_id: build_authoritative_request_id("cp", &target.absolute_path, params),
        op: ClientMutationOp::CreateFile {
            parent_inode_id: target.parent_inode_id,
            display_name: target.display_name.clone(),
            content_manifest_digest: current_revision.content_manifest_digest,
        },
    };
    let executed =
        execute_client_mutation_against_basis(store, &request, params, basis, Some(validated))?;
    let entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &target.absolute_path,
    )?;
    Ok(AuthoritativeCpResult {
        entry,
        committed_seq: executed.head_publish.resulting_head.seq,
    })
}

pub fn cp_replace_authoritative_file_within_namespace<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    from_absolute_path: &str,
    to_absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativeCpResult, AuthoritativePathWriteError> {
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    basis
        .metadata_state
        .ensure_distinct_visible_paths(from_absolute_path, to_absolute_path)?;
    let source = basis
        .metadata_state
        .resolve_visible_file_target(from_absolute_path, basis.head.seq)?;
    let target = basis
        .metadata_state
        .resolve_visible_file_target(to_absolute_path, basis.head.seq)?;
    let source_revision = basis
        .metadata_state
        .latest_revision_head_at_seq(source.inode_id, basis.head.seq)
        .ok_or_else(|| AuthoritativePathReadError::FileRevisionMissing {
            absolute_path: source.absolute_path.clone(),
            inode_id: source.inode_id,
        })?;
    let target_revision = basis
        .metadata_state
        .latest_revision_head_at_seq(target.inode_id, basis.head.seq)
        .ok_or_else(|| AuthoritativePathReadError::FileRevisionMissing {
            absolute_path: target.absolute_path.clone(),
            inode_id: target.inode_id,
        })?;
    let validated = validate_durable_content_reference(
        store,
        namespace_id,
        &source_revision.content_manifest_digest,
    )?;
    let request = ClientMutationRequest {
        namespace_id: namespace_id.clone(),
        client_request_id: build_authoritative_request_id(
            "cp-replace",
            &target.absolute_path,
            params,
        ),
        op: ClientMutationOp::ReplaceFile {
            inode_id: target.inode_id,
            base_revision_no: target_revision.revision_no,
            content_manifest_digest: source_revision.content_manifest_digest,
        },
    };
    let executed =
        execute_client_mutation_against_basis(store, &request, params, basis, Some(validated))?;
    let entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &target.absolute_path,
    )?;
    Ok(AuthoritativeCpResult {
        entry,
        committed_seq: executed.head_publish.resulting_head.seq,
    })
}

pub fn collect_authoritative_subtree_download<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativeSubtreeDownload, AuthoritativePathReadError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let subtree = basis
        .metadata_state
        .collect_visible_directory_subtree(absolute_path, basis.head.seq)
        .map_err(map_visible_subtree_error)?;
    build_authoritative_subtree_download(
        store,
        namespace_id,
        basis.head.seq,
        &basis.metadata_state,
        subtree,
    )
}

pub fn mkdir_authoritative_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativeMkdirResult, AuthoritativePathWriteError> {
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let target = basis
        .metadata_state
        .resolve_visible_create_target(absolute_path, basis.head.seq)?;
    let request = ClientMutationRequest {
        namespace_id: namespace_id.clone(),
        client_request_id: build_authoritative_request_id("mkdir", &target.absolute_path, params),
        op: ClientMutationOp::CreateDir {
            parent_inode_id: target.parent_inode_id,
            display_name: target.display_name.clone(),
        },
    };
    let executed = execute_client_mutation_against_basis(store, &request, params, basis, None)?;
    let entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &target.absolute_path,
    )?;
    Ok(AuthoritativeMkdirResult {
        entry,
        committed_seq: executed.head_publish.resulting_head.seq,
    })
}

pub fn rm_authoritative_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    recursive: bool,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativeRmResult, AuthoritativePathWriteError> {
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let resolved = basis
        .metadata_state
        .resolve_visible_path(absolute_path, basis.head.seq)?;
    reject_root_path(&resolved.absolute_path)?;
    let request = match resolved.inode_kind {
        InodeKind::File => ClientMutationRequest {
            namespace_id: namespace_id.clone(),
            client_request_id: build_authoritative_request_id(
                "rm-file",
                &resolved.absolute_path,
                params,
            ),
            op: ClientMutationOp::DeleteFile {
                inode_id: resolved.inode_id,
            },
        },
        InodeKind::Dir => {
            if !recursive {
                return Err(AuthoritativePathWriteError::DirectoryRequiresRecursive {
                    absolute_path: resolved.absolute_path,
                    inode_id: resolved.inode_id,
                });
            }
            ClientMutationRequest {
                namespace_id: namespace_id.clone(),
                client_request_id: build_authoritative_request_id(
                    "rm-subtree",
                    &resolved.absolute_path,
                    params,
                ),
                op: ClientMutationOp::DeleteSubtree {
                    root_inode_id: resolved.inode_id,
                },
            }
        }
        _ => ClientMutationRequest {
            namespace_id: namespace_id.clone(),
            client_request_id: build_authoritative_request_id(
                "rm-file",
                &resolved.absolute_path,
                params,
            ),
            op: ClientMutationOp::DeleteFile {
                inode_id: resolved.inode_id,
            },
        },
    };
    let executed = execute_client_mutation_against_basis(store, &request, params, basis, None)?;
    Ok(AuthoritativeRmResult {
        namespace_id: namespace_id.clone(),
        absolute_path: resolved.absolute_path,
        inode_id: resolved.inode_id,
        inode_kind: resolved.inode_kind,
        committed_seq: executed.head_publish.resulting_head.seq,
    })
}

pub fn mv_authoritative_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    from_absolute_path: &str,
    to_absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> Result<AuthoritativeMvResult, AuthoritativePathWriteError> {
    crate::mutation::lease::acquire_or_renew_namespace_lease(store, namespace_id, params)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    basis
        .metadata_state
        .ensure_distinct_visible_paths(from_absolute_path, to_absolute_path)?;
    let source = basis
        .metadata_state
        .resolve_visible_path(from_absolute_path, basis.head.seq)?;
    reject_root_path(&source.absolute_path)?;
    let target = basis
        .metadata_state
        .resolve_visible_create_target(to_absolute_path, basis.head.seq)?;
    let request = ClientMutationRequest {
        namespace_id: namespace_id.clone(),
        client_request_id: build_authoritative_request_id("mv", &source.absolute_path, params),
        op: ClientMutationOp::Rename {
            inode_id: source.inode_id,
            new_parent_inode_id: target.parent_inode_id,
            new_display_name: target.display_name.clone(),
        },
    };
    let executed = execute_client_mutation_against_basis(store, &request, params, basis, None)?;
    let moved_entry = resolve_entry_from_metadata_state(
        store,
        namespace_id,
        executed.head_publish.resulting_head.seq,
        &executed.resulting_metadata_state,
        &target.absolute_path,
    )?;
    Ok(AuthoritativeMvResult {
        namespace_id: namespace_id.clone(),
        from_absolute_path: source.absolute_path,
        to_absolute_path: moved_entry.absolute_path,
        inode_id: moved_entry.inode_id,
        inode_kind: moved_entry.inode_kind,
        committed_seq: executed.head_publish.resulting_head.seq,
    })
}

fn resolve_entry_from_metadata_state<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, AuthoritativePathWriteError> {
    let resolved = metadata_state.resolve_visible_path(absolute_path, head_seq)?;
    build_authoritative_path_entry(store, namespace_id, head_seq, metadata_state, &resolved)
        .map_err(AuthoritativePathWriteError::from)
}

fn reject_root_path(absolute_path: &str) -> Result<(), AuthoritativePathWriteError> {
    if absolute_path == "/" {
        return Err(AuthoritativePathWriteError::VisiblePathMutation(
            VisiblePathMutationError::RootPathRejected {
                absolute_path: absolute_path.to_owned(),
            },
        ));
    }
    Ok(())
}

fn map_visible_subtree_error(error: VisibleSubtreeError) -> AuthoritativePathReadError {
    match error {
        VisibleSubtreeError::VisiblePath(error) => AuthoritativePathReadError::VisiblePath(error),
        VisibleSubtreeError::RootNotDirectory {
            absolute_path,
            inode_id,
            inode_kind,
        } => AuthoritativePathReadError::DirectoryCommandRequiresDirectory {
            absolute_path,
            inode_id,
            inode_kind,
        },
        VisibleSubtreeError::UnsupportedDescendant {
            absolute_path,
            inode_id,
            inode_kind,
        } => AuthoritativePathReadError::UnsupportedSubtreeDescendant {
            absolute_path,
            inode_id,
            inode_kind,
        },
        VisibleSubtreeError::FileRevisionMissing {
            absolute_path,
            inode_id,
        } => AuthoritativePathReadError::FileRevisionMissing {
            absolute_path,
            inode_id,
        },
    }
}

fn build_authoritative_request_id(
    operation: &str,
    absolute_path: &str,
    params: &ClientMutationExecutionParams,
) -> String {
    format!(
        "authoritative-{operation}-{}-{}",
        params.now_ms,
        absolute_path.trim_start_matches('/').replace('/', "_")
    )
}

fn scan_recursive_put_source_tree(
    source_path: &Path,
) -> Result<RecursivePutSourceTree, RecursivePutSourceError> {
    let metadata = fs::symlink_metadata(source_path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            RecursivePutSourceError::MissingSource {
                path: source_path.display().to_string(),
            }
        } else {
            RecursivePutSourceError::ReadMetadata {
                path: source_path.display().to_string(),
                message: err.to_string(),
            }
        }
    })?;
    if !metadata.is_dir() {
        return Err(RecursivePutSourceError::SourceNotDirectory {
            path: source_path.display().to_string(),
        });
    }

    let mut tree = RecursivePutSourceTree {
        directories: vec![RecursivePutSourceDirectory {
            relative_path: String::new(),
        }],
        files: Vec::new(),
    };
    let mut seen_relative_paths = std::collections::BTreeSet::from([String::new()]);
    scan_recursive_put_directory_children(source_path, "", &mut tree, &mut seen_relative_paths)?;
    Ok(tree)
}

fn scan_recursive_put_directory_children(
    directory_path: &Path,
    parent_relative_path: &str,
    tree: &mut RecursivePutSourceTree,
    seen_relative_paths: &mut std::collections::BTreeSet<String>,
) -> Result<(), RecursivePutSourceError> {
    let read_dir =
        fs::read_dir(directory_path).map_err(|err| RecursivePutSourceError::ReadDirectory {
            path: directory_path.display().to_string(),
            message: err.to_string(),
        })?;
    let mut entries = read_dir.collect::<Result<Vec<_>, _>>().map_err(|err| {
        RecursivePutSourceError::ReadDirectory {
            path: directory_path.display().to_string(),
            message: err.to_string(),
        }
    })?;
    entries.sort_by(|left, right| left.file_name().cmp(&right.file_name()));

    for entry in entries {
        let file_name = entry.file_name().into_string().map_err(|_| {
            RecursivePutSourceError::InvalidUtf8PathComponent {
                path: entry.path().display().to_string(),
            }
        })?;
        let relative_path = if parent_relative_path.is_empty() {
            file_name.clone()
        } else {
            format!("{parent_relative_path}/{file_name}")
        };
        if !seen_relative_paths.insert(relative_path.clone()) {
            return Err(RecursivePutSourceError::DuplicateRelativePath { relative_path });
        }

        let file_type = entry
            .file_type()
            .map_err(|err| RecursivePutSourceError::ReadMetadata {
                path: entry.path().display().to_string(),
                message: err.to_string(),
            })?;
        if file_type.is_symlink() {
            return Err(RecursivePutSourceError::UnsupportedLocalKind {
                path: entry.path().display().to_string(),
                kind: "symlink".to_owned(),
            });
        }
        if file_type.is_dir() {
            tree.directories.push(RecursivePutSourceDirectory {
                relative_path: relative_path.clone(),
            });
            scan_recursive_put_directory_children(
                &entry.path(),
                &relative_path,
                tree,
                seen_relative_paths,
            )?;
            continue;
        }
        if file_type.is_file() {
            tree.files.push(RecursivePutSourceFile {
                relative_path,
                source_path: entry.path(),
            });
            continue;
        }

        return Err(RecursivePutSourceError::UnsupportedLocalKind {
            path: entry.path().display().to_string(),
            kind: "special".to_owned(),
        });
    }

    Ok(())
}

fn extend_unique_strings(out: &mut Vec<String>, next: &[String]) {
    for value in next {
        if !out.iter().any(|existing| existing == value) {
            out.push(value.clone());
        }
    }
}

fn load_namespace_checkpoint_manifest<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    snapshot_seq: ChangeSeq,
) -> Result<StoredCheckpointManifest, NamespaceStateSummaryError> {
    let object_key = snapshot_manifest(namespace_id.as_str(), snapshot_seq.0);
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(|err| NamespaceStateSummaryError::ReadCheckpointManifest {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?
        .ok_or_else(|| NamespaceStateSummaryError::MissingCheckpointManifest {
            object_key: object_key.clone(),
        })?;
    load_checkpoint_manifest(
        namespace_id,
        &StoredCheckpointManifest {
            object_key: object_key.clone(),
            encoded_bytes: encoded_bytes.clone(),
        },
    )
    .map_err(|error| NamespaceStateSummaryError::CheckpointManifest(Box::new(error)))?;
    Ok(StoredCheckpointManifest {
        object_key,
        encoded_bytes,
    })
}

fn load_file_content_summary_from_manifest<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_digest: &str,
) -> Result<AuthoritativeFileContentSummary, ManifestSummaryLoadError> {
    let object_key = content_manifest(namespace_id.as_str(), manifest_digest);
    let manifest_bytes = store
        .get(&object_key, None)
        .map_err(|err| ManifestSummaryLoadError::ReadManifest {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?
        .ok_or_else(|| ManifestSummaryLoadError::MissingManifest {
            object_key: object_key.clone(),
        })?;
    let manifest = decode_content_manifest_json(&manifest_bytes).map_err(|err| {
        ManifestSummaryLoadError::DecodeManifest {
            object_key: object_key.clone(),
            message: err.to_string(),
        }
    })?;
    Ok(AuthoritativeFileContentSummary {
        file_size_bytes: manifest.payload.file_size_bytes,
        file_digest_sha256: manifest.payload.file_digest_sha256,
    })
}

fn build_authoritative_path_entry<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    resolved: &ResolvedVisiblePath,
) -> Result<AuthoritativePathEntry, AuthoritativePathReadError> {
    let revision = metadata_state.latest_revision_head_at_seq(resolved.inode_id, head_seq);
    let content_manifest_digest = revision
        .as_ref()
        .map(|revision| revision.content_manifest_digest.clone());
    let (size_bytes, content_digest) = match content_manifest_digest.as_deref() {
        Some(manifest_digest) => {
            let summary =
                load_file_content_summary_from_manifest(store, namespace_id, manifest_digest)
                    .map_err(AuthoritativePathReadError::from)?;
            (
                Some(summary.file_size_bytes),
                Some(summary.file_digest_sha256),
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

fn build_authoritative_subtree_download<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    subtree: VisibleDirectorySubtree,
) -> Result<AuthoritativeSubtreeDownload, AuthoritativePathReadError> {
    let root = build_authoritative_path_entry(
        store,
        namespace_id,
        head_seq,
        metadata_state,
        &subtree.root,
    )?;
    let directories = subtree
        .directories
        .into_iter()
        .map(|directory| AuthoritativeSubtreeDirectoryEntry {
            relative_path: directory.relative_path,
            absolute_path: directory.absolute_path,
            inode_id: directory.inode_id,
            parent_inode_id: directory.parent_inode_id,
            display_name: directory.display_name,
        })
        .collect::<Vec<_>>();
    let mut files = Vec::new();
    for file in subtree.files {
        let summary = load_file_content_summary_from_manifest(
            store,
            namespace_id,
            &file.content_manifest_digest,
        )?;
        files.push(AuthoritativeSubtreeFileEntry {
            relative_path: file.relative_path,
            absolute_path: file.absolute_path,
            inode_id: file.inode_id,
            parent_inode_id: file.parent_inode_id,
            display_name: file.display_name,
            revision_no: file.revision_no,
            content_manifest_digest: file.content_manifest_digest,
            size_bytes: summary.file_size_bytes,
            content_digest: summary.file_digest_sha256,
        });
    }

    Ok(AuthoritativeSubtreeDownload {
        root,
        directories,
        files,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthoritativeFileContentSummary {
    file_size_bytes: u64,
    file_digest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
enum ManifestSummaryLoadError {
    #[error("failed to read content manifest `{object_key}`: {message}")]
    ReadManifest { object_key: String, message: String },
    #[error("missing content manifest `{object_key}`")]
    MissingManifest { object_key: String },
    #[error("failed to decode content manifest `{object_key}`: {message}")]
    DecodeManifest { object_key: String, message: String },
}

impl From<ManifestSummaryLoadError> for RemoteObservationTranslationError {
    fn from(value: ManifestSummaryLoadError) -> Self {
        match value {
            ManifestSummaryLoadError::ReadManifest {
                object_key,
                message,
            } => Self::ReadManifest {
                object_key,
                message,
            },
            ManifestSummaryLoadError::MissingManifest { object_key } => {
                Self::MissingManifest { object_key }
            }
            ManifestSummaryLoadError::DecodeManifest {
                object_key,
                message,
            } => Self::DecodeManifest {
                object_key,
                message,
            },
        }
    }
}

impl From<ManifestSummaryLoadError> for AuthoritativePathReadError {
    fn from(value: ManifestSummaryLoadError) -> Self {
        match value {
            ManifestSummaryLoadError::ReadManifest {
                object_key,
                message,
            } => Self::ReadManifest {
                object_key,
                message,
            },
            ManifestSummaryLoadError::MissingManifest { object_key } => {
                Self::MissingManifest { object_key }
            }
            ManifestSummaryLoadError::DecodeManifest {
                object_key,
                message,
            } => Self::DecodeManifest {
                object_key,
                message,
            },
        }
    }
}

fn join_absolute_path(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
    }
}

fn wal_seq_from_key(
    prefix: &str,
    object_key: &str,
) -> Result<ChangeSeq, NamespaceStateSummaryError> {
    let suffix = object_key.strip_prefix(prefix).ok_or_else(|| {
        NamespaceStateSummaryError::InvalidWalObjectKey {
            object_key: object_key.to_owned(),
        }
    })?;
    let seq_part = suffix.split_once('-').map(|(seq, _)| seq).ok_or_else(|| {
        NamespaceStateSummaryError::InvalidWalObjectKey {
            object_key: object_key.to_owned(),
        }
    })?;
    let seq =
        seq_part
            .parse::<u64>()
            .map_err(|_| NamespaceStateSummaryError::InvalidWalObjectKey {
                object_key: object_key.to_owned(),
            })?;
    Ok(ChangeSeq(seq))
}

#[cfg(test)]
mod tests {
    use super::{
        bootstrap_namespace, collect_authoritative_subtree_download,
        cp_authoritative_file_within_namespace, cp_authoritative_subtree_within_namespace,
        cp_replace_authoritative_file_within_namespace, list_authoritative_path,
        load_namespace_state_summary, mkdir_authoritative_path, mv_authoritative_path,
        put_authoritative_file_from_path, put_authoritative_subtree_from_path,
        read_authoritative_file_bytes, replace_authoritative_file_from_path,
        resolve_authoritative_path, rm_authoritative_path,
        translate_authoritative_state_to_remote_observations, AuthoritativePathReadError,
        AuthoritativePathWriteError, NamespaceBootstrapError, NamespaceBootstrapParams,
    };
    use crate::genesis::bootstrap_basis_metadata_state;
    use crate::mutation::{execute_client_mutation, ClientMutationExecutionParams};
    use loon_core::checkpoint::prepare_checkpoint;
    use loon_core::metadata::{DirentryRecord, InodeRecord, MetadataState, RevisionRecord};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{blob, content_manifest, namespace_head, namespace_lease};
    use loon_objectstore::ObjectStore;
    use loon_types::{
        sha256_digest, ChangeSeq, ClientMutationOp, ClientMutationRequest, ContentBlockDescriptor,
        ContentManifestEnvelope, ContentManifestPayload, ControlObjectKind, FenceToken, HeadState,
        HeadStateEnvelope, InodeId, InodeKind, LeaseState, LeaseStateEnvelope, NamespaceId,
        RevisionNo,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn bootstrap_namespace_creates_checkpoint_backed_control_state() {
        let temp_dir = unique_temp_dir("server-ops-bootstrap");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        let bootstrapped = bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");

        assert!(bootstrapped.created);
        assert_eq!(bootstrapped.head.next_inode_id, InodeId(2));
        assert_eq!(bootstrapped.head.snapshot_hint_seq, Some(ChangeSeq(0)));

        let summary =
            load_namespace_state_summary(&store, &namespace_id).expect("load namespace summary");
        assert_eq!(summary.head.snapshot_hint_seq, Some(ChangeSeq(0)));
        assert_eq!(summary.metadata.inode_count, 1);
        assert_eq!(summary.metadata.visible_inode_count, 1);
        assert_eq!(summary.metadata.direntry_count, 0);
        assert_eq!(summary.metadata.revision_count, 0);
    }

    #[test]
    fn freshly_bootstrapped_namespace_translates_root_observation() {
        let temp_dir = unique_temp_dir("server-ops-bootstrap-root-translation");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");

        let observations =
            translate_authoritative_state_to_remote_observations(&store, &namespace_id)
                .expect("translate observations");
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].inode_id, InodeId(1));
        assert_eq!(observations[0].inode_kind, InodeKind::Dir);
        assert_eq!(observations[0].observed_seq, ChangeSeq(0));
        assert_eq!(observations[0].parent_inode_id, None);
        assert_eq!(observations[0].display_name, "");
        assert!(!observations[0].is_deleted);
    }

    #[test]
    fn bootstrap_namespace_fails_closed_by_default_when_namespace_exists() {
        let temp_dir = unique_temp_dir("server-ops-bootstrap-existing");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let params = NamespaceBootstrapParams {
            holder_id: "writer-a".to_owned(),
            writer_version: "loon-server-test".to_owned(),
            now_ms: 1_000,
            lease_duration_ms: 60_000,
            allow_existing: false,
        };

        bootstrap_namespace(&store, &namespace_id, &params).expect("bootstrap fresh namespace");

        let error = bootstrap_namespace(&store, &namespace_id, &params)
            .expect_err("second bootstrap should fail closed");
        assert!(matches!(
            error,
            NamespaceBootstrapError::NamespaceAlreadyExists { .. }
        ));

        let existing = bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                allow_existing: true,
                ..params
            },
        )
        .expect("bootstrap with allow_existing");
        assert!(!existing.created);
    }

    #[test]
    fn load_namespace_state_summary_reconstructs_from_checkpoint_and_wal() {
        let temp_dir = unique_temp_dir("server-ops-summary-from-wal");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(1),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(3),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(0),
            lease_expires_at_ms: 60_000,
        };
        let original = seed_head_and_lease(&store, &head, &lease);
        let replacement = write_uploaded_content(&store, &namespace_id, b"replaced content\n");

        let executed = execute_client_mutation(
            &store,
            &ClientMutationRequest {
                namespace_id: namespace_id.clone(),
                client_request_id: "request-1".to_owned(),
                op: ClientMutationOp::ReplaceFile {
                    inode_id: InodeId(2),
                    base_revision_no: RevisionNo(1),
                    content_manifest_digest: replacement.content_manifest_digest.clone(),
                },
            },
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
                lease_duration_ms: 60_000,
            },
        )
        .expect("execute replace-file mutation");

        let summary =
            load_namespace_state_summary(&store, &namespace_id).expect("load namespace summary");
        assert_eq!(summary.head.seq, ChangeSeq(2));
        assert_eq!(summary.head.snapshot_hint_seq, Some(ChangeSeq(1)));
        assert_eq!(summary.wal_tail.object_count, 1);
        assert_eq!(summary.wal_tail.first_seq, Some(ChangeSeq(2)));
        assert_eq!(summary.wal_tail.last_seq, Some(ChangeSeq(2)));
        assert_eq!(
            summary
                .checkpoint_basis
                .as_ref()
                .expect("checkpoint basis")
                .checkpoint_seq,
            ChangeSeq(1)
        );
        assert_eq!(summary.metadata.inode_count, 2);
        assert_eq!(summary.metadata.visible_inode_count, 2);
        assert_eq!(summary.metadata.revision_count, 2);
        assert_eq!(summary.head.seq, executed.response.committed_seq);
        assert_ne!(
            replacement.content_manifest_digest,
            original.content_manifest_digest
        );
    }

    #[test]
    fn translate_authoritative_state_to_remote_observations_is_deterministic() {
        let temp_dir = unique_temp_dir("server-ops-observations");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(1),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(3),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(0),
            lease_expires_at_ms: 60_000,
        };
        let uploaded = seed_head_and_lease(&store, &head, &lease);

        let observations =
            translate_authoritative_state_to_remote_observations(&store, &namespace_id)
                .expect("translate observations");
        assert!(
            observations.iter().any(|observation| {
                observation.display_name == "hello.txt"
                    && observation.content_digest.as_deref()
                        == Some(uploaded.file_digest_sha256.as_str())
            }),
            "expected file observation in translated authoritative state"
        );
    }

    #[test]
    fn resolve_authoritative_path_reads_root_and_nested_file() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-resolve");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let uploaded = write_uploaded_content(&store, &namespace_id, b"report bytes\n");
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(2),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(4),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = sample_lease(&namespace_id);
        seed_control_objects(&store, &head, &lease);
        seed_verified_basis(
            &store,
            &head,
            &MetadataState {
                inodes: vec![
                    InodeRecord {
                        inode_id: InodeId(1),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(0),
                    },
                    InodeRecord {
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(1),
                    },
                    InodeRecord {
                        inode_id: InodeId(3),
                        inode_kind: InodeKind::File,
                        created_seq: ChangeSeq(2),
                    },
                ],
                direntries: vec![
                    DirentryRecord {
                        parent_inode_id: InodeId(1),
                        name_key: "docs".to_owned(),
                        display_name: "docs".to_owned(),
                        child_inode_id: InodeId(2),
                        bind_seq: ChangeSeq(1),
                        bind_op_index: 0,
                    },
                    DirentryRecord {
                        parent_inode_id: InodeId(2),
                        name_key: "report.txt".to_owned(),
                        display_name: "report.txt".to_owned(),
                        child_inode_id: InodeId(3),
                        bind_seq: ChangeSeq(2),
                        bind_op_index: 0,
                    },
                ],
                revisions: vec![RevisionRecord {
                    inode_id: InodeId(3),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(2),
                    revision_op_index: 0,
                    content_manifest_digest: uploaded.content_manifest_digest.clone(),
                }],
                subtree_tombstones: Vec::new(),
            },
        );

        let root = resolve_authoritative_path(&store, &namespace_id, "/").expect("resolve root");
        assert_eq!(root.absolute_path, "/");
        assert_eq!(root.inode_id, InodeId(1));
        assert_eq!(root.inode_kind, InodeKind::Dir);

        let file =
            resolve_authoritative_path(&store, &namespace_id, "/docs/report.txt").expect("file");
        assert_eq!(file.absolute_path, "/docs/report.txt");
        assert_eq!(file.inode_id, InodeId(3));
        assert_eq!(file.inode_kind, InodeKind::File);
        assert_eq!(file.revision_no, Some(RevisionNo(1)));
        assert_eq!(file.size_bytes, Some(13));
        assert_eq!(file.content_digest, Some(uploaded.file_digest_sha256));
    }

    #[test]
    fn list_authoritative_path_lists_sorted_visible_children() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-list");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(2),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(5),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = sample_lease(&namespace_id);
        seed_control_objects(&store, &head, &lease);
        seed_verified_basis(
            &store,
            &head,
            &MetadataState {
                inodes: vec![
                    InodeRecord {
                        inode_id: InodeId(1),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(0),
                    },
                    InodeRecord {
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(1),
                    },
                    InodeRecord {
                        inode_id: InodeId(3),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(1),
                    },
                    InodeRecord {
                        inode_id: InodeId(4),
                        inode_kind: InodeKind::File,
                        created_seq: ChangeSeq(2),
                    },
                ],
                direntries: vec![
                    DirentryRecord {
                        parent_inode_id: InodeId(1),
                        name_key: "docs".to_owned(),
                        display_name: "docs".to_owned(),
                        child_inode_id: InodeId(2),
                        bind_seq: ChangeSeq(1),
                        bind_op_index: 0,
                    },
                    DirentryRecord {
                        parent_inode_id: InodeId(1),
                        name_key: "archive".to_owned(),
                        display_name: "archive".to_owned(),
                        child_inode_id: InodeId(3),
                        bind_seq: ChangeSeq(1),
                        bind_op_index: 0,
                    },
                    DirentryRecord {
                        parent_inode_id: InodeId(1),
                        name_key: "hello.txt".to_owned(),
                        display_name: "hello.txt".to_owned(),
                        child_inode_id: InodeId(4),
                        bind_seq: ChangeSeq(2),
                        bind_op_index: 0,
                    },
                ],
                revisions: Vec::new(),
                subtree_tombstones: Vec::new(),
            },
        );

        let listed = list_authoritative_path(&store, &namespace_id, "/").expect("list root");
        let names = listed
            .into_iter()
            .map(|entry| {
                format!(
                    "{}{}",
                    entry.display_name,
                    if entry.inode_kind == InodeKind::Dir {
                        "/"
                    } else {
                        ""
                    }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["archive/", "docs/", "hello.txt"]);
    }

    #[test]
    fn read_authoritative_file_bytes_returns_validated_bytes() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-read");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let uploaded = seed_head_and_lease(
            &store,
            &HeadState {
                namespace_id: namespace_id.clone(),
                seq: ChangeSeq(1),
                active_fence_token: FenceToken(0),
                next_inode_id: InodeId(3),
                snapshot_hint_seq: None,
                retention_floor_seq: ChangeSeq(0),
            },
            &sample_lease(&namespace_id),
        );

        let read =
            read_authoritative_file_bytes(&store, &namespace_id, "/hello.txt").expect("read file");
        assert_eq!(
            read.entry.content_manifest_digest,
            Some(uploaded.content_manifest_digest)
        );
        assert_eq!(read.bytes, b"hello from loon\n");
    }

    #[test]
    fn read_authoritative_file_bytes_rejects_directory_and_missing_blocks() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-read-errors");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(1),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(3),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = sample_lease(&namespace_id);
        seed_control_objects(&store, &head, &lease);

        let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes: 5,
            file_digest_sha256: "sha256:missing".to_owned(),
            block_size_bytes: 5,
            blocks: vec![ContentBlockDescriptor {
                content_digest_sha256: "sha256:missing-block".to_owned(),
                plaintext_size_bytes: 5,
            }],
        })
        .expect("build manifest");
        let manifest_bytes = serde_json::to_vec(&manifest).expect("serialize manifest");
        let missing_manifest_digest = sha256_digest(&manifest_bytes);
        store
            .put_if_absent(
                &content_manifest(namespace_id.as_str(), &missing_manifest_digest),
                &manifest_bytes,
            )
            .expect("write broken manifest");

        seed_verified_basis(
            &store,
            &head,
            &MetadataState {
                inodes: vec![
                    InodeRecord {
                        inode_id: InodeId(1),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(0),
                    },
                    InodeRecord {
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::File,
                        created_seq: ChangeSeq(1),
                    },
                ],
                direntries: vec![DirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "broken.txt".to_owned(),
                    display_name: "broken.txt".to_owned(),
                    child_inode_id: InodeId(2),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                }],
                revisions: vec![RevisionRecord {
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(1),
                    revision_op_index: 0,
                    content_manifest_digest: missing_manifest_digest,
                }],
                subtree_tombstones: Vec::new(),
            },
        );

        let directory_error =
            read_authoritative_file_bytes(&store, &namespace_id, "/").expect_err("dir should fail");
        assert!(matches!(
            directory_error,
            AuthoritativePathReadError::FileCommandRequiresFile { inode_id, .. } if inode_id == InodeId(1)
        ));

        let broken_error = read_authoritative_file_bytes(&store, &namespace_id, "/broken.txt")
            .expect_err("missing block should fail");
        assert!(matches!(
            broken_error,
            AuthoritativePathReadError::DurableContent(_)
        ));
    }

    #[test]
    fn authoritative_write_ops_create_move_and_delete_visible_entries() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-write");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");

        let mkdir = mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir authoritative path");
        assert_eq!(mkdir.entry.absolute_path, "/docs");

        let source_path = temp_dir.join("hello.txt");
        fs::write(&source_path, b"hello from write path\n").expect("write local source");
        let put = put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &source_path,
            "/docs/hello.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put authoritative file");
        assert_eq!(put.entry.absolute_path, "/docs/hello.txt");
        assert_eq!(put.entry.size_bytes, Some(22));

        let mv = mv_authoritative_path(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            "/docs/archive.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_300,
                lease_duration_ms: 60_000,
            },
        )
        .expect("move authoritative file");
        assert_eq!(mv.to_absolute_path, "/docs/archive.txt");

        let rm = rm_authoritative_path(
            &store,
            &namespace_id,
            "/docs/archive.txt",
            false,
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_400,
                lease_duration_ms: 60_000,
            },
        )
        .expect("remove authoritative file");
        assert_eq!(rm.absolute_path, "/docs/archive.txt");

        let listing = list_authoritative_path(&store, &namespace_id, "/docs").expect("list docs");
        assert!(listing.is_empty());
    }

    #[test]
    fn authoritative_put_replace_and_cp_update_visible_file_without_reuploading_copy() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-replace-copy");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");

        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir docs");

        let source_path = temp_dir.join("hello.txt");
        fs::write(&source_path, b"hello from write path\n").expect("write local source");
        let created = put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &source_path,
            "/docs/hello.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put authoritative file");
        assert_eq!(created.entry.revision_no, Some(RevisionNo(1)));

        let replace_path = temp_dir.join("hello-v2.txt");
        fs::write(&replace_path, b"hello from replace path\n").expect("write replace source");
        let replaced = replace_authoritative_file_from_path(
            &store,
            &namespace_id,
            &replace_path,
            "/docs/hello.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_300,
                lease_duration_ms: 60_000,
            },
        )
        .expect("replace authoritative file");
        assert_eq!(replaced.entry.absolute_path, "/docs/hello.txt");
        assert_eq!(replaced.entry.revision_no, Some(RevisionNo(2)));
        assert_eq!(replaced.entry.size_bytes, Some(24));

        let copied = cp_authoritative_file_within_namespace(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            "/docs/hello-copy.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_400,
                lease_duration_ms: 60_000,
            },
        )
        .expect("copy authoritative file");
        assert_eq!(copied.entry.absolute_path, "/docs/hello-copy.txt");
        assert_eq!(copied.entry.revision_no, Some(RevisionNo(1)));
        assert_ne!(copied.entry.inode_id, replaced.entry.inode_id);
        assert_eq!(
            copied.entry.content_manifest_digest,
            replaced.entry.content_manifest_digest
        );

        let source_bytes = read_authoritative_file_bytes(&store, &namespace_id, "/docs/hello.txt")
            .expect("read source bytes after replace");
        let copied_bytes =
            read_authoritative_file_bytes(&store, &namespace_id, "/docs/hello-copy.txt")
                .expect("read copied bytes");
        assert_eq!(source_bytes.bytes, b"hello from replace path\n");
        assert_eq!(copied_bytes.bytes, source_bytes.bytes);
    }

    #[test]
    fn authoritative_cp_replace_preserves_destination_inode_and_source_visibility() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-cp-replace");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir docs");

        let source_path = temp_dir.join("source.txt");
        fs::write(&source_path, b"source bytes\n").expect("write source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &source_path,
            "/docs/source.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put source");

        let target_path = temp_dir.join("target.txt");
        fs::write(&target_path, b"target bytes\n").expect("write target");
        let target_created = put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &target_path,
            "/docs/target.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_300,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put target");

        let replaced = cp_replace_authoritative_file_within_namespace(
            &store,
            &namespace_id,
            "/docs/source.txt",
            "/docs/target.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_400,
                lease_duration_ms: 60_000,
            },
        )
        .expect("copy replace target");
        assert_eq!(replaced.entry.inode_id, target_created.entry.inode_id);
        assert_eq!(replaced.entry.revision_no, Some(RevisionNo(2)));

        let source_bytes = read_authoritative_file_bytes(&store, &namespace_id, "/docs/source.txt")
            .expect("read source after replace");
        let target_bytes = read_authoritative_file_bytes(&store, &namespace_id, "/docs/target.txt")
            .expect("read target after replace");
        assert_eq!(source_bytes.bytes, b"source bytes\n");
        assert_eq!(target_bytes.bytes, source_bytes.bytes);
    }

    #[test]
    fn authoritative_recursive_cp_commits_nested_subtree_atomically() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-cp-recursive");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir docs");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs/drafts",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_150,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir drafts");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/archive",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_175,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir archive");

        let report_path = temp_dir.join("report.txt");
        fs::write(&report_path, b"report bytes\n").expect("write report source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &report_path,
            "/docs/report.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put report file");
        let note_path = temp_dir.join("note.txt");
        fs::write(&note_path, b"note bytes\n").expect("write note source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &note_path,
            "/docs/drafts/note.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_250,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put note file");

        let source_root = resolve_authoritative_path(&store, &namespace_id, "/docs")
            .expect("resolve source root");
        let source_report = resolve_authoritative_path(&store, &namespace_id, "/docs/report.txt")
            .expect("resolve source report");
        let copied = cp_authoritative_subtree_within_namespace(
            &store,
            &namespace_id,
            "/docs",
            "/archive/docs-copy",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_300,
                lease_duration_ms: 60_000,
            },
        )
        .expect("copy authoritative subtree");
        assert_eq!(copied.root_entry.absolute_path, "/archive/docs-copy");
        assert_eq!(copied.directory_count, 2);
        assert_eq!(copied.file_count, 2);
        assert_eq!(copied.total_size_bytes, 24);
        assert_ne!(copied.root_entry.inode_id, source_root.inode_id);

        let copied_report =
            resolve_authoritative_path(&store, &namespace_id, "/archive/docs-copy/report.txt")
                .expect("resolve copied report");
        assert_ne!(copied_report.inode_id, source_report.inode_id);
        assert_eq!(
            copied_report.content_manifest_digest,
            source_report.content_manifest_digest
        );

        let source_listing =
            list_authoritative_path(&store, &namespace_id, "/docs").expect("list source subtree");
        assert_eq!(
            source_listing
                .iter()
                .map(|entry| entry.absolute_path.as_str())
                .collect::<Vec<_>>(),
            vec!["/docs/drafts", "/docs/report.txt"]
        );
        let copied_listing = list_authoritative_path(&store, &namespace_id, "/archive/docs-copy")
            .expect("list copied subtree");
        assert_eq!(
            copied_listing
                .iter()
                .map(|entry| entry.absolute_path.as_str())
                .collect::<Vec<_>>(),
            vec!["/archive/docs-copy/drafts", "/archive/docs-copy/report.txt"]
        );
        let report_bytes =
            read_authoritative_file_bytes(&store, &namespace_id, "/archive/docs-copy/report.txt")
                .expect("read copied report");
        let note_bytes = read_authoritative_file_bytes(
            &store,
            &namespace_id,
            "/archive/docs-copy/drafts/note.txt",
        )
        .expect("read copied note");
        assert_eq!(report_bytes.bytes, b"report bytes\n");
        assert_eq!(note_bytes.bytes, b"note bytes\n");
    }

    #[test]
    fn authoritative_recursive_cp_supports_empty_directory_and_fails_closed_before_commit() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-cp-recursive-empty");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        for (now_ms, path) in [
            (1_100, "/empty-src"),
            (1_150, "/archive"),
            (1_200, "/archive/existing"),
        ] {
            mkdir_authoritative_path(
                &store,
                &namespace_id,
                path,
                &ClientMutationExecutionParams {
                    writer_id: "writer-a".to_owned(),
                    writer_version: "loon-server-test".to_owned(),
                    now_ms,
                    lease_duration_ms: 60_000,
                },
            )
            .expect("mkdir authoritative path");
        }

        let copied = cp_authoritative_subtree_within_namespace(
            &store,
            &namespace_id,
            "/empty-src",
            "/archive/empty-copy",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_250,
                lease_duration_ms: 60_000,
            },
        )
        .expect("copy empty subtree");
        assert_eq!(copied.root_entry.absolute_path, "/archive/empty-copy");
        assert_eq!(copied.directory_count, 1);
        assert_eq!(copied.file_count, 0);

        let occupied = cp_authoritative_subtree_within_namespace(
            &store,
            &namespace_id,
            "/empty-src",
            "/archive/existing",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_300,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("occupied recursive destination should fail");
        assert!(matches!(
            occupied,
            AuthoritativePathWriteError::RecursiveCopyPlan(error)
                if matches!(
                    error.as_ref(),
                    loon_core::metadata::RecursiveCopyPlanError::RecursivePutPlan(plan_error)
                        if matches!(
                            plan_error,
                            loon_core::metadata::RecursivePutPlanError::VisiblePathMutation(
                                loon_core::metadata::VisiblePathMutationError::DestinationOccupied { .. }
                            )
                        )
                )
        ));
        assert!(
            resolve_authoritative_path(&store, &namespace_id, "/archive/existing/report.txt")
                .is_err()
        );
    }

    #[test]
    fn authoritative_recursive_cp_rejects_active_foreign_holder() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-cp-recursive-foreign-holder");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir docs");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/archive",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_150,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir archive");

        let summary =
            load_namespace_state_summary(&store, &namespace_id).expect("load namespace summary");
        let foreign_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-b".to_owned(),
            fence_token: summary.head.active_fence_token,
            lease_expires_at_ms: 2_000,
        };
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-server-test",
            foreign_lease,
        )
        .expect("encode foreign lease");
        let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize foreign lease");
        store
            .put_overwrite(&namespace_lease(namespace_id.as_str()), &lease_bytes)
            .expect("overwrite lease with foreign holder");

        let error = cp_authoritative_subtree_within_namespace(
            &store,
            &namespace_id,
            "/docs",
            "/archive/docs-copy",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("active foreign holder should fail");
        assert!(matches!(
            error,
            AuthoritativePathWriteError::LeaseAcquire(error)
                if matches!(
                    error.as_ref(),
                    crate::mutation::lease::LeaseAcquireError::HeldByOtherWriter { .. }
                )
        ));
    }

    #[test]
    fn authoritative_recursive_put_commits_nested_subtree_atomically() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-put-recursive");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/imports",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir imports");

        let docs_root = temp_dir.join("docs");
        let drafts_root = docs_root.join("drafts");
        fs::create_dir_all(&drafts_root).expect("create local drafts");
        fs::write(docs_root.join("report.txt"), b"report bytes\n").expect("write report");
        fs::write(drafts_root.join("note.txt"), b"note bytes\n").expect("write note");

        let put = put_authoritative_subtree_from_path(
            &store,
            &namespace_id,
            &docs_root,
            "/imports/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("recursive authoritative put");
        assert_eq!(put.root_entry.absolute_path, "/imports/docs");
        assert_eq!(put.directory_count, 2);
        assert_eq!(put.file_count, 2);
        assert_eq!(put.total_size_bytes, 24);

        let listing = list_authoritative_path(&store, &namespace_id, "/imports/docs")
            .expect("list uploaded root");
        assert_eq!(
            listing
                .iter()
                .map(|entry| entry.absolute_path.as_str())
                .collect::<Vec<_>>(),
            vec!["/imports/docs/drafts", "/imports/docs/report.txt"]
        );
        let report_bytes =
            read_authoritative_file_bytes(&store, &namespace_id, "/imports/docs/report.txt")
                .expect("read uploaded report");
        let note_bytes =
            read_authoritative_file_bytes(&store, &namespace_id, "/imports/docs/drafts/note.txt")
                .expect("read uploaded note");
        assert_eq!(report_bytes.bytes, b"report bytes\n");
        assert_eq!(note_bytes.bytes, b"note bytes\n");
    }

    #[test]
    fn authoritative_recursive_put_supports_empty_directory_and_fails_closed_before_commit() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-put-recursive-empty");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/imports",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir imports");

        let empty_root = temp_dir.join("empty");
        fs::create_dir_all(&empty_root).expect("create empty root");
        let created = put_authoritative_subtree_from_path(
            &store,
            &namespace_id,
            &empty_root,
            "/imports/empty",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put empty directory");
        assert_eq!(created.root_entry.absolute_path, "/imports/empty");
        assert_eq!(created.directory_count, 1);
        assert_eq!(created.file_count, 0);

        let docs_root = temp_dir.join("docs");
        fs::create_dir_all(&docs_root).expect("create docs root");
        let occupied = put_authoritative_subtree_from_path(
            &store,
            &namespace_id,
            &docs_root,
            "/imports/empty",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_300,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("occupied recursive target should fail");
        assert!(matches!(
            occupied,
            AuthoritativePathWriteError::RecursivePutPlan(error)
                if matches!(
                    error.as_ref(),
                    super::RecursivePutPlanError::VisiblePathMutation(
                        loon_core::metadata::VisiblePathMutationError::DestinationOccupied { .. }
                    )
                )
        ));
        assert!(resolve_authoritative_path(&store, &namespace_id, "/imports/docs").is_err());
    }

    #[test]
    fn authoritative_recursive_put_rejects_active_foreign_holder() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-put-recursive-foreign-holder");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/imports",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir imports");

        let summary =
            load_namespace_state_summary(&store, &namespace_id).expect("load namespace summary");
        let foreign_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-b".to_owned(),
            fence_token: summary.head.active_fence_token,
            lease_expires_at_ms: 2_000,
        };
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-server-test",
            foreign_lease,
        )
        .expect("encode foreign lease");
        let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize foreign lease");
        store
            .put_overwrite(&namespace_lease(namespace_id.as_str()), &lease_bytes)
            .expect("overwrite lease with foreign holder");

        let docs_root = temp_dir.join("docs");
        fs::create_dir_all(&docs_root).expect("create docs root");
        let error = put_authoritative_subtree_from_path(
            &store,
            &namespace_id,
            &docs_root,
            "/imports/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("active foreign lease holder should fail");
        assert!(matches!(
            error,
            AuthoritativePathWriteError::LeaseAcquire(error)
                if matches!(
                    error.as_ref(),
                    crate::mutation::lease::LeaseAcquireError::HeldByOtherWriter { .. }
                )
        ));
    }

    #[test]
    fn authoritative_rm_rejects_directory_without_recursive() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-rm-dir");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir authoritative path");

        let error = rm_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            false,
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("directory remove without recursive should fail");
        assert!(matches!(
            error,
            AuthoritativePathWriteError::DirectoryRequiresRecursive { .. }
        ));
    }

    #[test]
    fn authoritative_copy_rejects_active_foreign_holder() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-copy-foreign-holder");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir docs");

        let source_path = temp_dir.join("hello.txt");
        fs::write(&source_path, b"hello from write path\n").expect("write local source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &source_path,
            "/docs/hello.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put authoritative file");

        let summary =
            load_namespace_state_summary(&store, &namespace_id).expect("load namespace summary");
        let foreign_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-b".to_owned(),
            fence_token: summary.head.active_fence_token,
            lease_expires_at_ms: 2_000,
        };
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-server-test",
            foreign_lease,
        )
        .expect("encode foreign lease");
        let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize foreign lease");
        store
            .put_overwrite(&namespace_lease(namespace_id.as_str()), &lease_bytes)
            .expect("overwrite lease with foreign holder");

        let error = cp_authoritative_file_within_namespace(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            "/docs/hello-copy.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("active foreign lease holder should fail");
        assert!(matches!(
            error,
            AuthoritativePathWriteError::LeaseAcquire(error)
                if matches!(
                    error.as_ref(),
                    crate::mutation::lease::LeaseAcquireError::HeldByOtherWriter { .. }
                )
        ));
    }

    #[test]
    fn authoritative_cp_replace_rejects_active_foreign_holder() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-copy-replace-foreign-holder");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir docs");

        let source_path = temp_dir.join("hello.txt");
        fs::write(&source_path, b"hello from write path\n").expect("write local source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &source_path,
            "/docs/hello.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put authoritative file");
        let target_path = temp_dir.join("target.txt");
        fs::write(&target_path, b"target\n").expect("write target source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &target_path,
            "/docs/target.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_300,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put target file");

        let summary =
            load_namespace_state_summary(&store, &namespace_id).expect("load namespace summary");
        let foreign_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-b".to_owned(),
            fence_token: summary.head.active_fence_token,
            lease_expires_at_ms: 2_000,
        };
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-server-test",
            foreign_lease,
        )
        .expect("encode foreign lease");
        let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize foreign lease");
        store
            .put_overwrite(&namespace_lease(namespace_id.as_str()), &lease_bytes)
            .expect("overwrite lease with foreign holder");

        let error = cp_replace_authoritative_file_within_namespace(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            "/docs/target.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("active foreign lease holder should fail");
        assert!(matches!(
            error,
            AuthoritativePathWriteError::LeaseAcquire(error)
                if matches!(
                    error.as_ref(),
                    crate::mutation::lease::LeaseAcquireError::HeldByOtherWriter { .. }
                )
        ));
    }

    #[test]
    fn collect_authoritative_subtree_download_returns_deterministic_entries() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-subtree-download");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");

        bootstrap_namespace(
            &store,
            &namespace_id,
            &NamespaceBootstrapParams {
                holder_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_000,
                lease_duration_ms: 60_000,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_100,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir docs");
        mkdir_authoritative_path(
            &store,
            &namespace_id,
            "/docs/drafts",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_150,
                lease_duration_ms: 60_000,
            },
        )
        .expect("mkdir drafts");

        let report_path = temp_dir.join("report.txt");
        fs::write(&report_path, b"report bytes\n").expect("write report source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &report_path,
            "/docs/report.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_200,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put report file");

        let note_path = temp_dir.join("note.txt");
        fs::write(&note_path, b"note bytes\n").expect("write note source");
        put_authoritative_file_from_path(
            &store,
            &namespace_id,
            &note_path,
            "/docs/drafts/note.txt",
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_250,
                lease_duration_ms: 60_000,
            },
        )
        .expect("put note file");

        let subtree = collect_authoritative_subtree_download(&store, &namespace_id, "/docs")
            .expect("collect subtree");
        assert_eq!(subtree.root.absolute_path, "/docs");
        assert_eq!(
            subtree
                .directories
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["drafts"]
        );
        assert_eq!(
            subtree
                .files
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["drafts/note.txt", "report.txt"]
        );
    }

    #[test]
    fn collect_authoritative_subtree_download_rejects_unsupported_descendants() {
        let temp_dir = unique_temp_dir("server-ops-authoritative-subtree-unsupported");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(2),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(4),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = sample_lease(&namespace_id);
        seed_control_objects(&store, &head, &lease);
        seed_verified_basis(
            &store,
            &head,
            &MetadataState {
                inodes: vec![
                    InodeRecord {
                        inode_id: InodeId(1),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(0),
                    },
                    InodeRecord {
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(1),
                    },
                    InodeRecord {
                        inode_id: InodeId(3),
                        inode_kind: InodeKind::Symlink,
                        created_seq: ChangeSeq(2),
                    },
                ],
                direntries: vec![
                    DirentryRecord {
                        parent_inode_id: InodeId(1),
                        name_key: "docs".to_owned(),
                        display_name: "docs".to_owned(),
                        child_inode_id: InodeId(2),
                        bind_seq: ChangeSeq(1),
                        bind_op_index: 0,
                    },
                    DirentryRecord {
                        parent_inode_id: InodeId(2),
                        name_key: "shortcut".to_owned(),
                        display_name: "shortcut".to_owned(),
                        child_inode_id: InodeId(3),
                        bind_seq: ChangeSeq(2),
                        bind_op_index: 0,
                    },
                ],
                revisions: Vec::new(),
                subtree_tombstones: Vec::new(),
            },
        );

        let error = collect_authoritative_subtree_download(&store, &namespace_id, "/docs")
            .expect_err("unsupported descendant should fail");
        assert!(matches!(
            error,
            AuthoritativePathReadError::UnsupportedSubtreeDescendant {
                absolute_path,
                inode_id,
                inode_kind,
            } if absolute_path == "/docs/shortcut"
                && inode_id == InodeId(3)
                && inode_kind == InodeKind::Symlink
        ));
    }

    fn write_uploaded_content(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> UploadedContent {
        let file_digest_sha256 = sha256_digest(bytes);
        let block_digest = sha256_digest(bytes);
        store
            .put_if_absent(&blob(namespace_id.as_str(), &block_digest), bytes)
            .expect("write content block");
        let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes: bytes.len() as u64,
            file_digest_sha256: file_digest_sha256.clone(),
            block_size_bytes: bytes.len() as u64,
            blocks: vec![ContentBlockDescriptor {
                content_digest_sha256: block_digest,
                plaintext_size_bytes: bytes.len() as u64,
            }],
        })
        .expect("build manifest");
        let manifest_bytes = serde_json::to_vec(&manifest).expect("serialize manifest");
        let content_manifest_digest = sha256_digest(&manifest_bytes);
        store
            .put_if_absent(
                &content_manifest(namespace_id.as_str(), &content_manifest_digest),
                &manifest_bytes,
            )
            .expect("write manifest");
        UploadedContent {
            namespace_id: namespace_id.clone(),
            file_digest_sha256,
            content_manifest_digest,
        }
    }

    fn seed_head_and_lease(
        store: &LocalFsStore,
        head: &HeadState,
        lease: &LeaseState,
    ) -> UploadedContent {
        seed_control_objects(store, head, lease);

        let uploaded = write_uploaded_content(store, &head.namespace_id, b"hello from loon\n");
        let mut metadata_state = bootstrap_basis_metadata_state();
        metadata_state.inodes.push(InodeRecord {
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(1),
        });
        metadata_state.direntries.push(DirentryRecord {
            parent_inode_id: InodeId(1),
            name_key: "hello.txt".to_owned(),
            display_name: "hello.txt".to_owned(),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(1),
            bind_op_index: 0,
        });
        metadata_state.revisions.push(RevisionRecord {
            inode_id: InodeId(2),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(1),
            revision_op_index: 0,
            content_manifest_digest: uploaded.content_manifest_digest.clone(),
        });
        seed_verified_basis(store, head, &metadata_state);
        uploaded
    }

    fn seed_control_objects(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
        let head_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "loon-server-test",
            head.clone(),
        )
        .expect("encode head envelope");
        let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize head envelope");
        store
            .put_if_absent(&namespace_head(head.namespace_id.as_str()), &head_bytes)
            .expect("seed head object");

        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-server-test",
            lease.clone(),
        )
        .expect("encode lease envelope");
        let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize lease envelope");
        store
            .put_if_absent(&namespace_lease(lease.namespace_id.as_str()), &lease_bytes)
            .expect("seed lease object");
    }

    fn seed_verified_basis(store: &LocalFsStore, head: &HeadState, metadata_state: &MetadataState) {
        let basis_head = HeadState {
            snapshot_hint_seq: Some(head.seq),
            ..head.clone()
        };
        let checkpoint = prepare_checkpoint(&basis_head, metadata_state, "loon-server-test")
            .expect("prepare checkpoint");
        store
            .put_overwrite(
                &checkpoint.manifest.object_key,
                &checkpoint.manifest.encoded_bytes,
            )
            .expect("seed checkpoint manifest");
        for segment in &checkpoint.segments {
            store
                .put_overwrite(&segment.object_key, &segment.encoded_bytes)
                .expect("seed checkpoint segment");
        }

        let head_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "loon-server-test",
            basis_head,
        )
        .expect("encode basis head envelope");
        let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize basis head envelope");
        store
            .put_overwrite(&namespace_head(head.namespace_id.as_str()), &head_bytes)
            .expect("overwrite head with checkpoint-backed basis");
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UploadedContent {
        namespace_id: NamespaceId,
        file_digest_sha256: String,
        content_manifest_digest: String,
    }

    fn sample_lease(namespace_id: &NamespaceId) -> LeaseState {
        LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(0),
            lease_expires_at_ms: 60_000,
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loondb-server-ops-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
