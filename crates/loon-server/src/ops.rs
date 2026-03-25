use crate::genesis::bootstrap_basis_metadata_state;
use crate::mutation::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::mutation::loading::{read_head_object, read_lease_object, ControlObjectLoadError};
use loon_core::checkpoint::{
    load_checkpoint, load_checkpoint_manifest, prepare_checkpoint, prepare_checkpoint_head_publish,
    publish_checkpoint_head, CheckpointBuildError, CheckpointHeadPublishRequest,
    CheckpointPublishError, CheckpointReplayError, StoredCheckpointManifest,
    StoredCheckpointSegment,
};
use loon_objectstore::keys::{
    content_manifest, namespace_head, namespace_lease, snapshot_manifest,
};
use loon_objectstore::ObjectStore;
use loon_types::{
    decode_content_manifest_json, ChangeSeq, ControlObjectKind, HeadState, HeadStateEnvelope,
    LeaseState, LeaseStateEnvelope, NamespaceId, ObservedRemoteInode, RevisionNo,
};
use serde::{Deserialize, Serialize};
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
            Some(manifest_digest) => Some(load_file_digest_from_manifest(
                store,
                namespace_id,
                manifest_digest,
            )?),
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

fn load_file_digest_from_manifest<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_digest: &str,
) -> Result<String, RemoteObservationTranslationError> {
    let object_key = content_manifest(namespace_id.as_str(), manifest_digest);
    let manifest_bytes = store
        .get(&object_key, None)
        .map_err(|err| RemoteObservationTranslationError::ReadManifest {
            object_key: object_key.clone(),
            message: err.to_string(),
        })?
        .ok_or_else(|| RemoteObservationTranslationError::MissingManifest {
            object_key: object_key.clone(),
        })?;
    let manifest = decode_content_manifest_json(&manifest_bytes).map_err(|err| {
        RemoteObservationTranslationError::DecodeManifest {
            object_key: object_key.clone(),
            message: err.to_string(),
        }
    })?;
    Ok(manifest.payload.file_digest_sha256)
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
        bootstrap_namespace, load_namespace_state_summary,
        translate_authoritative_state_to_remote_observations, NamespaceBootstrapError,
        NamespaceBootstrapParams,
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
