//! Installs manifest 1 after the content descriptor and discovery hint.

use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::checkpoint::record::write_checkpoint_record_if_absent;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::metadata::{AccessRevisionRecord, InodeRecord, MetadataState};
use crate::namespace::control::{
    load_current_manifest, load_current_manifest_if_present, load_discovered_manifest,
};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, CheckpointOwner, CheckpointRecordState, ContentStoreState,
    ControlObjectKind, HintState,
};
use loonfs_api::wire::manifest::{
    encode_namespace_manifest_json, NamespaceAccess, NamespaceManifestPayload,
};
use loonfs_api::{
    AccessRevisionNo, ActorId, ChangeSeq, CheckpointId, ContentStoreId, ErrorCode, InodeKind,
    ManifestNo, Namespace, NamespaceId, WalNo, ROOT_INODE_ID,
};
use loonfs_objectstore::keys::{content_store, hint, metadata_manifest_object};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum BootstrapNamespaceError {
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error(transparent)]
    Core(#[from] CoreError),
}

impl BootstrapNamespaceError {
    /// Returns the stable machine-readable reason for this error.
    ///
    /// This is the single source of truth for the wire code every surface
    /// (HTTP server, CLI) reports for a bootstrap failure, mirroring
    /// [`CoreError::code`](crate::Error::code).
    pub fn code(&self) -> ErrorCode {
        match self {
            BootstrapNamespaceError::NamespaceAlreadyExists { .. } => ErrorCode::NamespaceExists,
            BootstrapNamespaceError::Core(error) => error.code(),
        }
    }

    /// Returns the structured context the code's consumers report beside it,
    /// mirroring [`CoreError::details`](crate::Error::details): only the
    /// wrapped core failure carries any.
    pub fn details(&self) -> Option<loonfs_api::ErrorDetails> {
        match self {
            BootstrapNamespaceError::Core(error) => error.details(),
            BootstrapNamespaceError::NamespaceAlreadyExists { .. } => None,
        }
    }

    /// Returns a safe message when bootstrap failed in the object store.
    pub fn object_store_public_message(&self) -> Option<std::borrow::Cow<'static, str>> {
        match self {
            BootstrapNamespaceError::Core(error) => error.object_store_public_message(),
            _ => None,
        }
    }
}

pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    actor_id: &loonfs_api::ActorId,
    access: &NamespaceAccess,
    allow_existing: bool,
) -> Result<Namespace, BootstrapNamespaceError> {
    let manifest = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        ContentStoreId::generate(),
        context.now_ms,
        actor_id.clone(),
        access.clone(),
    );
    match install_namespace_manifest(store, &manifest, || Ok(())).await? {
        NamespaceInstall::Landed => {}
        NamespaceInstall::Exists if allow_existing => {}
        NamespaceInstall::Exists => {
            return Err(BootstrapNamespaceError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            })
        }
        NamespaceInstall::Deleted => {
            return recreate_namespace(
                store,
                namespace_id,
                context,
                actor_id,
                access,
                allow_existing,
            )
            .await
        }
    }
    super::status::load_namespace(store, namespace_id)
        .await
        .map_err(Into::into)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NamespaceInstall {
    Landed,
    Exists,
    Deleted,
}

pub(super) async fn install_namespace_manifest<
    S: ObjectStore + ?Sized,
    F: Fn() -> Result<(), CoreError>,
>(
    store: &S,
    manifest: &NamespaceManifestPayload,
    validate_install: F,
) -> Result<NamespaceInstall, CoreError> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let namespace_id = &manifest.namespace_id;
    if let Some(current) = load_current_manifest_if_present(store, namespace_id).await? {
        return Ok(if current.envelope.payload().status.is_deleted() {
            NamespaceInstall::Deleted
        } else {
            NamespaceInstall::Exists
        });
    }
    validate_install()?;
    write_content_store_descriptor(store, &manifest.content_store_id, manifest.created_at_ms)
        .await?;
    let hint_key = hint(namespace_id);
    let hint = HintState {
        namespace_id: namespace_id.clone(),
        manifest_no: ManifestNo(1),
        wal_no: WalNo(0),
    };
    let hint_bytes =
        encode_control_state(ControlObjectKind::Hint, &hint).map_err(|error| CoreError::Codec {
            object_key: hint_key.clone(),
            message: error.to_string(),
        })?;
    let manifest_key = metadata_manifest_object(namespace_id, &ManifestNo(1));
    let bytes = encode_namespace_manifest_json(manifest.clone())
        .map_err(|error| CoreError::Codec {
            object_key: manifest_key.clone(),
            message: error.to_string(),
        })?
        .into_bytes();
    match store
        .put_if_absent(&hint_key, Bytes::from(hint_bytes))
        .await
    {
        Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => {}
        Err(error) => return Err(CoreError::store(&hint_key, &error)),
    }
    crate::checkpoint::ensure_metadata_publication_budget(&timer, started_ms, namespace_id)?;
    validate_install()?;
    match store.put_if_absent(&manifest_key, Bytes::from(bytes)).await {
        Ok(_) => Ok(NamespaceInstall::Landed),
        Err(
            error @ (ObjectStoreError::PreconditionFailed { .. }
            | ObjectStoreError::Transport { .. }),
        ) => {
            let Some(installed) =
                load_discovered_manifest(store, namespace_id, ManifestNo(1)).await?
            else {
                return Err(CoreError::store(&manifest_key, &error));
            };
            let current = load_current_manifest(store, namespace_id).await?;
            if current.envelope.payload().status.is_deleted() {
                return Ok(NamespaceInstall::Deleted);
            }
            if matches!(error, ObjectStoreError::Transport { .. })
                && installed.envelope.payload() == manifest
            {
                Ok(NamespaceInstall::Landed)
            } else {
                Ok(NamespaceInstall::Exists)
            }
        }
        Err(error) => Err(CoreError::store(&manifest_key, &error)),
    }
}

async fn write_content_store_descriptor<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    created_at_ms: u64,
) -> Result<(), CoreError> {
    let object_key = content_store(content_store_id);
    let descriptor = ContentStoreState {
        content_store_id: content_store_id.clone(),
        created_at_ms,
    };
    let bytes =
        encode_control_state(ControlObjectKind::ContentStore, &descriptor).map_err(|error| {
            CoreError::Codec {
                object_key: object_key.clone(),
                message: error.to_string(),
            }
        })?;
    match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(()),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

async fn recreate_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    actor_id: &ActorId,
    access: &NamespaceAccess,
    allow_existing: bool,
) -> Result<Namespace, BootstrapNamespaceError> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    loop {
        let current = load_current_manifest(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let tombstone = current.envelope.payload();
        if !tombstone.status.is_deleted() {
            if allow_existing {
                return super::status::load_namespace(store, namespace_id)
                    .await
                    .map_err(Into::into);
            }
            return Err(BootstrapNamespaceError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            });
        }

        let retired = CheckpointRecordState {
            namespace_id: namespace_id.clone(),
            pin_id: CheckpointId::retired(namespace_id, tombstone.manifest_no),
            manifest_no: tombstone.manifest_no,
            manifest_head_seq: tombstone.head_seq,
            manifest_payload_checksum: current.state.manifest.manifest_payload_checksum.clone(),
            head_commit_id: tombstone.head_commit_id.clone(),
            created_at_ms: context.now_ms,
            owner: CheckpointOwner::Retired {},
        };
        write_checkpoint_record_if_absent(store, &retired).await?;

        let content_store_id = ContentStoreId::generate();
        write_content_store_descriptor(store, &content_store_id, context.now_ms).await?;

        let manifest_no = tombstone
            .manifest_no
            .successor()
            .map_err(|error| CoreError::Internal(format!("manifest number {error}")))?;
        let generation = tombstone
            .generation
            .successor()
            .map_err(|error| CoreError::Internal(format!("namespace generation {error}")))?;
        let genesis_seq = tombstone
            .head_seq
            .successor()
            .map_err(|error| CoreError::Internal(format!("change sequence {error}")))?;
        let writer_epoch = tombstone
            .writer_epoch
            .successor()
            .map_err(|error| CoreError::Internal(format!("writer epoch {error}")))?;
        let compactor_epoch = tombstone
            .compactor_epoch
            .checked_add(1)
            .ok_or_else(|| CoreError::Internal("compactor epoch overflow".to_owned()))?;
        let mut payload = NamespaceManifestPayload::initial(
            namespace_id.clone(),
            content_store_id,
            context.now_ms,
            actor_id.clone(),
            access.clone(),
        );
        payload.manifest_no = manifest_no;
        payload.generation = generation;
        payload.generation_first_manifest_no = manifest_no;
        payload.head_seq = genesis_seq;
        payload.base_seq = genesis_seq;
        payload.retention_floor_seq = genesis_seq;
        payload.last_folded_wal_no = tombstone.last_folded_wal_no;
        payload.next_inode_id = tombstone.next_inode_id;
        payload.next_run_no = tombstone.next_run_no;
        payload.writer_epoch = writer_epoch;
        payload.compactor_epoch = compactor_epoch;

        let manifest = encode_manifest(payload)?;
        if let ManifestPublicationOutcome::Published(_) = publish_manifest(
            store,
            namespace_id,
            manifest,
            Some(tombstone.manifest_no),
            &timer,
            started_ms,
        )
        .await?
        {
            return super::status::load_namespace(store, namespace_id)
                .await
                .map_err(Into::into);
        }
    }
}

pub(crate) fn bootstrap_metadata_state(
    created_at_ms: u64,
    access: &NamespaceAccess,
    genesis_seq: ChangeSeq,
) -> MetadataState {
    let access_revisions = match access {
        NamespaceAccess::Acl { root_grants, .. } if !root_grants.is_empty() => {
            vec![AccessRevisionRecord {
                inode_id: ROOT_INODE_ID,
                access_revision_no: AccessRevisionNo(0),
                committed_seq: genesis_seq,
                commit_id: loonfs_api::wire::control::genesis_commit_id(),
                delta_index: 0,
                updated_by: ActorId::loonfs(),
                updated_at_ms: created_at_ms,
                boundary: false,
                grants: root_grants.clone(),
            }]
        }
        _ => Vec::new(),
    };
    MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: ROOT_INODE_ID,
            inode_kind: InodeKind::Directory,
            created_seq: genesis_seq,
            commit_id: loonfs_api::wire::control::genesis_commit_id(),
            created_by: loonfs_api::ActorId::loonfs(),
            created_at_ms,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        access_revisions,
    )
}
