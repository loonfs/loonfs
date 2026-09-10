//! Installs manifest 1 after the content descriptor and discovery hint.

use crate::context::MutationContext;
use crate::error::CoreError;
use crate::metadata::{InodeRecord, MetadataState};
use crate::namespace::control::{
    load_current_manifest, load_current_manifest_if_present, load_discovered_manifest,
};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, ContentStoreState, ControlObjectKind, HintState,
};
use loonfs_api::wire::manifest::{encode_namespace_manifest_json, NamespaceManifestPayload};
use loonfs_api::{
    ChangeSeq, ContentStoreId, ErrorCode, InodeKind, ManifestNo, Namespace, NamespaceId, WalNo,
    ROOT_INODE_ID,
};
use loonfs_objectstore::keys::{content_store, hint, metadata_manifest_object};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum BootstrapNamespaceError {
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is deleted and its id is retired")]
    NamespaceDeleted { namespace_id: NamespaceId },
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
            BootstrapNamespaceError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            BootstrapNamespaceError::Core(error) => error.code(),
        }
    }

    /// Returns the structured context the code's consumers report beside it,
    /// mirroring [`CoreError::details`](crate::Error::details): only the
    /// wrapped core failure carries any.
    pub fn details(&self) -> Option<loonfs_api::ErrorDetails> {
        match self {
            BootstrapNamespaceError::Core(error) => error.details(),
            BootstrapNamespaceError::NamespaceAlreadyExists { .. }
            | BootstrapNamespaceError::NamespaceDeleted { .. } => None,
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
    allow_existing: bool,
) -> Result<Namespace, BootstrapNamespaceError> {
    let manifest = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        ContentStoreId::generate(),
        context.now_ms,
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
            return Err(BootstrapNamespaceError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            })
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
    let descriptor_key = content_store(&manifest.content_store_id);
    let descriptor = ContentStoreState {
        content_store_id: manifest.content_store_id.clone(),
        created_at_ms: manifest.created_at_ms,
    };
    let hint_key = hint(namespace_id);
    let hint = HintState {
        namespace_id: namespace_id.clone(),
        manifest_no: ManifestNo(1),
        wal_no: WalNo(0),
    };
    let descriptor_bytes = encode_control_state(ControlObjectKind::ContentStore, &descriptor)
        .map_err(|error| CoreError::Codec {
            object_key: descriptor_key.clone(),
            message: error.to_string(),
        })?;
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
    for (key, bytes) in [(descriptor_key, descriptor_bytes), (hint_key, hint_bytes)] {
        match store.put_if_absent(&key, Bytes::from(bytes)).await {
            Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => {}
            Err(error) => return Err(CoreError::store(&key, &error)),
        }
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

pub(crate) fn bootstrap_metadata_state(created_at_ms: u64) -> MetadataState {
    MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: ROOT_INODE_ID,
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(0),
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
    )
}
