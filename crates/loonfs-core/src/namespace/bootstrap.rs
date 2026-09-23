//! Creates namespaces, including a deleted id's next generation.

use super::generation::{publish_generation, GenerationPublication};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::metadata::{AccessRevisionRecord, InodeRecord, MetadataState};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::{NamespaceAccess, NamespaceManifestPayload};
use loonfs_api::{
    AccessRevisionNo, ActorId, ChangeSeq, ErrorCode, InodeKind, Namespace, NamespaceId,
    ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;
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
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let start = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        context.now_ms,
        actor_id.clone(),
        access.clone(),
    );
    if publish_generation(store, &start, &timer, started_ms).await? == GenerationPublication::Exists
        && !allow_existing
    {
        return Err(BootstrapNamespaceError::NamespaceAlreadyExists {
            namespace_id: namespace_id.clone(),
        });
    }
    super::status::load_namespace(store, namespace_id)
        .await
        .map_err(Into::into)
}

pub(crate) fn bootstrap_metadata_state(
    created_at_ms: u64,
    access: &NamespaceAccess,
) -> MetadataState {
    let access_revisions = match access {
        NamespaceAccess::Acl { root_grants, .. } if !root_grants.is_empty() => {
            vec![AccessRevisionRecord {
                inode_id: ROOT_INODE_ID,
                access_revision_no: AccessRevisionNo(0),
                committed_seq: ChangeSeq(0),
                commit_id: loonfs_api::wire::control::genesis_commit_id(),
                delta_index: 0,
                committed_by: ActorId::loonfs(),
                committed_at_ms: created_at_ms,
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
            committed_seq: ChangeSeq(0),
            commit_id: loonfs_api::wire::control::genesis_commit_id(),
            committed_by: loonfs_api::ActorId::loonfs(),
            committed_at_ms: created_at_ms,
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
