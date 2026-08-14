//! Namespace creation: one conditional write of a complete head.
//!
//! The head is the namespace. Nothing is written before it and nothing
//! after it, so a create either lands entirely or leaves the namespace
//! absent — there is no partial state to classify, complete, or repair
//! (format spec, "Creating a namespace").

use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, StoreFailureClass};
use crate::metadata::{InodeRecord, MetadataState};
use crate::namespace::control::read_head_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope, NamespaceState,
    WriterBlock,
};
use loonfs_api::{
    ChangeSeq, ContentStoreId, ErrorCode, InodeKind, NamespaceId, NamespaceStatusResponse,
    ROOT_INODE_ID,
};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum BootstrapNamespaceError {
    #[error("holder id must not be empty")]
    EmptyHolderId,
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is deleted and its id is retired")]
    NamespaceDeleted { namespace_id: NamespaceId },
    #[error(transparent)]
    Head(#[from] ControlObjectLoadError),
    /// A failure inside the installation protocol shared with fork — the
    /// head write itself, or engine plumbing such as assembling the
    /// mutation context. The wire code delegates to
    /// [`CoreError::code`](crate::Error::code), so store failures keep
    /// their failure class.
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
            BootstrapNamespaceError::EmptyHolderId => ErrorCode::InvalidRequest,
            BootstrapNamespaceError::NamespaceAlreadyExists { .. } => ErrorCode::NamespaceExists,
            BootstrapNamespaceError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            BootstrapNamespaceError::Head(ControlObjectLoadError::Store {
                class: StoreFailureClass::PermissionDenied,
                ..
            }) => ErrorCode::PermissionDenied,
            BootstrapNamespaceError::Head(_) => ErrorCode::ServerError,
            BootstrapNamespaceError::Core(error) => error.code(),
        }
    }

    /// Returns the structured context the code's consumers report beside it,
    /// mirroring [`CoreError::details`](crate::Error::details): only the
    /// wrapped core failure carries any.
    pub fn details(&self) -> Option<loonfs_api::ErrorDetails> {
        match self {
            BootstrapNamespaceError::Core(error) => error.details(),
            BootstrapNamespaceError::EmptyHolderId
            | BootstrapNamespaceError::NamespaceAlreadyExists { .. }
            | BootstrapNamespaceError::NamespaceDeleted { .. }
            | BootstrapNamespaceError::Head(_) => None,
        }
    }
}

pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    allow_existing: bool,
) -> Result<NamespaceStatusResponse, BootstrapNamespaceError> {
    if context.writer_id.trim().is_empty() {
        return Err(BootstrapNamespaceError::EmptyHolderId);
    }

    // A fresh content-store id per namespace. Nothing claims it durably:
    // uniqueness rests on the generated id's randomness, exactly as it does
    // for every other generated id in the format.
    let mut head = HeadState::initial(
        namespace_id.clone(),
        ContentStoreId::generate(),
        context.now_ms,
    );
    head.writer = Some(WriterBlock {
        writer_id: context.writer_id.clone(),
        acquired_at_ms: context.now_ms,
    });

    match install_namespace_head(store, namespace_id, &head).await? {
        NamespaceHeadInstall::Landed => {}
        // Whoever wrote the head owns the id. A caller retrying after a
        // lost acknowledgment gets the same answer as a caller who lost the
        // race outright, and the namespace it names is complete and usable
        // either way — the old flow's `namespace_partial`, which named a
        // namespace nobody could use, is gone. `allow_existing` is how a
        // caller says "create it if it is not there", including on a retry.
        NamespaceHeadInstall::Exists if allow_existing => {}
        NamespaceHeadInstall::Exists => {
            return Err(BootstrapNamespaceError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            })
        }
        NamespaceHeadInstall::Deleted => {
            return Err(BootstrapNamespaceError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            })
        }
    }

    crate::namespace::status::load_namespace_head_summary(store, namespace_id)
        .await
        .map_err(BootstrapNamespaceError::Core)
}

/// How one namespace-installing conditional write resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NamespaceHeadInstall {
    /// This attempt's write created the namespace.
    Landed,
    /// A namespace already owns the id.
    Exists,
    /// The id is retired by a deletion tombstone.
    Deleted,
}

/// Publishes a complete namespace head as the namespace's one and only
/// installation write.
///
/// Conditional creation is the whole protocol: exactly one attempt can win,
/// and the loser reads the head back to say what it lost to — an existing
/// namespace or a deletion tombstone. There is no third answer, because
/// there is no state between absent and complete.
///
/// The loser is not told whether the winner was its own earlier attempt.
/// Nothing durable can say: the head's writer block is one writer session,
/// and a server holds one session across every caller it serves, so
/// "written by my session" does not mean "written by this attempt". A
/// caller that wants a retry to succeed asks for that with
/// `allow_existing`.
pub(super) async fn install_namespace_head<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    head: &HeadState,
) -> Result<NamespaceHeadInstall, CoreError> {
    let object_key = wal_head(namespace_id);
    let envelope = HeadStateEnvelope::from_state(ControlObjectKind::WalHead, head.clone())
        .map_err(|err| CoreError::Internal(format!("failed to build head envelope: {err}")))?;
    let bytes = encode_control_object(&envelope)
        .map_err(|err| CoreError::Internal(format!("failed to encode head object: {err}")))?;
    // The namespace head is mutable control state, so installation must be a conditional create.
    match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(_) => Ok(NamespaceHeadInstall::Landed),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            let existing = match read_head_object(store, namespace_id).await {
                Ok(loaded) => loaded.state,
                // An unreadable head still occupies the id: report the
                // corruption rather than a lifecycle answer this attempt
                // cannot support.
                Err(error) => return Err(CoreError::load_head(error)),
            };
            if existing.state == NamespaceState::Deleted {
                return Ok(NamespaceHeadInstall::Deleted);
            }
            Ok(NamespaceHeadInstall::Exists)
        }
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

/// The built-in genesis metadata state: the root directory inode, and
/// nothing else.
///
/// A created namespace materializes no manifest, so this is synthesized at
/// read time as its basis until the first flush publishes one.
pub(crate) fn bootstrap_metadata_state(created_at_ms: u64) -> MetadataState {
    MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: ROOT_INODE_ID,
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(0),
            created_by: loonfs_api::ActorRef::loonfs_system(),
            created_at_ms,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}
