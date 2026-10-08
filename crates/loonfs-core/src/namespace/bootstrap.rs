//! Creates namespaces.

use super::create::publish_namespace;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::metadata::{AccessRevisionRecord, InodeRecord, MetadataState};
use crate::options::CreateNamespaceOptions;
use crate::time::{Deadline, StdMonotonicTimer};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{NamespaceAccess, NamespaceManifestPayload};
use loonfs_types::{
    AccessRevisionNo, ActorId, ChangeSeq, InodeKind, NamespaceId, NamespaceMetadata, ROOT_INODE_ID,
};
use std::sync::Arc;

pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    actor_id: &loonfs_types::ActorId,
    options: &CreateNamespaceOptions,
) -> Result<NamespaceMetadata> {
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    let start = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        context.now_ms,
        actor_id.clone(),
        options.access.clone(),
        options.naming,
    );
    match publish_namespace(store, &start, &deadline).await {
        Ok(()) => {}
        Err(CoreError::NamespaceExists { .. }) if options.allow_existing => {}
        Err(error) => return Err(error),
    }
    super::status::load_namespace(store, namespace_id).await
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
                commit_id: loonfs_types::format::control::genesis_commit_id(),
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
            commit_id: loonfs_types::format::control::genesis_commit_id(),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: created_at_ms,
        }],
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
