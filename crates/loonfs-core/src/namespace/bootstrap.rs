//! Creates namespaces.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::metadata::{AccessRevisionRecord, InodeRecord, MetadataState};
use crate::options::CreateNamespaceOptions;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{NamespaceAccess, NamespaceManifestPayload};
use loonfs_types::{
    AccessRevisionNo, AccessRight, ActorId, ChangeSeq, InodeKind, NamespaceMetadata, ROOT_INODE_ID,
};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates the namespace if it does not already exist.
    ///
    /// Use this before normal reads and writes for a new namespace. Returns
    /// the namespace's status after manifest 1 is installed.
    pub async fn bootstrap_namespace(
        &self,
        actor_id: &loonfs_types::ActorId,
        options: &CreateNamespaceOptions,
    ) -> Result<NamespaceMetadata> {
        if let NamespaceAccess::Acl { root_grants, .. } = &options.access {
            if !root_grants
                .iter()
                .any(|(_, rights)| rights.contains(AccessRight::Admin))
            {
                return Err(CoreError::RootGrantsWithoutAdministrator);
            }
        }
        let clock = self.namespace.start_clock();
        let start = NamespaceManifestPayload {
            retention_window_ms: options.retention_window_ms,
            ..NamespaceManifestPayload::initial(
                self.namespace.namespace_id.clone(),
                clock.request_at_ms(),
                actor_id.clone(),
                options.access.clone(),
            )
        };
        let created = async {
            self.namespace.ensure_absent().await?;
            self.namespace.create_manifest(start.clone(), &clock).await
        };
        match created.await {
            Ok(()) => Ok(super::status::created_namespace(&start)),
            Err(CoreError::NamespaceExists { .. }) if options.allow_existing => {
                self.namespace.load_metadata().await
            }
            Err(error) => Err(error),
        }
    }
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
        Vec::new(),
        access_revisions,
    )
}
