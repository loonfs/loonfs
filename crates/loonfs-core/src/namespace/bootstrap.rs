//! Creates namespaces.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::metadata::{AccessRevisionRecord, InodeRecord, MetadataState};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::PoolPayload;
use loonfs_types::format::manifest::{NamespaceAccess, NamespaceManifestPayload};
use loonfs_types::options::CreateNamespaceOptions;
use loonfs_types::{
    AccessRevisionNo, AccessRight, ActorId, CommitSeq, InodeKind, NamespaceMetadata, PoolId,
    ROOT_INODE_ID,
};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates the namespace if it does not already exist.
    ///
    /// Use this before normal reads and writes for a new namespace. Returns
    /// the namespace's status after manifest 1 is installed. The call looks
    /// for the namespace first and writes nothing when the namespace exists.
    /// It writes the pool record of a generated pool before manifest 1, and
    /// deletes the record again when another creation's manifest 1 wins
    /// (format spec, "Creating a namespace").
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
        let pool = PoolPayload {
            pool_id: PoolId::generate(),
            first_member_namespace_id: self.namespace.namespace_id.clone(),
            created_at_ms: clock.request_at_ms(),
        };
        let start = NamespaceManifestPayload {
            retention_window_ms: options.retention_window_ms,
            ..NamespaceManifestPayload::initial(
                pool.first_member_namespace_id.clone(),
                pool.pool_id.clone(),
                pool.created_at_ms,
                actor_id.clone(),
                options.access.clone(),
            )
        };
        let created = async {
            self.namespace.ensure_namespace_absent().await?;
            self.namespace.write_pool_record(&pool).await?;
            let published = self
                .namespace
                .publish_first_manifest(start.clone(), &clock)
                .await;
            if let Err(
                error @ (CoreError::NamespaceExists { .. } | CoreError::NamespaceDeleted { .. }),
            ) = &published
            {
                self.namespace.delete_unused_pool_record(&pool, error).await;
            }
            published
        }
        .await;
        match created {
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
                committed_seq: CommitSeq(0),
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
            committed_seq: CommitSeq(0),
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
