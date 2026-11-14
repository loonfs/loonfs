//! Creates namespaces.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::manifest::publish::ExistingNamespace;
use crate::metadata::{AccessRevisionRecord, InodeRecord, MetadataState};
use crate::namespace::basis::PublicationBasis;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{NamespaceAccess, NamespaceManifestPayload};
use loonfs_types::options::CreateNamespaceOptions;
use loonfs_types::{
    AccessRevisionNo, AccessRight, ActorId, CommitSeq, InodeKind, NamespaceMetadata, PoolId,
    ROOT_INODE_ID,
};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates the namespace if it does not already exist, or finishes a
    /// create of it that stopped before manifest 2
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/namespace-creation.md#creating-a-namespace-without-a-source)).
    ///
    /// Use this before normal reads and writes for a new namespace. Returns
    /// the namespace's status after manifest 2.
    ///
    /// 1. Finds the newest manifest. An active namespace returns
    ///    `namespace_exists`, or its state with `allow_existing`, and a
    ///    tombstone or a retired ID returns `namespace_deleted`. An
    ///    initializing namespace is finished from step 3 when its manifest 1
    ///    matches the request, and returns `namespace_exists` otherwise, also
    ///    with `allow_existing`.
    /// 2. Publishes manifest 1 with the status `initializing` and a generated
    ///    pool ID. When another call's manifest 1 won, the call continues as
    ///    in step 1.
    /// 3. Writes the namespace's member entry in the pool that manifest 1
    ///    records.
    /// 4. Publishes manifest 2 with the status `active`.
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
        let existing = match self.namespace.find_existing_namespace().await? {
            Some(existing) => existing,
            None => {
                let manifest_1 = NamespaceManifestPayload {
                    retention_window_ms: options.retention_window_ms,
                    ..NamespaceManifestPayload::initial(
                        self.namespace.namespace_id.clone(),
                        PoolId::generate(),
                        clock.request_at_ms(),
                        actor_id.clone(),
                        options.access.clone(),
                    )
                };
                match self
                    .namespace
                    .publish_first_manifest(manifest_1, &clock)
                    .await
                {
                    Ok(manifest_1) => return self.finish_create(manifest_1, &clock).await,
                    Err(CoreError::NamespaceExists { .. }) => {
                        self.namespace.find_existing_namespace().await?.ok_or_else(|| {
                            CoreError::NamespaceCorrupt(format!(
                                "the manifest 1 put of namespace `{}` lost to a manifest that isn't there",
                                self.namespace.namespace_id
                            ))
                        })?
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        let manifest_1 = match existing {
            ExistingNamespace::Initializing(manifest_1) => manifest_1,
            ExistingNamespace::Active if options.allow_existing => {
                return self.namespace.load_metadata().await
            }
            ExistingNamespace::Active => {
                return Err(CoreError::NamespaceExists {
                    namespace_id: self.namespace.namespace_id.clone(),
                })
            }
        };
        ensure_create_request_matches(manifest_1.manifest.payload(), options)?;
        self.finish_create(manifest_1, &clock).await
    }

    /// Writes the member entry in the pool that `manifest_1` records, and
    /// publishes manifest 2 (steps 3 and 4 of [`Self::bootstrap_namespace`]).
    async fn finish_create(
        &self,
        manifest_1: PublicationBasis,
        clock: &OperationClock,
    ) -> Result<NamespaceMetadata> {
        self.namespace
            .write_member_entry(&manifest_1.manifest.payload().pool_id)
            .await?;
        let active = self
            .namespace
            .publish_active_manifest(manifest_1, clock)
            .await?;
        Ok(super::status::created_namespace(active.manifest.payload()))
    }
}

/// Checks that `manifest_1`, the manifest 1 of an initializing namespace, is
/// a create that a call with `options` finishes, and returns
/// `namespace_exists` otherwise.
///
/// The request matches when manifest 1 has no fork basis and records the
/// access and the retention window of `options`. The creator, the creation
/// time and the pool ID aren't compared.
fn ensure_create_request_matches(
    manifest_1: &NamespaceManifestPayload,
    options: &CreateNamespaceOptions,
) -> Result<()> {
    let matches = manifest_1.fork_basis.is_none()
        && manifest_1.access == options.access
        && manifest_1.retention_window_ms == options.retention_window_ms;
    if !matches {
        return Err(CoreError::NamespaceExists {
            namespace_id: manifest_1.namespace_id.clone(),
        });
    }
    Ok(())
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
