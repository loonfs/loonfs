//! Publishes manifest 2 of a namespace that's initializing, the namespace's
//! manifest 1 with the status `active`.

use crate::cache::CachedHead;
use crate::error::{CoreError, Result};
use crate::manifest::publish::ManifestPublicationOutcome;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::format::manifest::NamespaceManifestPayload;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes manifest 2 of this namespace, `manifest_1` with the status
    /// `active`, and returns the active manifest
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/namespace-creation.md#finishing-a-namespace-thats-initializing)).
    ///
    /// The activation puts once, and once more over a fork's manifest 2 that
    /// won its put (see [`Self::publish_over_initializing`]). When another
    /// call made the namespace active, the function returns the manifest
    /// that won the put, and a tombstone returns `namespace_deleted`.
    ///
    /// An active manifest that this call published becomes the namespace's
    /// cached head, so this process's first read and first writer claim start
    /// from it without a request. The head's manifest check and its head
    /// confirmation are both timed at the start of the call's clock, because
    /// the call put the manifest after that start, so at that start no
    /// manifest followed it and no commit above it had been acknowledged. A call that found the namespace already active leaves
    /// the cached head as it is, and the first read loads the namespace by
    /// discovery.
    pub(crate) async fn publish_active_manifest(
        &self,
        manifest_1: Held<LoadedManifest>,
        clock: &OperationClock,
    ) -> Result<Held<LoadedManifest>> {
        let winner = match self
            .publish_over_initializing(manifest_1, clock, NamespaceManifestPayload::activated)
            .await?
        {
            ManifestPublicationOutcome::Published(active) => {
                let head = NamespaceReadState::from(active.manifest().payload());
                self.head_and_wal_tail_cache.replace_head(CachedHead::new(
                    active.with_head(head),
                    active.checked().clone(),
                ));
                return Ok(active);
            }
            ManifestPublicationOutcome::Lost(winner) => winner,
        };
        match winner.manifest().payload().status {
            NamespaceStatus::Active {} => Ok(winner),
            NamespaceStatus::Deleted { .. } => Err(self.namespace_deleted()),
            NamespaceStatus::Initializing {} => Err(CoreError::NamespaceCorrupt(format!(
                "manifest {} of namespace `{}` is initializing after a fork's manifest 2",
                winner.manifest_no(),
                self.namespace_id
            ))),
        }
    }
}
