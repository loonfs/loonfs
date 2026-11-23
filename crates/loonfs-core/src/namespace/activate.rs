//! Publishes manifest 2 of a namespace that's initializing, the namespace's
//! manifest 1 with the status `active`.

use crate::cache::CachedHead;
use crate::error::Result;
use crate::manifest::publish::ManifestChange;
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::time::{Observation, OperationClock};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes manifest 2 of this namespace, `manifest_1` with the status
    /// `active`, and returns the active manifest
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/namespace-creation.md#finishing-a-namespace-thats-initializing)).
    ///
    /// A namespace that another call made active returns that call's manifest,
    /// and a tombstone returns `namespace_deleted`. The active manifest becomes
    /// the namespace's cached head (see [`Self::install_active_head`]).
    pub(crate) async fn publish_active_manifest(
        &self,
        manifest_1: Held<LoadedManifest>,
        clock: &OperationClock,
    ) -> Result<Held<LoadedManifest>> {
        let update = self
            .update_manifest(Some(manifest_1), clock, |current: LoadedManifest| {
                let changed = match current.payload().status {
                    NamespaceStatus::Initializing {} => Ok(ManifestChange::Next(
                        Box::new(current.payload().activated()),
                        (),
                    )),
                    NamespaceStatus::Active {} => Ok(ManifestChange::Finished(())),
                    NamespaceStatus::Deleted { .. } => Err(self.namespace_deleted()),
                };
                async move { changed }
            })
            .await?;
        self.install_active_head(&update.current, clock.origin());
        Ok(update.current)
    }

    /// Installs `active`, the active manifest that a create or a fork
    /// published or found, as the namespace's cached head, so this process's
    /// first read and first writer claim start from it. A read replays the
    /// empty WAL tail above `active` without a request.
    ///
    /// `started` is when the create or the fork started. The namespace wasn't
    /// active then, so no commit had been acknowledged and no manifest
    /// followed `active`. Both checks of the head are at `started`.
    pub(crate) fn install_active_head(&self, active: &Held<LoadedManifest>, started: Observation) {
        self.head_and_wal_tail_cache.replace_head(CachedHead {
            read_basis: ReadBasis {
                head: NamespaceReadState::from(active.manifest().payload()),
                basis: MetadataBasis::new(active.manifest().manifest().clone()),
            },
            basis_checked: started.clone(),
            head_confirmed: started,
            boundary: active.boundary().clone(),
        });
    }
}
