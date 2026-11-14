//! Publishes manifest 2 of a namespace that's initializing, the namespace's
//! manifest 1 with the status `active`.

use crate::error::Result;
use crate::manifest::publish::ManifestChange;
use crate::namespace::basis::PublicationBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::NamespaceStatus;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes manifest 2 of this namespace, `manifest_1` with the status
    /// `active`, and returns the active manifest
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/namespace-creation.md#finishing-a-namespace-thats-initializing)).
    ///
    /// A namespace that another call made active returns that call's manifest,
    /// and a tombstone returns `namespace_deleted`.
    pub(crate) async fn publish_active_manifest(
        &self,
        manifest_1: PublicationBasis,
        clock: &OperationClock,
    ) -> Result<PublicationBasis> {
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
        Ok(update.current)
    }
}
