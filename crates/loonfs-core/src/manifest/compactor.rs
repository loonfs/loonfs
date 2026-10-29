//! Claims the namespace compactor role by publishing a manifest epoch.

use super::publish::ManifestChange;
use super::published::{PublicationReport, PublishedManifest};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::Result;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::CompactorEpoch;

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Claims the namespace compactor epoch for a maintenance runtime. The
    /// report includes the manifest that records the claim.
    pub async fn claim_compactor(&self) -> Result<PublicationReport<CompactorEpoch>> {
        let clock = self.namespace.start_clock();
        let (epoch, claimed, _) = self
            .namespace
            .update_live_manifest(
                None,
                &clock,
                |mut payload: NamespaceManifestPayload| async move {
                    payload.compactor_epoch =
                        CompactorEpoch(payload.compactor_epoch.0.checked_add(1).expect(
                            "compactor epoch should be bounded by the manifest number write stop",
                        ));
                    let epoch = payload.compactor_epoch;
                    Ok(ManifestChange::Next(Box::new(payload), epoch))
                },
            )
            .await?;
        Ok(PublicationReport {
            response: epoch,
            published: Some(PublishedManifest::new(
                &claimed.envelope,
                Some(clock.origin()),
            )),
        })
    }
}
