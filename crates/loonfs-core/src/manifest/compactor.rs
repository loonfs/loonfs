//! Claims the namespace compactor role by publishing a manifest epoch.

use super::publish::ManifestChange;
use super::published::{PublicationReport, PublishedManifest};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::Result;
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::namespace::control::LoadedManifest;
use crate::namespace::{control::ensure_namespace_live, state::NamespaceReadState};
use crate::time::Deadline;
use loonfs_objectstore::ObjectStore;
use loonfs_types::CompactorEpoch;
use std::sync::Arc;

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Claims the namespace compactor epoch for a maintenance runtime. The
    /// report includes the manifest that records the claim.
    pub async fn claim_compactor(&self) -> Result<PublicationReport<CompactorEpoch>> {
        let deadline = Deadline::start(Arc::clone(&self.namespace.timer));
        let (epoch, claimed, _) = self
            .namespace
            .update_manifest(
                None,
                &deadline,
                MAX_CONTENTION_ATTEMPTS,
                |current: LoadedManifest| async move {
                    let mut payload = current.state.envelope.payload().clone();
                    ensure_namespace_live(&NamespaceReadState::from(&payload))?;
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
                &claimed.state.envelope,
                Some(deadline.origin()),
            )),
        })
    }
}
