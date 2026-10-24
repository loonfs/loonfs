//! Claims the namespace compactor role by publishing a manifest epoch.

use super::publish::{update_manifest, ManifestChange};
use super::published::{PublicationReport, PublishedManifest};
use crate::error::Result;
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::namespace::control::{LastHintRaise, LoadedManifest};
use crate::namespace::{control::ensure_namespace_live, state::NamespaceReadState};
use crate::time::{Deadline, MonotonicTimer};
use loonfs_objectstore::ObjectStore;
use loonfs_types::CompactorEpoch;
use loonfs_types::NamespaceId;
use std::sync::Arc;

pub(crate) async fn claim_compactor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    timer: Arc<dyn MonotonicTimer>,
    last_raise: Option<&LastHintRaise>,
) -> Result<PublicationReport<CompactorEpoch>> {
    let deadline = Deadline::start(timer);
    let (epoch, claimed, _) =
        update_manifest(
            store,
            namespace_id,
            &deadline,
            MAX_CONTENTION_ATTEMPTS,
            last_raise,
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
