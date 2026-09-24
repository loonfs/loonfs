//! Claims the namespace compactor role by publishing a manifest epoch.

use super::publish::{update_manifest, ManifestChange};
use crate::error::Result;
use crate::namespace::{control::ensure_namespace_live, state::NamespaceReadState};
use crate::time::{Deadline, StdMonotonicTimer};
use loonfs_api::CompactorEpoch;
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) async fn claim_compactor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<CompactorEpoch> {
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    update_manifest(store, namespace_id, &deadline, |mut payload| async move {
        ensure_namespace_live(&NamespaceReadState::from(&payload))?;
        payload.compactor_epoch = CompactorEpoch(
            payload
                .compactor_epoch
                .0
                .checked_add(1)
                .expect("compactor epoch should be bounded by the manifest number write stop"),
        );
        let epoch = payload.compactor_epoch;
        Ok(ManifestChange::Next(Box::new(payload), epoch))
    })
    .await
}
