//! Claims the namespace compactor role by publishing a manifest epoch.

use super::publish::{update_manifest, ManifestChange};
use crate::error::Result;
use crate::namespace::{control::ensure_namespace_live, state::NamespaceReadState};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

pub(crate) async fn claim_compactor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<u64> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    update_manifest(
        store,
        namespace_id,
        &timer,
        started_ms,
        |mut payload| async move {
            ensure_namespace_live(&NamespaceReadState::from(&payload))?;
            payload.compactor_epoch = payload
                .compactor_epoch
                .checked_add(1)
                .expect("compactor epoch should be bounded by the manifest number write stop");
            let epoch = payload.compactor_epoch;
            Ok(ManifestChange::Next(Box::new(payload), epoch))
        },
    )
    .await
}
