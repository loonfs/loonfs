//! Claims the namespace compactor role by publishing a manifest epoch.

use super::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

/// Publishes the next manifest number with the compactor epoch raised by one
/// and returns the raised epoch. A namespace is claimed only when it has
/// work to compact, so its manifest exists.
pub(crate) async fn claim_compactor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<u64> {
    let head = crate::namespace::control::load_head_object(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    crate::namespace::control::ensure_namespace_live(&head.state)?;
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    loop {
        super::flush::ensure_metadata_publication_budget(&timer, started_ms, namespace_id)?;
        let current = load_current_manifest(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let mut payload = current.envelope.payload().clone();
        payload.manifest_no = super::flush::next_manifest_no_after(payload.manifest_no)?;
        payload.compactor_epoch = payload
            .compactor_epoch
            .checked_add(1)
            .expect("compactor epoch should be bounded by the manifest number write stop");
        let epoch = payload.compactor_epoch;
        let manifest = encode_manifest(payload)?;
        if matches!(
            publish_manifest(
                store,
                namespace_id,
                &manifest,
                Some(current.state.manifest.manifest_no),
                &timer,
                started_ms,
            )
            .await?,
            ManifestPublicationOutcome::Published(_)
        ) {
            return Ok(epoch);
        }
    }
}
