//! Content and source-pin cleanup for an eligible tombstone.

use super::deletes::apply_decisions;
use super::fork_pins::delete_source_pin;
use super::live_set::{LiveSet, RetirementStatus};
use super::sweep::Decision;
use crate::error::{CoreError, Result};
use futures::TryStreamExt;
use loonfs_objectstore::keys::content_prefix;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::GcResponse;

pub(super) async fn reclaim_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    live: &LiveSet,
    report: &mut GcResponse,
) -> Result<()> {
    if live.retirement_state() != RetirementStatus::Eligible {
        return Ok(());
    }
    let tombstone = live
        .current_tombstone
        .as_ref()
        .expect("eligible namespace should have a tombstone");
    sweep_content(store, tombstone, report).await?;
    if let Some(basis) = &tombstone.fork_basis {
        if delete_source_pin(store, basis).await? {
            report.deleted_checkpoints_by_owner.fork += 1;
        }
    }
    Ok(())
}

async fn sweep_content<S: ObjectStore + ?Sized>(
    store: &S,
    tombstone: &NamespaceManifestPayload,
    report: &mut GcResponse,
) -> Result<()> {
    let prefix = content_prefix(&tombstone.namespace_id);
    let decisions = store
        .list_prefix_stream(&prefix)
        .map_err(|error| CoreError::store(&prefix, &error))
        .map_ok(|key| Decision::Delete {
            key,
            counter: |report| &mut report.deleted.retired_content_objects,
        });
    apply_decisions(store, decisions, report).await
}
