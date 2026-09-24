//! Content and source-pin cleanup for an eligible tombstone.

use super::fork_checkpoints::delete_source_checkpoint;
use super::live_set::{LiveSet, RetirementState};
use crate::error::{CoreError, Result};
use futures::{StreamExt, TryStreamExt};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::GcResponse;
use loonfs_objectstore::keys::content_prefix;
use loonfs_objectstore::ObjectStore;

const CONTENT_DELETE_CONCURRENCY: usize = 16;

pub(super) async fn reclaim_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    live: &LiveSet,
    report: &mut GcResponse,
) -> Result<()> {
    if live.retirement_state() != RetirementState::Eligible {
        return Ok(());
    }
    let tombstone = live
        .current_tombstone
        .as_ref()
        .expect("eligible namespace should have a tombstone");
    sweep_content(store, tombstone, report).await?;
    if let Some(basis) = &tombstone.fork_basis {
        if delete_source_checkpoint(store, basis).await? {
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
    let mut deletions = store
        .list_prefix_stream(&prefix)
        .map_err(|error| CoreError::store(&prefix, &error))
        .map(|key| async move {
            let key = key?;
            store
                .delete(&key)
                .await
                .map_err(|error| CoreError::store(&key, &error))
        })
        .buffer_unordered(CONTENT_DELETE_CONCURRENCY);
    while deletions.try_next().await?.is_some() {
        report.deleted.retired_content_objects += 1;
    }
    Ok(())
}
