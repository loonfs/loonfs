//! Content and source-pin cleanup for eligible generations.

use super::fork_checkpoints::delete_source_checkpoint;
use super::live_set::{GenerationState, LiveSet, RetiredPin};
use crate::checkpoint::load_owned_manifest_segments_for_inspection;
use crate::checkpoint::record::delete_checkpoint_record;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control::load_current_manifest;
use loonfs_api::wire::manifest::{
    lookup_keys, MetadataRow, MetadataRowFamily, NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::{GcResponse, NamespaceGeneration, NamespaceId};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::collections::BTreeSet;

const PUBLICATION_SCAN_PAGE_ROWS: usize = 256;

pub(super) async fn reclaim_generations<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    live: &LiveSet,
    retained_sessions: &BTreeSet<NamespaceGeneration>,
    report: &mut GcResponse,
) -> Result<()> {
    let generations = live
        .current_tombstone
        .iter()
        .map(|tombstone| (tombstone, None))
        .chain(
            live.retired_pins
                .iter()
                .map(|pin| (&pin.tombstone, Some(pin))),
        );
    for (tombstone, pin) in generations {
        if live.generation_state(tombstone.generation) != GenerationState::Eligible
            || !confirm_generation(store, namespace_id, tombstone, pin).await?
        {
            continue;
        }
        sweep_content(store, tombstone, report).await?;
        if let Some(basis) = &tombstone.fork_basis {
            if delete_source_checkpoint(store, basis).await? {
                report.deleted_checkpoints_by_owner.fork += 1;
            }
        }
        if let Some(pin) = pin.filter(|_| !retained_sessions.contains(&tombstone.generation)) {
            delete_checkpoint_record(store, namespace_id, &pin.id).await?;
            report.deleted_checkpoints_by_owner.retired += 1;
        }
    }
    Ok(())
}

async fn confirm_generation<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    tombstone: &NamespaceManifestPayload,
    pin: Option<&RetiredPin>,
) -> Result<bool> {
    if let Some(pin) = pin {
        return store
            .head(&pin.key)
            .await
            .map(|metadata| metadata.is_some())
            .map_err(|error| CoreError::store(&pin.key, &error));
    }
    let current = load_current_manifest(store, namespace_id).await?;
    Ok(current.envelope.payload().manifest_no == tombstone.manifest_no)
}

async fn sweep_content<S: ObjectStore + ?Sized>(
    store: &S,
    tombstone: &NamespaceManifestPayload,
    report: &mut GcResponse,
) -> Result<()> {
    let segments = load_owned_manifest_segments_for_inspection(
        store,
        &tombstone.namespace_id,
        &tombstone.manifest_no,
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    })?;
    let family = MetadataRowFamily::ContentPublications;
    let mut lower_bound = family.row_key_prefix().to_owned();
    let upper_bound = string_prefix_upper_bound(&lower_bound);
    loop {
        let rows = segments
            .scan_range_page_with_keys(
                family,
                &lower_bound,
                upper_bound.as_deref(),
                PUBLICATION_SCAN_PAGE_ROWS,
            )
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        let exhausted = rows.len() < PUBLICATION_SCAN_PAGE_ROWS;
        let Some((last_key, _)) = rows.last() else {
            break;
        };
        lower_bound = lookup_keys::after_row_key(last_key);
        for (_, row) in rows {
            let MetadataRow::ContentPublication(record) = row else {
                continue;
            };
            if record.owner_namespace_id != tombstone.namespace_id
                || record.owner_generation != tombstone.generation
            {
                continue;
            }
            let key = content_blob(&record.owner_namespace_id, &record.content_id);
            match store.delete(&key).await {
                Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {
                    report.deleted.retired_content_objects += 1
                }
                Err(error) => return Err(CoreError::store(&key, &error)),
            }
        }
        if exhausted {
            break;
        }
    }
    Ok(())
}
