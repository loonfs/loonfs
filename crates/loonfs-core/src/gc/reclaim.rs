//! Content and source-pin cleanup for eligible generations.

use super::fork_checkpoints::delete_source_checkpoint;
use super::live_set::{GenerationState, LiveSet};
use crate::checkpoint::load_manifest_segments_for_inspection;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use loonfs_api::wire::manifest::{
    lookup_keys, MetadataRow, MetadataRowFamily, NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::GcResponse;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::ObjectStore;

const PUBLICATION_SCAN_PAGE_ROWS: usize = 256;

pub(super) async fn reclaim_generations<S: ObjectStore + ?Sized>(
    store: &S,
    live: &LiveSet,
    report: &mut GcResponse,
) -> Result<()> {
    for retired in &live.retired_generations {
        let tombstone = &retired.tombstone;
        if live.generation_state(tombstone.generation) != GenerationState::Eligible {
            continue;
        }
        sweep_content(store, tombstone, report).await?;
        if let Some(basis) = &tombstone.fork_basis {
            if delete_source_checkpoint(store, basis).await? {
                report.deleted_checkpoints_by_owner.fork += 1;
            }
        }
        if let Some(key) = &retired.record_key {
            store
                .delete(key)
                .await
                .map_err(|error| CoreError::store(key, &error))?;
            report.deleted.retired_generation_records += 1;
        }
    }
    Ok(())
}

async fn sweep_content<S: ObjectStore + ?Sized>(
    store: &S,
    tombstone: &NamespaceManifestPayload,
    report: &mut GcResponse,
) -> Result<()> {
    let segments = load_manifest_segments_for_inspection(
        store,
        None,
        &tombstone.namespace_id,
        &tombstone.manifest_no,
        Some(&tombstone.namespace_id),
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
                Ok(()) => report.deleted.retired_content_objects += 1,
                Err(error) => return Err(CoreError::store(&key, &error)),
            }
        }
        if exhausted {
            break;
        }
    }
    Ok(())
}
