//! Content and source-pin cleanup for eligible generations.

use super::fork_checkpoints::delete_source_checkpoint;
use super::live_set::{GenerationState, LiveSet, RetiredGeneration};
use crate::checkpoint::load_owned_manifest_segments_for_inspection;
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
    let generations =
        live.current_tombstone
            .iter()
            .map(|tombstone| {
                let record = live
                    .retired_generations
                    .iter()
                    .find(|record| record.reference.manifest_no == tombstone.manifest_no);
                (tombstone, record)
            })
            .chain(
                live.retired_generations
                    .iter()
                    .filter(|record| {
                        !live.current_tombstone.as_ref().is_some_and(|current| {
                            current.manifest_no == record.reference.manifest_no
                        })
                    })
                    .filter_map(|record| {
                        record
                            .tombstone
                            .as_ref()
                            .map(|tombstone| (tombstone, Some(record)))
                    }),
            );
    for (tombstone, record) in generations {
        if live.generation_state(tombstone.generation) != GenerationState::Eligible
            || !confirm_generation(store, namespace_id, tombstone, record).await?
        {
            continue;
        }
        sweep_content(
            store,
            tombstone,
            record.map(|record| &record.reference),
            report,
        )
        .await?;
        if let Some(basis) = &tombstone.fork_basis {
            if delete_source_checkpoint(store, basis).await? {
                report.deleted_checkpoints_by_owner.fork += 1;
            }
        }
        if let Some(record) = record.filter(|_| !retained_sessions.contains(&tombstone.generation))
        {
            delete_retired_record(store, record, report).await?;
        }
    }
    for record in live
        .retired_generations
        .iter()
        .filter(|record| record.tombstone.is_none())
    {
        delete_retired_record(store, record, report).await?;
    }
    Ok(())
}

async fn delete_retired_record<S: ObjectStore + ?Sized>(
    store: &S,
    record: &RetiredGeneration,
    report: &mut GcResponse,
) -> Result<()> {
    match store.delete(&record.key).await {
        Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {
            report.deleted.retired_generation_records += 1;
            Ok(())
        }
        Err(error) => Err(CoreError::store(&record.key, &error)),
    }
}

async fn confirm_generation<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    tombstone: &NamespaceManifestPayload,
    record: Option<&RetiredGeneration>,
) -> Result<bool> {
    if let Some(record) = record {
        return store
            .head(&record.key)
            .await
            .map(|metadata| metadata.is_some())
            .map_err(|error| CoreError::store(&record.key, &error));
    }
    let current = load_current_manifest(store, namespace_id).await?;
    Ok(current.envelope.payload().manifest_no == tombstone.manifest_no)
}

async fn sweep_content<S: ObjectStore + ?Sized>(
    store: &S,
    tombstone: &NamespaceManifestPayload,
    reference: Option<&loonfs_api::wire::control::ManifestRef>,
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
    if let Some(reference) = reference {
        crate::checkpoint::ensure_manifest_reference_matches(
            "retired generation",
            reference,
            segments.manifest(),
        )?;
    }
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
