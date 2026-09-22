//! Content and source-pin cleanup for eligible generations.

use super::fork_checkpoints::delete_source_checkpoint;
use super::live_set::{GenerationState, LiveSet, RetiredPin};
use crate::checkpoint::record::delete_checkpoint_record;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use futures::StreamExt;
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{GcResponse, NamespaceGeneration, NamespaceId, RetainedReason};
use loonfs_objectstore::keys::content_owner_prefix;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::collections::BTreeSet;

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
        sweep_content(store, namespace_id, tombstone, report).await?;
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
    namespace_id: &NamespaceId,
    tombstone: &NamespaceManifestPayload,
    report: &mut GcResponse,
) -> Result<()> {
    let prefix = content_owner_prefix(
        &tombstone.content_store_id,
        namespace_id,
        tombstone.generation,
    );
    let generation = tombstone.generation.to_string();
    let mut listing = store.list_prefix_stream(&prefix);
    while let Some(key) = listing
        .next()
        .await
        .transpose()
        .map_err(|error| CoreError::store(&prefix, &error))?
    {
        if !key.starts_with(&prefix)
            || parse_object_key(&key).is_none_or(|parsed| {
                parsed.family() != DurableObjectFamily::ContentBlob
                    || parsed.owner_namespace_id() != Some(namespace_id.as_str())
                    || parsed.owner_generation() != Some(generation.as_str())
            })
        {
            report.retain(RetainedReason::UnrecognizedKey);
            continue;
        }
        match store.delete(&key).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {
                report.deleted.retired_content_objects += 1
            }
            Err(error) => return Err(CoreError::store(&key, &error)),
        }
    }
    Ok(())
}
