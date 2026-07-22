//! Reaping: age-gated deletion of unreachable objects and the explicit
//! namespace-repair reap for abandoned installation debris.

use super::config::GcReport;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use crate::namespace::control::ControlObjectLoadError;
use loonfs_api::{ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{namespace_config, wal_head};
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbandonedBootstrapReap {
    Reaped,
    InFlight,
    AlreadyComplete,
}

/// Reaps a non-completable namespace tree for explicit admin repair once its
/// newest object is older than [`GC_MIN_GRACE_WINDOW_MS`], re-checking the
/// complete head-and-descriptor pair immediately before deletion.
pub(crate) async fn reap_abandoned_bootstrap<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AbandonedBootstrapReap, CoreError> {
    let namespace_prefix = format!("namespaces/{}/", namespace_id.as_str());
    let keys = list_prefix(store, &namespace_prefix).await?;
    if keys.is_empty() {
        return Ok(AbandonedBootstrapReap::Reaped);
    }

    // The newest object bounds the tree's age; a missing timestamp reads as
    // "young" and blocks the reap.
    for key in &keys {
        let Some(metadata) = store
            .head(key)
            .await
            .map_err(|error| CoreError::store(key, &error))?
        else {
            continue;
        };
        let Some(last_modified_ms) = metadata.last_modified_ms else {
            return Ok(AbandonedBootstrapReap::InFlight);
        };
        if context.now_ms.saturating_sub(last_modified_ms) < GC_MIN_GRACE_WINDOW_MS {
            return Ok(AbandonedBootstrapReap::InFlight);
        }
    }

    // Re-check the complete pair immediately before deleting. A descriptor
    // without a head is itself non-completable debris; a real completed
    // namespace always has both because the descriptor is written last.
    let descriptor_key = namespace_config(namespace_id.as_str());
    let head_key = wal_head(namespace_id.as_str());
    let descriptor_exists = store
        .head(&descriptor_key)
        .await
        .map_err(|error| CoreError::store(&descriptor_key, &error))?
        .is_some();
    let head_exists = store
        .head(&head_key)
        .await
        .map_err(|error| CoreError::store(&head_key, &error))?
        .is_some();
    if descriptor_exists && head_exists {
        return Ok(AbandonedBootstrapReap::AlreadyComplete);
    }
    for key in keys {
        store
            .delete(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?;
    }
    Ok(AbandonedBootstrapReap::Reaped)
}

pub(super) async fn delete_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
    report: &mut GcReport,
) -> Result<bool, CoreError> {
    let Some(metadata) = store
        .head(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        // Already gone; nothing to count.
        return Ok(false);
    };
    let Some(last_modified_ms) = metadata.last_modified_ms else {
        // No provider timestamp: treat as young, retain (rule 1).
        report.retained_candidates += 1;
        return Ok(false);
    };
    if context.now_ms.saturating_sub(last_modified_ms) < grace_window_ms {
        report.retained_candidates += 1;
        return Ok(false);
    }
    store
        .delete(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?;
    Ok(true)
}

pub(super) async fn list_prefix<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> Result<Vec<String>, CoreError> {
    store
        .list_prefix(prefix)
        .await
        .map_err(|error| CoreError::store(prefix, &error))
}

pub(super) fn manifest_object_id_of(key: &str) -> Option<ManifestObjectId> {
    let name = key.rsplit('/').next()?;
    let object_id = name.strip_suffix(".manifest.json")?;
    ManifestObjectId::parse(object_id).ok()
}

pub(super) fn load_error(error: ControlObjectLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
}
