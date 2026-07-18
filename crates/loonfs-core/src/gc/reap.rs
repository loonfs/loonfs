//! Reaping: age-gated deletion of unreachable objects and abandoned
//! bootstrap debris.

use super::config::{GcConfig, GcReport};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::namespace::control::ControlObjectLoadError;
use loonfs_api::{ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::namespace_config;
use loonfs_objectstore::ObjectStore;

/// Rule 9: a namespace tree with no `namespace.json` whose newest object is
/// older than the reap window may be reaped, re-checking the completion
/// marker's absence immediately before deleting.
pub(super) async fn reap_abandoned_bootstrap<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcReport, CoreError> {
    let namespace_prefix = format!("namespaces/{}/", namespace_id.as_str());
    let keys = list_prefix(store, &namespace_prefix).await?;
    let mut report = GcReport::default();
    if keys.is_empty() {
        return Ok(report);
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
            report.retained_candidates += u64::try_from(keys.len()).unwrap_or(u64::MAX);
            return Ok(report);
        };
        if context.now_ms.saturating_sub(last_modified_ms) < config.reap_window_ms {
            report.retained_candidates += u64::try_from(keys.len()).unwrap_or(u64::MAX);
            return Ok(report);
        }
    }

    // Re-check the absence of the completion marker immediately before
    // deleting (rule 9).
    let complete_now = store
        .head(&namespace_config(namespace_id.as_str()))
        .await
        .map_err(|error| CoreError::store(namespace_config(namespace_id.as_str()), &error))?
        .is_some();
    if complete_now {
        report.retained_candidates = u64::try_from(keys.len()).unwrap_or(u64::MAX);
        return Ok(report);
    }
    for key in keys {
        store
            .delete(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?;
        report.reaped_abandoned_objects += 1;
    }
    Ok(report)
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
