//! The GC entry point: orchestrates root collection, verification,
//! and the sweep.

use super::config::{GcConfig, GcReport};
use super::fork_checkpoints::{
    maybe_release_fork_checkpoint, release_missing_basis_checkpoint, ForkCheckpointSweep,
};
use super::live_set::{collect_live_set, SweepVerifier};
use super::reap::{delete_if_aged, list_prefix, manifest_object_id_of};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::namespace::catalog::{
    map_namespace_initialization_error_to_core, namespace_initialization_state,
    NamespaceInitializationState,
};
use loonfs_api::{ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_prefix, metadata_table_prefix, upload_session_prefix,
    wal_segment_prefix,
};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

/// Everything reachable from the fresh root set (rule 4).
pub(super) struct LiveSet {
    pub(super) manifests: BTreeSet<ManifestObjectId>,
    pub(super) tables: BTreeSet<String>,
    pub(super) wal_segments: BTreeSet<String>,
    pub(super) checkpoint_keys: BTreeSet<String>,
    /// Still-active records whose basis manifest is verifiably absent —
    /// the crash window between record write and verification. The pass
    /// releases them; they never degrade sweeping.
    pub(super) missing_basis_records: Vec<String>,
    /// Record resolution failed somewhere: manifest/table deletion must not
    /// proceed on this pass.
    pub(super) degraded: bool,
}

pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcReport, CoreError> {
    gc_namespace_with_reverify_chunk(store, namespace_id, config, context, SWEEP_REVERIFY_CHUNK)
        .await
}

/// How many sweep candidates may be decided against one live set before the
/// set is re-collected (rule 3: candidate selection may be stale, deletion
/// may not). On a large sweep, deciding every candidate against a single
/// snapshot would leave the last deletions working from an arbitrarily old
/// view; re-collecting every chunk bounds that staleness.
const SWEEP_REVERIFY_CHUNK: usize = 1024;

pub(super) async fn gc_namespace_with_reverify_chunk<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
    reverify_chunk: usize,
) -> Result<GcReport, CoreError> {
    config.validate()?;
    let initialization_state = namespace_initialization_state(store, namespace_id)
        .await
        .map_err(map_namespace_initialization_error_to_core)?;
    if initialization_state != NamespaceInitializationState::Complete {
        return Ok(GcReport {
            incomplete_namespace_ignored: true,
            ..GcReport::default()
        });
    }

    // Mark: select sweep candidates against one live-set snapshot. The
    // selection may be stale (rule 3); the delete-time checks below decide.
    let mark = collect_live_set(store, namespace_id, config, context).await?;
    let mut report = GcReport::default();

    let candidate_segments = list_prefix(store, &wal_segment_prefix(namespace_id.as_str())).await?;
    let candidate_tables =
        list_prefix(store, &metadata_table_prefix(namespace_id.as_str())).await?;
    let candidate_manifests =
        list_prefix(store, &metadata_manifest_prefix(namespace_id.as_str())).await?;
    let candidate_checkpoints =
        list_prefix(store, &checkpoint_prefix(namespace_id.as_str())).await?;
    let candidate_upload_sessions =
        list_prefix(store, &upload_session_prefix(namespace_id.as_str())).await?;

    let segment_candidates: Vec<String> = candidate_segments
        .into_iter()
        .filter(|key| !mark.wal_segments.contains(key))
        .collect();
    let table_candidates: Vec<String> = candidate_tables
        .into_iter()
        .filter(|key| !mark.tables.contains(key))
        .collect();
    let manifest_candidates: Vec<String> = candidate_manifests
        .into_iter()
        .filter(|key| manifest_object_id_of(key).is_none_or(|id| !mark.manifests.contains(&id)))
        .collect();
    let checkpoint_candidates: Vec<String> = candidate_checkpoints
        .into_iter()
        .filter(|key| !mark.checkpoint_keys.contains(key))
        .collect();

    // Sweep: every deletion is decided against a fresh live set (rule 3:
    // candidate selection may be stale, deletion may not). Zero decisions
    // separate the mark from the first sweep step, so the mark set seeds
    // the verifier directly; it is re-collected every `reverify_chunk`
    // candidates so a large sweep never runs far ahead of its snapshot.
    let mut sweep = SweepVerifier::seeded(mark, reverify_chunk);

    // Delete data before records. Every readable checkpoint record roots
    // its basis in the live set, so the pass that deletes a record can
    // never also delete the data that record was protecting. A crash
    // mid-sweep leaves orphaned data for the next pass — never a record
    // whose data vanished underneath it.
    for key in segment_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        if sweep.live.wal_segments.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_wal_segments += 1;
        }
    }
    for key in table_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        // Rule 5: ambiguous roots suppress manifest and table deletion for
        // the whole pass; degradation seen by any re-collection is sticky.
        if sweep.degraded {
            report.retained_candidates += 1;
            continue;
        }
        if sweep.live.tables.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_metadata_tables += 1;
        }
    }
    for key in manifest_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        if sweep.degraded {
            report.retained_candidates += 1;
            continue;
        }
        if manifest_object_id_of(&key).is_some_and(|id| sweep.live.manifests.contains(&id)) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_manifests += 1;
        }
    }
    for key in checkpoint_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        if sweep.live.checkpoint_keys.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        // Active fork-owned records are released by compare-and-swap,
        // never deleted outright, so a fork retrying at the same moment
        // finds a released record it can revive instead of a vanished one.
        // Everything else (released records, expired user records) goes
        // through the normal age-gated delete.
        match maybe_release_fork_checkpoint(store, &key, config, context).await? {
            ForkCheckpointSweep::Released => {
                report.released_fork_checkpoints += 1;
                continue;
            }
            ForkCheckpointSweep::Retained => {
                report.retained_candidates += 1;
                continue;
            }
            ForkCheckpointSweep::NotAnActiveFork => {}
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_checkpoint_records += 1;
        }
    }
    // Zombie release: an active record whose basis manifest is verifiably
    // gone can never serve a read — the crash window between the record
    // write and its verification skipped the release the creator would
    // have performed. The release is the same compare-and-swap create runs
    // on verification failure, decided against fresh reads and gated by
    // the grace window so an in-flight create is never raced.
    for key in sweep.live.missing_basis_records.clone() {
        if release_missing_basis_checkpoint(store, namespace_id, &key, config, context).await? {
            report.released_missing_basis_checkpoints += 1;
        } else {
            report.retained_candidates += 1;
        }
    }

    // Upload sessions root nothing and nothing durable references them, so
    // provider age past the reap window is the whole decision — the
    // AbortIncompleteMultipartUpload convention. A session that old is dead
    // whatever its state: presigned grants expired long ago, and a
    // `complete` after the window answers session-not-found by design.
    // Writer clocks are never consulted (rule 1: provider timestamps only).
    for key in candidate_upload_sessions {
        if delete_if_aged(store, &key, config.reap_window_ms, context, &mut report).await? {
            report.deleted_upload_sessions += 1;
        }
    }
    report.degraded_retention = sweep.degraded;

    Ok(report)
}
