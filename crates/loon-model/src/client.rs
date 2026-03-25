use crate::{
    ModelClientIssue, ModelConflictArtifactLifecycleState, ModelConflictArtifactListFilter,
    ModelConflictArtifactRestoreError, ModelLocalOnlyIssue, ModelLocalOnlyObservationCandidate,
    ModelObservedRemoteInode, ModelPlannedInodeAction, ModelPlannedLocalOnlyAction,
    ModelRemoteObservationSelectionError, ModelRestoredConflictFile, ModelRestoredConflictSubtree,
    ModelRestoredConflictSubtreeEntry, ModelScheduledClientAction,
};
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, SubtreeConflictArtifactEntry};
use serde_json::json;
use std::cmp::Ordering;
use std::path::Path;

pub fn allocate_client_request_id(next_counter: u64) -> String {
    format!("client-req-{next_counter:020}")
}

pub fn reuse_or_allocate_client_request_id(
    existing_request_id: Option<&str>,
    next_counter: u64,
) -> (String, bool) {
    match existing_request_id {
        Some(existing) => (existing.to_owned(), false),
        None => (allocate_client_request_id(next_counter), true),
    }
}

pub fn retry_reuses_pending_request_id(
    pending_request_id_before_restart: Option<&str>,
    pending_request_id_after_retry: Option<&str>,
    retried_request_id: Option<&str>,
) -> bool {
    pending_request_id_before_restart.is_some()
        && pending_request_id_before_restart == pending_request_id_after_retry
        && pending_request_id_before_restart == retried_request_id
}

pub fn duplicate_response_delivery_is_idempotent(
    winner_already_durable: bool,
    second_delivery_applied_cleanly: bool,
    duplicate_winner_apply_count: u64,
) -> bool {
    winner_already_durable && second_delivery_applied_cleanly && duplicate_winner_apply_count == 0
}

pub fn late_remote_observation_converges_once(
    response_apply_succeeded: bool,
    observation_apply_succeeded: bool,
    winner_apply_count: u64,
) -> bool {
    response_apply_succeeded && observation_apply_succeeded && winner_apply_count == 1
}

pub fn response_after_newer_observation_is_idempotent(
    observation_apply_succeeded: bool,
    delayed_response_applied_cleanly: bool,
    delayed_response_changed_state: bool,
    winner_apply_count: u64,
) -> bool {
    observation_apply_succeeded
        && delayed_response_applied_cleanly
        && !delayed_response_changed_state
        && winner_apply_count == 1
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn bound_file_observation_plans_upload_local_edit(
    planned_decision: &str,
    exists_on_disk: bool,
    dirty: bool,
) -> bool {
    exists_on_disk && dirty && planned_decision == "upload_local_edit"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn converged_bound_file_reobservation_is_noop(
    planned_decision: &str,
    exists_on_disk: bool,
    dirty: bool,
) -> bool {
    exists_on_disk && !dirty && planned_decision == "no_op"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn local_only_observation_under_bound_parent_plans_upload_local_create(
    planned_decision: &str,
    parent_is_bound_directory: bool,
    exists_on_disk: bool,
    dirty: bool,
) -> bool {
    parent_is_bound_directory
        && exists_on_disk
        && dirty
        && planned_decision == "upload_local_create"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn repeated_local_only_observation_reuses_identity(
    first_client_file_id: &str,
    second_client_file_id: &str,
) -> bool {
    !first_client_file_id.is_empty() && first_client_file_id == second_client_file_id
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn bound_file_delete_observation_plans_delete_file(
    planned_decision: &str,
    exists_on_disk: bool,
    dirty: bool,
) -> bool {
    !exists_on_disk && dirty && planned_decision == "delete_file"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn bound_directory_delete_observation_plans_delete_subtree(
    planned_decision: &str,
    exists_on_disk: bool,
    dirty: bool,
) -> bool {
    !exists_on_disk && dirty && planned_decision == "delete_subtree"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn bound_move_observation_plans_rename(
    planned_decision: &str,
    exists_on_disk: bool,
    dirty: bool,
    path_changed: bool,
) -> bool {
    exists_on_disk && dirty && path_changed && planned_decision == "rename"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn local_only_delete_clears_temp_state(
    local_only_row_removed: bool,
    planned_action_removed: bool,
) -> bool {
    local_only_row_removed && planned_action_removed
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn local_only_move_preserves_identity(
    original_client_file_id: &str,
    moved_client_file_id: &str,
) -> bool {
    !original_client_file_id.is_empty() && original_client_file_id == moved_client_file_id
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_observation_updates_bound_edit_and_local_only_create(
    bound_planned_decision: &str,
    local_only_planned_decision: &str,
) -> bool {
    bound_planned_decision == "upload_local_edit"
        && local_only_planned_decision == "upload_local_create"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_missing_bound_file_plans_delete_file(planned_decision: &str) -> bool {
    planned_decision == "delete_file"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_missing_bound_directory_plans_delete_subtree(
    planned_decision: &str,
) -> bool {
    planned_decision == "delete_subtree"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_missing_local_only_directory_clears_subtree(
    removed_client_file_count: usize,
) -> bool {
    removed_client_file_count > 0
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_repeat_reuses_local_only_identity(
    first_client_file_id: &str,
    second_client_file_id: &str,
) -> bool {
    repeated_local_only_observation_reuses_identity(first_client_file_id, second_client_file_id)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_unique_bound_file_move_is_paired(
    planned_decision: &str,
    paired_bound_move_count: usize,
) -> bool {
    planned_decision == "rename" && paired_bound_move_count == 1
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_unique_local_only_file_move_preserves_identity(
    first_client_file_id: &str,
    second_client_file_id: &str,
    paired_local_only_move_count: usize,
) -> bool {
    paired_local_only_move_count == 1
        && local_only_move_preserves_identity(first_client_file_id, second_client_file_id)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_ambiguous_file_pairing_fails_closed(error_kind: &str) -> bool {
    error_kind == "ambiguous_move_pairing"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_unique_bound_directory_move_is_paired(
    planned_decision: &str,
    paired_bound_move_count: usize,
) -> bool {
    planned_decision == "rename" && paired_bound_move_count == 1
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_unique_local_only_directory_move_preserves_identity(
    original_root_client_file_id: &str,
    moved_root_client_file_id: &str,
    original_child_client_file_id: &str,
    moved_child_client_file_id: &str,
    paired_local_only_move_count: usize,
) -> bool {
    paired_local_only_move_count == 1
        && local_only_move_preserves_identity(
            original_root_client_file_id,
            moved_root_client_file_id,
        )
        && local_only_move_preserves_identity(
            original_child_client_file_id,
            moved_child_client_file_id,
        )
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_ambiguous_directory_pairing_fails_closed(error_kind: &str) -> bool {
    error_kind == "ambiguous_move_pairing"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_directory_move_with_descendant_drift_does_not_pair(
    paired_bound_move_count: usize,
    paired_local_only_move_count: usize,
) -> bool {
    paired_bound_move_count == 0 && paired_local_only_move_count == 0
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn recursive_subtree_changed_digest_file_does_not_pair(
    paired_bound_move_count: usize,
    paired_local_only_move_count: usize,
) -> bool {
    paired_bound_move_count == 0 && paired_local_only_move_count == 0
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn subtree_observation_batch_rollback_is_atomic<T>(
    before_failed_batch: &T,
    after_failed_batch: &T,
) -> bool
where
    T: PartialEq,
{
    before_failed_batch == after_failed_batch
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_create_then_write_reduces_to_observe_local(intent_kinds: &[&str]) -> bool {
    intent_kinds == ["observe_local"]
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_rename_reduces_to_observe_move(intent_kinds: &[&str]) -> bool {
    intent_kinds == ["observe_move"]
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_repeated_edits_reduce_to_one_subtree(
    intent_kinds: &[&str],
    relative_paths: &[&str],
) -> bool {
    intent_kinds == ["observe_subtree"]
        && relative_paths.len() == 1
        && !relative_paths[0].is_empty()
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_delete_burst_reduces_to_highest_root_delete(
    intent_kinds: &[&str],
    relative_paths: &[&str],
    expected_root: &str,
) -> bool {
    intent_kinds == ["observe_delete"] && relative_paths == [expected_root]
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_atomic_save_returns_error(reason_code: &str) -> bool {
    reason_code == "contradictory_path_events"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_conflicting_rename_edges_return_error(reason_code: &str) -> bool {
    reason_code == "ambiguous_rename_source"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_conflicting_native_id_reuse_returns_error(reason_code: &str) -> bool {
    reason_code == "ambiguous_native_object_id"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn fs_event_descendants_under_root_move_or_delete_are_absorbed(
    intent_kinds: &[&str],
    intent_count: usize,
) -> bool {
    intent_count == 1 && matches!(intent_kinds, ["observe_move"] | ["observe_delete"])
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn sync_until_idle_stops_on_no_work(
    final_outcome: &str,
    steps_run: usize,
    max_steps: u64,
) -> bool {
    final_outcome == "no_work" && steps_run > 0 && (steps_run as u64) <= max_steps
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn sync_until_idle_fails_on_max_steps(steps_run: usize, max_steps: u64) -> bool {
    max_steps > 0 && steps_run == max_steps as usize
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn authoritative_snapshot_import_discovers_remote_only_state(
    translated_observation_count: usize,
    discovered_remote_only_count: usize,
) -> bool {
    translated_observation_count == discovered_remote_only_count
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn authoritative_snapshot_import_replans_root_materialization(
    planned_decision: &str,
    parent_inode_id_is_none: bool,
) -> bool {
    parent_inode_id_is_none && planned_decision == "materialize_remote_dir"
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn authoritative_snapshot_import_keeps_child_waiting_for_parent(
    parent_planned_decision: &str,
    child_planned_action_present: bool,
) -> bool {
    parent_planned_decision == "materialize_remote_dir" && !child_planned_action_present
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn authoritative_snapshot_import_is_idempotent(
    translated_observation_count: usize,
    ignored_stale_count: usize,
    changed_outcome_count: usize,
) -> bool {
    translated_observation_count == ignored_stale_count && changed_outcome_count == 0
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn authoritative_snapshot_import_batch_rollback_is_atomic<T>(
    before_failed_batch: &T,
    after_failed_batch: &T,
) -> bool
where
    T: PartialEq,
{
    before_failed_batch == after_failed_batch
}

pub fn delivery_order_is_seed_stable<T>(first: &[T], second: &[T]) -> bool
where
    T: PartialEq,
{
    first == second
}

pub fn stale_writer_stays_fenced_after_handover(
    stale_publish_rejected: bool,
    head_fence_matches_lease: bool,
    stale_writer_fence_token: loon_types::FenceToken,
    active_fence_token: loon_types::FenceToken,
) -> bool {
    stale_publish_rejected
        && head_fence_matches_lease
        && stale_writer_fence_token != active_fence_token
}

pub fn checkpoint_publish_waits_for_required_progress(
    first_publish_blocked: bool,
    second_publish_succeeded: bool,
) -> bool {
    first_publish_blocked && second_publish_succeeded
}

pub fn checkpoint_publish_head_summary_is_monotonic(
    snapshot_hint_before: Option<ChangeSeq>,
    snapshot_hint_after_blocked: Option<ChangeSeq>,
    snapshot_hint_after_success: Option<ChangeSeq>,
    retention_floor_before: ChangeSeq,
    retention_floor_after_blocked: ChangeSeq,
    retention_floor_after_success: ChangeSeq,
) -> bool {
    snapshot_hint_after_blocked >= snapshot_hint_before
        && snapshot_hint_after_success >= snapshot_hint_after_blocked
        && retention_floor_after_blocked >= retention_floor_before
        && retention_floor_after_success >= retention_floor_after_blocked
}

pub fn snapshot_repair_tracks_latest_visible_head_seq(
    repaired_through_seq: Option<ChangeSeq>,
    latest_visible_head_seq: ChangeSeq,
) -> bool {
    repaired_through_seq == Some(latest_visible_head_seq)
}

pub fn checkpoint_publish_uses_latest_visible_head(
    published_snapshot_hint_seq: Option<ChangeSeq>,
    latest_visible_head_seq: ChangeSeq,
) -> bool {
    published_snapshot_hint_seq == Some(latest_visible_head_seq)
}

pub fn stale_writer_fence_survives_inflight_client_request(
    stale_publish_rejected: bool,
    inflight_client_request_present: bool,
    delayed_client_response_converged: bool,
) -> bool {
    stale_publish_rejected && inflight_client_request_present && delayed_client_response_converged
}

pub fn download_transfer_id(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "download:{}:{}:{}",
        namespace_id.as_str(),
        inode_id.0,
        content_manifest_digest
    )
}

pub fn upload_transfer_id(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "upload:{}:{}:{}",
        namespace_id.as_str(),
        inode_id.0,
        content_manifest_digest
    )
}

pub fn local_only_upload_transfer_id(
    client_file_id: &str,
    content_manifest_digest: &str,
) -> String {
    format!("upload-local-only:{client_file_id}:{content_manifest_digest}")
}

pub fn expected_download_staged_size(block_sizes: &[u64], next_block_index: u64) -> u64 {
    let clamped = usize::try_from(next_block_index)
        .unwrap_or(usize::MAX)
        .min(block_sizes.len());
    block_sizes.iter().take(clamped).sum()
}

pub fn reconcile_download_resume_block_index(
    requested_block_index: u64,
    block_sizes: &[u64],
    staged_size_bytes: u64,
) -> u64 {
    let clamped = requested_block_index.min(block_sizes.len() as u64);
    if staged_size_bytes == expected_download_staged_size(block_sizes, clamped) {
        clamped
    } else {
        0
    }
}

pub fn reconcile_upload_resume_block_index(
    requested_block_index: u64,
    block_count: u64,
    transfer_matches_plan: bool,
) -> u64 {
    if transfer_matches_plan {
        requested_block_index.min(block_count)
    } else {
        0
    }
}

pub fn select_next_local_only_action(
    actions: &[ModelPlannedLocalOnlyAction],
) -> Option<ModelPlannedLocalOnlyAction> {
    actions
        .iter()
        .min_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.client_file_id.cmp(&right.client_file_id))
        })
        .cloned()
}

pub fn select_next_client_action(
    next_local_only: Option<&ModelPlannedLocalOnlyAction>,
    next_executable_inode_action: Option<&ModelPlannedInodeAction>,
    next_deferred_inode_action: Option<&ModelPlannedInodeAction>,
) -> Option<ModelScheduledClientAction> {
    match (
        next_local_only,
        next_executable_inode_action,
        next_deferred_inode_action,
    ) {
        (Some(local_only), _, _) => Some(ModelScheduledClientAction::LocalOnlyCreate(
            local_only.clone(),
        )),
        (None, Some(inode_action), _) => Some(ModelScheduledClientAction::PlannedInodeAction(
            inode_action.clone(),
        )),
        (None, None, Some(inode_action)) => Some(ModelScheduledClientAction::PlannedInodeAction(
            inode_action.clone(),
        )),
        (None, None, None) => None,
    }
}

pub fn remote_observation_is_stale(
    current_observed_seq: Option<ChangeSeq>,
    incoming_observed_seq: ChangeSeq,
) -> bool {
    matches!(current_observed_seq, Some(current) if incoming_observed_seq <= current)
}

pub fn bound_local_matches_remote_observation(
    inode_kind: &loon_types::InodeKind,
    content_digest: Option<&str>,
    parent_inode_id: Option<InodeId>,
    display_name: &str,
    exists_on_disk: bool,
    observed: &ModelObservedRemoteInode,
) -> bool {
    exists_on_disk
        && !observed.is_deleted
        && observed.inode_kind == *inode_kind
        && observed.content_digest.as_deref() == content_digest
        && observed.parent_inode_id == parent_inode_id
        && observed.display_name == display_name
}

pub fn local_only_matches_remote_observation(
    candidate: &ModelLocalOnlyObservationCandidate,
    observed: &ModelObservedRemoteInode,
) -> bool {
    candidate.exists_on_disk
        && !observed.is_deleted
        && candidate.namespace_id == observed.namespace_id
        && candidate.inode_kind == observed.inode_kind
        && candidate.content_digest == observed.content_digest
        && candidate.parent_inode_id == observed.parent_inode_id
        && candidate.display_name == observed.display_name
}

pub fn remote_only_discovery_supported(observed: &ModelObservedRemoteInode) -> bool {
    matches!(observed.inode_kind, InodeKind::File | InodeKind::Dir) && !observed.is_deleted
}

pub fn remote_only_placeholder_matches_remote_observation(
    inode_kind: &loon_types::InodeKind,
    parent_inode_id: Option<InodeId>,
    display_name: &str,
    exists_on_disk: bool,
    dirty: bool,
    observed: &ModelObservedRemoteInode,
) -> bool {
    !exists_on_disk
        && !dirty
        && !observed.is_deleted
        && observed.inode_kind == *inode_kind
        && observed.parent_inode_id == parent_inode_id
        && observed.display_name == display_name
}

pub fn select_local_only_observation_bind_candidate(
    candidates: &[ModelLocalOnlyObservationCandidate],
    observed: &ModelObservedRemoteInode,
) -> Result<Option<String>, ModelRemoteObservationSelectionError> {
    let mut matches = candidates
        .iter()
        .filter(|candidate| local_only_matches_remote_observation(candidate, observed))
        .map(|candidate| candidate.client_file_id.clone());
    let first = matches.next();
    let second = matches.next();
    match (first, second) {
        (None, _) => Ok(None),
        (Some(client_file_id), None) => Ok(Some(client_file_id)),
        (Some(_), Some(_)) => {
            let total_matches = candidates
                .iter()
                .filter(|candidate| local_only_matches_remote_observation(candidate, observed))
                .count();
            Err(
                ModelRemoteObservationSelectionError::AmbiguousLocalOnlyBind {
                    matches: total_matches,
                },
            )
        }
    }
}

pub fn upsert_client_issue(
    issues: &[ModelClientIssue],
    next_issue: ModelClientIssue,
) -> Vec<ModelClientIssue> {
    let mut next = issues
        .iter()
        .filter(|issue| {
            issue.namespace_id != next_issue.namespace_id
                || issue.inode_id != next_issue.inode_id
                || issue.kind != next_issue.kind
        })
        .cloned()
        .collect::<Vec<_>>();
    next.push(next_issue);
    next.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.kind.cmp(&right.kind))
    });
    next
}

pub fn upsert_local_only_issue(
    issues: &[ModelLocalOnlyIssue],
    next_issue: ModelLocalOnlyIssue,
) -> Vec<ModelLocalOnlyIssue> {
    let mut next: Vec<ModelLocalOnlyIssue> = issues
        .iter()
        .filter(|issue| {
            issue.client_file_id != next_issue.client_file_id || issue.kind != next_issue.kind
        })
        .cloned()
        .collect();
    next.push(next_issue);
    next.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.kind.cmp(&right.kind))
    });
    next
}

pub fn model_restore_file_conflict_artifact(
    destination_path: &str,
    destination_exists: bool,
    destination_parent_exists: bool,
    destination_parent_is_dir: bool,
    loser_content_manifest_digest: Option<&str>,
) -> Result<ModelRestoredConflictFile, ModelConflictArtifactRestoreError> {
    validate_restore_destination(
        destination_path,
        destination_exists,
        destination_parent_exists,
        destination_parent_is_dir,
    )?;
    let content_manifest_digest = loser_content_manifest_digest
        .filter(|digest| !digest.is_empty())
        .ok_or(ModelConflictArtifactRestoreError::FileArtifactMissingContentManifest)?;
    Ok(ModelRestoredConflictFile {
        destination_path: destination_path.to_owned(),
        content_manifest_digest: content_manifest_digest.to_owned(),
    })
}

pub fn model_conflict_artifact_lifecycle_state(
    archived_at_ms: Option<u64>,
) -> ModelConflictArtifactLifecycleState {
    match archived_at_ms {
        Some(_) => ModelConflictArtifactLifecycleState::Archived,
        None => ModelConflictArtifactLifecycleState::Active,
    }
}

pub fn model_archive_conflict_artifact() -> ModelConflictArtifactLifecycleState {
    ModelConflictArtifactLifecycleState::Archived
}

pub fn model_unarchive_conflict_artifact() -> ModelConflictArtifactLifecycleState {
    ModelConflictArtifactLifecycleState::Active
}

pub fn model_conflict_artifact_matches_filter(
    lifecycle_state: ModelConflictArtifactLifecycleState,
    filter: ModelConflictArtifactListFilter,
) -> bool {
    match filter {
        ModelConflictArtifactListFilter::Active => {
            lifecycle_state == ModelConflictArtifactLifecycleState::Active
        }
        ModelConflictArtifactListFilter::All => true,
        ModelConflictArtifactListFilter::Archived => {
            lifecycle_state == ModelConflictArtifactLifecycleState::Archived
        }
    }
}

pub fn model_restore_subtree_conflict_artifact(
    destination_root: &str,
    destination_exists: bool,
    destination_parent_exists: bool,
    destination_parent_is_dir: bool,
    entries: &[SubtreeConflictArtifactEntry],
) -> Result<ModelRestoredConflictSubtree, ModelConflictArtifactRestoreError> {
    validate_restore_destination(
        destination_root,
        destination_exists,
        destination_parent_exists,
        destination_parent_is_dir,
    )?;

    let mut restored_entries = Vec::with_capacity(entries.len());
    let mut previous: Option<&SubtreeConflictArtifactEntry> = None;
    for entry in entries {
        if entry.relative_path.is_empty() {
            return Err(ModelConflictArtifactRestoreError::SubtreeEntryMissingRelativePath);
        }
        if let Some(previous_entry) = previous {
            if compare_subtree_entries(previous_entry, entry) != Ordering::Less {
                return Err(ModelConflictArtifactRestoreError::SubtreeEntriesUnordered {
                    previous_relative_path: previous_entry.relative_path.clone(),
                    current_relative_path: entry.relative_path.clone(),
                });
            }
        }
        if entry.inode_kind == InodeKind::File && entry.content_manifest_digest.is_none() {
            return Err(
                ModelConflictArtifactRestoreError::SubtreeFileEntryMissingContentManifest {
                    relative_path: entry.relative_path.clone(),
                },
            );
        }
        restored_entries.push(ModelRestoredConflictSubtreeEntry {
            relative_path: entry.relative_path.clone(),
            inode_kind: entry.inode_kind.clone(),
            content_manifest_digest: entry.content_manifest_digest.clone(),
        });
        previous = Some(entry);
    }

    Ok(ModelRestoredConflictSubtree {
        destination_root: destination_root.to_owned(),
        entries: restored_entries,
    })
}

pub fn remote_observation_bind_ambiguous_issue(
    observed: &ModelObservedRemoteInode,
    matches: usize,
    created_at_ms: u64,
) -> ModelClientIssue {
    ModelClientIssue {
        namespace_id: observed.namespace_id.clone(),
        inode_id: observed.inode_id,
        kind: "remote_observation_bind_ambiguous".to_owned(),
        summary: format!(
            "ambiguous remote observation bind matched {matches} local-only candidates"
        ),
        detail_json: json!({
            "matches": matches,
            "observed_seq": observed.observed_seq.0,
            "revision_no": observed.revision_no.0,
            "inode_kind": match &observed.inode_kind {
                InodeKind::File => "file",
                InodeKind::Dir => "dir",
                InodeKind::Symlink => "symlink",
                InodeKind::Mount => "mount",
            },
            "parent_inode_id": observed.parent_inode_id.map(|inode_id| inode_id.0),
            "display_name": observed.display_name.clone(),
        }),
        created_at_ms,
    }
}

fn validate_restore_destination(
    destination_path: &str,
    destination_exists: bool,
    destination_parent_exists: bool,
    destination_parent_is_dir: bool,
) -> Result<(), ModelConflictArtifactRestoreError> {
    if destination_exists {
        return Err(ModelConflictArtifactRestoreError::DestinationExists {
            destination_path: destination_path.to_owned(),
        });
    }
    let path = Path::new(destination_path);
    if path.file_name().is_none() {
        return Err(
            ModelConflictArtifactRestoreError::DestinationPathMissingLeaf {
                destination_path: destination_path.to_owned(),
            },
        );
    }
    if !destination_parent_exists {
        return Err(
            ModelConflictArtifactRestoreError::DestinationParentMissing {
                destination_path: destination_path.to_owned(),
            },
        );
    }
    if !destination_parent_is_dir {
        return Err(
            ModelConflictArtifactRestoreError::DestinationParentNotDirectory {
                destination_path: destination_path.to_owned(),
            },
        );
    }
    Ok(())
}

fn compare_subtree_entries(
    left: &SubtreeConflictArtifactEntry,
    right: &SubtreeConflictArtifactEntry,
) -> Ordering {
    left.relative_path
        .cmp(&right.relative_path)
        .then_with(|| left.client_file_id.cmp(&right.client_file_id))
        .then_with(|| left.inode_id.cmp(&right.inode_id))
}

pub fn local_apply_failed_issue(
    namespace_id: &loon_types::NamespaceId,
    inode_id: InodeId,
    kind: &str,
    summary: &str,
    detail_json: serde_json::Value,
    created_at_ms: u64,
) -> ModelClientIssue {
    ModelClientIssue {
        namespace_id: namespace_id.clone(),
        inode_id,
        kind: kind.to_owned(),
        summary: summary.to_owned(),
        detail_json,
        created_at_ms,
    }
}

pub fn upload_failed_issue(
    namespace_id: &loon_types::NamespaceId,
    inode_id: InodeId,
    kind: &str,
    summary: &str,
    detail_json: serde_json::Value,
    created_at_ms: u64,
) -> ModelClientIssue {
    ModelClientIssue {
        namespace_id: namespace_id.clone(),
        inode_id,
        kind: kind.to_owned(),
        summary: summary.to_owned(),
        detail_json,
        created_at_ms,
    }
}

#[cfg(test)]
pub fn transfer_reset_issue(
    namespace_id: &loon_types::NamespaceId,
    inode_id: InodeId,
    kind: &str,
    summary: &str,
    reason: &str,
    created_at_ms: u64,
) -> ModelClientIssue {
    ModelClientIssue {
        namespace_id: namespace_id.clone(),
        inode_id,
        kind: kind.to_owned(),
        summary: summary.to_owned(),
        detail_json: json!({
            "reason": reason,
        }),
        created_at_ms,
    }
}

pub fn local_only_upload_failed_issue(
    client_file_id: &str,
    namespace_id: &loon_types::NamespaceId,
    kind: &str,
    summary: &str,
    detail_json: serde_json::Value,
    created_at_ms: u64,
) -> ModelLocalOnlyIssue {
    ModelLocalOnlyIssue {
        client_file_id: client_file_id.to_owned(),
        namespace_id: namespace_id.clone(),
        kind: kind.to_owned(),
        summary: summary.to_owned(),
        detail_json,
        created_at_ms,
    }
}

#[cfg(test)]
pub fn local_only_transfer_reset_issue(
    client_file_id: &str,
    namespace_id: &loon_types::NamespaceId,
    kind: &str,
    summary: &str,
    reason: &str,
    created_at_ms: u64,
) -> ModelLocalOnlyIssue {
    ModelLocalOnlyIssue {
        client_file_id: client_file_id.to_owned(),
        namespace_id: namespace_id.clone(),
        kind: kind.to_owned(),
        summary: summary.to_owned(),
        detail_json: json!({
            "reason": reason,
        }),
        created_at_ms,
    }
}

#[cfg(test)]
pub fn advance_transfer_one_block(next_block_index: u64, block_count: u64) -> (u64, bool) {
    let next = next_block_index.saturating_add(1).min(block_count);
    let completed = next >= block_count;
    (next, completed)
}
