use crate::{
    ModelClientIssue, ModelLocalOnlyObservationCandidate, ModelObservedRemoteInode,
    ModelPlannedInodeAction, ModelPlannedLocalOnlyAction, ModelRemoteObservationSelectionError,
    ModelScheduledClientAction,
};
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId};
use serde_json::json;

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

pub fn local_apply_failed_issue(
    namespace_id: &loon_types::NamespaceId,
    inode_id: InodeId,
    kind: &str,
    summary: &str,
    operation: &str,
    path: &str,
    source: &str,
    created_at_ms: u64,
) -> ModelClientIssue {
    ModelClientIssue {
        namespace_id: namespace_id.clone(),
        inode_id,
        kind: kind.to_owned(),
        summary: summary.to_owned(),
        detail_json: json!({
            "operation": operation,
            "path": path,
            "source": source,
        }),
        created_at_ms,
    }
}
