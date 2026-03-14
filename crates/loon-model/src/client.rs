use crate::{
    ModelLocalOnlyObservationCandidate, ModelObservedRemoteInode, ModelPlannedInodeAction,
    ModelPlannedLocalOnlyAction, ModelRemoteObservationSelectionError, ModelScheduledClientAction,
};
use loon_types::{ChangeSeq, InodeId};

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
    next_inode_action: Option<&ModelPlannedInodeAction>,
) -> Option<ModelScheduledClientAction> {
    match (next_local_only, next_inode_action) {
        (Some(local_only), Some(inode_action)) => {
            if local_only.created_at_ms <= inode_action.created_at_ms {
                Some(ModelScheduledClientAction::LocalOnlyCreate(
                    local_only.clone(),
                ))
            } else {
                Some(ModelScheduledClientAction::PlannedInodeAction(
                    inode_action.clone(),
                ))
            }
        }
        (Some(local_only), None) => Some(ModelScheduledClientAction::LocalOnlyCreate(
            local_only.clone(),
        )),
        (None, Some(inode_action)) => Some(ModelScheduledClientAction::PlannedInodeAction(
            inode_action.clone(),
        )),
        (None, None) => None,
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
