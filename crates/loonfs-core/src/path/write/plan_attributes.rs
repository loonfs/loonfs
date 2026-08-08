//! The publish plan for one attribute update.

use super::planning_helpers::{
    publish_binding_is_precondition, PlannedOperation, PublishPathPlanningView,
};
use crate::commit::{
    CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition, CommitValidationError,
};
use crate::error::{CoreError, Result};
use crate::path::helpers::ensure_mutation_path;
use loonfs_api::{
    AbsolutePath, AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, InodeId, NameKey,
    ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn plan_publish_update_attributes<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    set: &BTreeMap<AttributeKey, AttributeValue>,
    remove: &[AttributeKey],
    expected_inode_id: Option<InodeId>,
    expected_attributes_revision_no: Option<AttributeRevisionNo>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<PlannedOperation> {
    ensure_mutation_path(absolute_path)?;
    validate_request_shape(set, remove)?;

    // Attributes belong to the resource, so a directory is as valid a target
    // as a file; nothing here looks at the inode kind.
    let target = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await?;
    // Planning happens under the publish lock, so this check is race-free,
    // and it reports what the delete guard reports: a caller that resolved
    // the path earlier writes to that exact inode or fails, never to a raced
    // rebinding.
    if let Some(expected) = expected_inode_id {
        if target.inode_id != expected {
            return Err(CommitValidationError::BindingPreconditionMismatch {
                // The root is rejected above, so a resolved target always has
                // a parent.
                parent_inode_id: target.parent_inode_id.unwrap_or(ROOT_INODE_ID),
                name_key: NameKey::for_display_name(
                    &absolute_path
                        .final_component()
                        .expect("non-root mutation path should have a final component")
                        .to_display_name(),
                ),
                expected_child_inode_id: expected,
                actual_child_inode_id: target.inode_id,
            }
            .into());
        }
    }

    let (current_revision_no, current) = view
        .metadata_state
        .attributes_at_visible_seq(target.inode_id)
        .await?;
    // A caller-supplied guard replaces the freshly-read revision in the op,
    // so commit validation rejects a raced update with the stale-attributes
    // error and its expected/actual details.
    let base_attributes_revision_no =
        expected_attributes_revision_no.unwrap_or(current_revision_no);

    let mut updated: BTreeMap<AttributeKey, AttributeValue> = current.as_map().clone();
    for (key, value) in set {
        updated.insert(key.clone(), value.clone());
    }
    for key in remove {
        updated.remove(key);
    }
    // An update that leaves the map exactly as it was publishes nothing. This
    // is new: an identical-content put deliberately appends a revision,
    // because a file's revisions are its history. Attributes are current
    // state with no history, so a revision that states the same map is a
    // number with nothing behind it.
    if updated == *current.as_map() {
        return Err(CoreError::InvalidCommitRequest(format!(
            "the update leaves the attributes of `{}` unchanged",
            absolute_path.as_str()
        )));
    }
    let attributes = Attributes::new(updated).map_err(|error| {
        CoreError::InvalidCommitRequest(format!("the resulting attribute map is invalid: {error}"))
    })?;
    Ok(PlannedOperation::new(
        vec![ApiCommitOp::UpdateAttributes {
            inode_id: target.inode_id,
            base_attributes_revision_no,
            attributes,
        }],
        vec![
            publish_binding_is_precondition(view, &target).await?,
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: target.inode_id,
            },
        ],
    ))
}

/// Rejects requests that do not describe one coherent update, each with a
/// message naming what is wrong.
fn validate_request_shape(
    set: &BTreeMap<AttributeKey, AttributeValue>,
    remove: &[AttributeKey],
) -> Result<()> {
    if set.is_empty() && remove.is_empty() {
        return Err(CoreError::InvalidCommitRequest(
            "the update sets no attribute and removes none".to_owned(),
        ));
    }
    let mut seen_removals = BTreeSet::new();
    for key in remove {
        if !seen_removals.insert(key) {
            return Err(CoreError::InvalidCommitRequest(format!(
                "attribute `{key}` is removed more than once"
            )));
        }
        if set.contains_key(key) {
            return Err(CoreError::InvalidCommitRequest(format!(
                "attribute `{key}` is both set and removed"
            )));
        }
    }
    for key in set.keys().chain(remove) {
        if key.is_reserved() {
            return Err(CoreError::InvalidCommitRequest(format!(
                "attribute `{key}` is system-owned and cannot be written by a caller"
            )));
        }
    }
    Ok(())
}
