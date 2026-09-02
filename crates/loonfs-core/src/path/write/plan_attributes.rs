//! The publish plan for one attribute update.

use super::ensure_expected_inode;
use super::publish_path_planning::{CompiledFilesystemOperation, PublishPathPlanningView};
use crate::commit::CommitOp;
use crate::error::{CoreError, Result};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_api::{
    AbsolutePath, AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, InodeId,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn plan_update_attributes<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    set: &BTreeMap<AttributeKey, AttributeValue>,
    remove: &[AttributeKey],
    expected_inode_id: Option<InodeId>,
    expected_attributes_revision_no: Option<AttributeRevisionNo>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    validate_request_shape(set, remove)?;

    // Attributes belong to the resource, so a directory is as valid a target
    // as a file; nothing here looks at the inode kind.
    let target = view.view.resolve_visible_path(absolute_path).await?;
    ensure_expected_inode(&target, expected_inode_id, &final_component(absolute_path)?)?;

    let (current_revision_no, current) =
        view.view.attributes_at_visible_seq(target.inode_id).await?;
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
    Ok(CompiledFilesystemOperation::new(vec![
        CommitOp::UpdateAttributes {
            inode_id: target.inode_id,
            base_attributes_revision_no,
            attributes,
        },
    ]))
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
