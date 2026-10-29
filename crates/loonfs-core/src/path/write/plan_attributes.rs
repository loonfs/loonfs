//! The publish plan for one attribute update.

use super::planner::OperationPlanningSession;
use super::publish_path_planning::{CompiledFilesystemOperation, Target};
use crate::commit::CommitOperation;
use crate::error::{CoreError, Result};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{AccessRight, AttributeKey, AttributeValue, Attributes, AttributesRevisionNo};
use std::collections::{BTreeMap, BTreeSet};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, S> {
    pub(super) async fn plan_update_attributes(
        &self,
        target: Target<'_>,
        set: &BTreeMap<AttributeKey, AttributeValue>,
        remove: &[AttributeKey],
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    ) -> Result<CompiledFilesystemOperation> {
        validate_request_shape(set, remove)?;
        // Attributes belong to the resource, so a directory is as valid a
        // target as a file; nothing here looks at the inode kind.
        let target = self
            .view
            .resolve_authorized_target(target, AccessRight::Write.into())
            .await?;
        let inode_id = target.inode_id();
        let current = self.view.metadata.attributes(inode_id).await?;
        // A caller-supplied precondition replaces the freshly-read revision in
        // the op, so commit validation rejects a raced update with the
        // stale-attributes error and its expected/actual details.
        let base_attributes_revision_no =
            expected_attributes_revision_no.unwrap_or(current.attributes_revision_no);

        let mut updated: BTreeMap<AttributeKey, AttributeValue> =
            current.attributes.as_map().clone();
        for (key, value) in set {
            updated.insert(key.clone(), value.clone());
        }
        for key in remove {
            updated.remove(key);
        }
        let attributes = Attributes::new(updated).map_err(|error| {
            CoreError::InvalidCommitRequest(format!(
                "the resulting attribute map is invalid: {error}"
            ))
        })?;
        Ok(CompiledFilesystemOperation::new(vec![
            CommitOperation::UpdateAttributes {
                inode_id,
                base_attributes_revision_no,
                attributes,
            },
        ]))
    }
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
