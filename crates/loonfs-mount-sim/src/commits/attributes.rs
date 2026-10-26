//! Planning attribute updates: `update_attributes` and `update_attributes_by_inode`.

use super::files::final_name;
use super::planner::{ensure_expected_inode, root_mutation, Planner};
use crate::entries::path_lookup_error;
use crate::error::{MountSimError, Result};
use loonfs_model::visibility::PathLookup;
use loonfs_types::api::v0::validate_attributes_precondition;
use loonfs_types::{
    AbsolutePath, AttributeKey, AttributeValue, Attributes, AttributesRevisionNo, ErrorCode,
    InodeId, ROOT_INODE_ID,
};
use std::collections::{BTreeMap, BTreeSet};

impl Planner<'_> {
    pub(super) fn update_attributes(
        &mut self,
        path: &AbsolutePath,
        set: &BTreeMap<AttributeKey, AttributeValue>,
        remove: &[AttributeKey],
        expected_inode_id: Option<InodeId>,
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    ) -> Result<()> {
        final_name(path)?;
        validate_changes(set, remove)?;
        validate_attributes_precondition(expected_inode_id, expected_attributes_revision_no)
            .map_err(|error| MountSimError::invalid_request(error.to_string()))?;
        let inode_id = match self.view().resolve_path(path) {
            PathLookup::Found { inode, .. } => inode.inode_id,
            lookup => return Err(path_lookup_error(&lookup)),
        };
        ensure_expected_inode(expected_inode_id, inode_id)?;
        self.write_attributes(inode_id, set, remove, expected_attributes_revision_no)
    }

    pub(super) fn update_attributes_by_inode(
        &mut self,
        inode_id: InodeId,
        set: &BTreeMap<AttributeKey, AttributeValue>,
        remove: &[AttributeKey],
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    ) -> Result<()> {
        if inode_id == ROOT_INODE_ID {
            return Err(root_mutation());
        }
        validate_changes(set, remove)?;
        self.visible(inode_id)?;
        self.write_attributes(inode_id, set, remove, expected_attributes_revision_no)
    }

    /// Writes the inode's map with `set` applied and `remove` removed, as its next revision.
    fn write_attributes(
        &mut self,
        inode_id: InodeId,
        set: &BTreeMap<AttributeKey, AttributeValue>,
        remove: &[AttributeKey],
        expected: Option<AttributesRevisionNo>,
    ) -> Result<()> {
        let current = self.view().attributes(inode_id);
        if let Some(expected) = expected.filter(|expected| *expected != current.revision_no) {
            return Err(MountSimError::new(
                ErrorCode::StaleAttributes,
                format!("the inode's attribute revision isn't the expected revision {expected}"),
            )
            .with_details(|details| {
                details.inode_id = Some(inode_id);
                details.expected_attributes_revision_no = Some(expected);
                details.actual_attributes_revision_no = Some(current.revision_no);
            }));
        }
        let mut map = current.attributes.as_map().clone();
        map.extend(set.iter().map(|(key, value)| (key.clone(), value.clone())));
        for key in remove {
            map.remove(key);
        }
        let attributes = Attributes::new(map).map_err(|error| {
            MountSimError::invalid_request(format!(
                "the resulting attribute map is invalid: {error}"
            ))
        })?;
        self.set_attributes(inode_id, current.revision_no, attributes);
        Ok(())
    }
}

/// Rejects an update that changes nothing, that sets and removes one key, that removes a key
/// twice, or that writes a key under the reserved `loonfs.` prefix.
fn validate_changes(
    set: &BTreeMap<AttributeKey, AttributeValue>,
    remove: &[AttributeKey],
) -> Result<()> {
    if set.is_empty() && remove.is_empty() {
        return Err(MountSimError::invalid_request(
            "the update sets no attribute and removes none",
        ));
    }
    let mut removed = BTreeSet::new();
    for key in remove {
        if !removed.insert(key) {
            return Err(MountSimError::invalid_request(format!(
                "attribute `{key}` is removed more than once"
            )));
        }
        if set.contains_key(key) {
            return Err(MountSimError::invalid_request(format!(
                "attribute `{key}` is both set and removed"
            )));
        }
    }
    if let Some(key) = set.keys().chain(remove).find(|key| key.is_reserved()) {
        return Err(MountSimError::invalid_request(format!(
            "attribute `{key}` is reserved for LoonFS"
        )));
    }
    Ok(())
}
