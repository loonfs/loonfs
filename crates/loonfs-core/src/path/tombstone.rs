use super::helpers::map_path_error_to_core;
use crate::error::CoreError;
use crate::metadata::MetadataState;
use loonfs_api::{AbsolutePath, ChangeSeq, DisplayName, InodeId, NameKey};

pub(crate) fn reject_tombstoned_path_ancestor(
    metadata_state: &MetadataState,
    absolute_path: &AbsolutePath,
    name_policy: loonfs_api::NamePolicy,
    seq: ChangeSeq,
) -> Result<(), CoreError> {
    let Some((path, root_inode, tombstone_seq)) =
        tombstoned_path_ancestor(metadata_state, absolute_path, name_policy, seq)?
    else {
        return Ok(());
    };
    Err(CoreError::TombstoneConflict {
        path,
        root_inode,
        tombstone_seq,
    })
}

fn tombstoned_path_ancestor(
    metadata_state: &MetadataState,
    absolute_path: &AbsolutePath,
    name_policy: loonfs_api::NamePolicy,
    seq: ChangeSeq,
) -> Result<Option<(String, InodeId, ChangeSeq)>, CoreError> {
    let mut current_inode = InodeId(1);
    let mut current_path = AbsolutePath::root();

    for component in absolute_path.components() {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(name_policy, &display_name);
        let Some(bound_child) =
            metadata_state.bound_child_at_seq(current_inode, name_key.as_str(), seq)
        else {
            return Ok(None);
        };
        let visible_component =
            DisplayName::parse(&bound_child.display_name).map_err(map_path_error_to_core)?;
        let visible_path = current_path.join(&visible_component);
        if let Some(tombstone) =
            metadata_state.covering_subtree_tombstone(bound_child.child_inode_id, seq)
        {
            return Ok(Some((
                visible_path.as_str().to_owned(),
                tombstone.root_inode_id,
                tombstone.tombstone_seq,
            )));
        }
        let Some(latest_binding) =
            metadata_state.current_parent_binding_for_child(bound_child.child_inode_id, seq)
        else {
            return Ok(None);
        };
        if latest_binding.parent_inode_id != bound_child.parent_inode_id
            || latest_binding.name_key != bound_child.name_key
            || latest_binding.bind_seq != bound_child.bind_seq
            || latest_binding.bind_delta_index != bound_child.bind_delta_index
        {
            return Ok(None);
        }

        if metadata_state
            .visible_inode(bound_child.child_inode_id, seq)
            .is_none()
        {
            return Ok(None);
        }
        current_path = visible_path;
        current_inode = bound_child.child_inode_id;
    }

    Ok(None)
}
