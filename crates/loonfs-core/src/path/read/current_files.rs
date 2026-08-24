//! Resolves the current state and path of a batch of inode IDs.
//!
//! Stale or unknown IDs are returned as not visible rather than rejected.

use super::materialized_view::LoadedMetadataView;
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataViewSession, ResolvedVisiblePath};
use loonfs_api::{AbsolutePath, InodeId, InodeKind, RevisionNo, ROOT_INODE_ID};
use loonfs_objectstore::ObjectStore;
use std::collections::{HashMap, HashSet};

/// Maximum number of inode IDs accepted by a batch resolution.
pub const MAX_RESOLVE_CURRENT_FILES: usize = loonfs_api::DEFAULT_MAX_PAGE_LIMIT as usize;

/// Current namespace state for one inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentFileState {
    /// Requested inode ID.
    pub inode_id: InodeId,
    /// Whether the inode exists and has a visible path from the root.
    pub visible: bool,
    /// Current revision number for a visible file.
    pub current_revision_no: Option<RevisionNo>,
    /// Current path when visible.
    pub current_path: Option<AbsolutePath>,
}

/// Refuses an oversized batch before anything is loaded.
///
/// Called at the API boundary so an over-cap request costs no reads.
pub(crate) fn ensure_resolve_batch_within_cap(requested: usize) -> Result<()> {
    if requested > MAX_RESOLVE_CURRENT_FILES {
        return Err(CoreError::BatchTooLarge {
            requested,
            max: MAX_RESOLVE_CURRENT_FILES,
        });
    }
    Ok(())
}

/// Resolves inode IDs against one loaded view in input order.
///
/// The batch shares one namespace state and caches paths for common ancestors.
pub(crate) async fn resolve_current_files<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    inode_ids: &[InodeId],
) -> Result<Vec<CurrentFileState>> {
    ensure_resolve_batch_within_cap(inode_ids.len())?;
    let mut session = view.metadata_view().session();
    let mut ancestor_paths = HashMap::new();
    let mut states = Vec::with_capacity(inode_ids.len());
    for &inode_id in inode_ids {
        states.push(resolve_one(&mut session, &mut ancestor_paths, inode_id).await?);
    }
    Ok(states)
}

async fn resolve_one<S: ObjectStore + ?Sized>(
    session: &mut MetadataViewSession<'_, '_, S>,
    ancestor_paths: &mut HashMap<InodeId, AbsolutePath>,
    inode_id: InodeId,
) -> Result<CurrentFileState> {
    let Some(resolved) = resolve_visible_inode(session, ancestor_paths, inode_id).await? else {
        return Ok(missing(inode_id));
    };
    let current_revision_no = if resolved.inode_kind == InodeKind::File {
        session
            .latest_revision_head_of_visible(inode_id)
            .await?
            .map(|revision| revision.revision_no)
    } else {
        None
    };
    Ok(CurrentFileState {
        inode_id,
        visible: true,
        current_revision_no,
        current_path: Some(
            AbsolutePath::parse(&resolved.absolute_path).map_err(|error| {
                CoreError::NamespaceCorrupt(format!(
                    "resolved inode `{inode_id}` to invalid path `{}`: {error}",
                    resolved.absolute_path
                ))
            })?,
        ),
    })
}

/// Resolves a visible inode and its current path through parent bindings.
pub(super) async fn resolve_visible_inode<S: ObjectStore + ?Sized>(
    session: &mut MetadataViewSession<'_, '_, S>,
    ancestor_paths: &mut HashMap<InodeId, AbsolutePath>,
    inode_id: InodeId,
) -> Result<Option<ResolvedVisiblePath>> {
    let Some(inode) = session.visible_inode(inode_id).await? else {
        return Ok(None);
    };
    let Some(current_path) = current_path(session, ancestor_paths, inode_id).await? else {
        return Ok(None);
    };
    let current_binding = if inode_id == ROOT_INODE_ID {
        None
    } else {
        let Some(binding) = session.current_parent_binding_for_child(inode_id).await? else {
            return Ok(None);
        };
        Some(binding)
    };
    Ok(Some(ResolvedVisiblePath {
        absolute_path: current_path.to_string(),
        inode_id,
        inode_kind: inode.inode_kind,
        created_by: inode.created_by,
        created_at_ms: inode.created_at_ms,
        parent_inode_id: current_binding
            .as_ref()
            .map(|binding| binding.parent_inode_id),
        display_name: current_binding
            .map(|binding| binding.display_name.to_string())
            .unwrap_or_default(),
    }))
}

fn missing(inode_id: InodeId) -> CurrentFileState {
    CurrentFileState {
        inode_id,
        visible: false,
        current_revision_no: None,
        current_path: None,
    }
}

/// Derives an inode's path by following parent bindings to the root.
///
/// Returns `None` for a cycle or a chain that does not reach the root.
async fn current_path<S: ObjectStore + ?Sized>(
    session: &mut MetadataViewSession<'_, '_, S>,
    ancestor_paths: &mut HashMap<InodeId, AbsolutePath>,
    inode_id: InodeId,
) -> Result<Option<AbsolutePath>> {
    let mut climbed = Vec::new();
    let mut visited = HashSet::new();
    let mut current = inode_id;
    let base = loop {
        if current == ROOT_INODE_ID {
            break AbsolutePath::root();
        }
        if let Some(known) = ancestor_paths.get(&current) {
            break known.clone();
        }
        if !visited.insert(current) {
            return Ok(None);
        }
        let Some(binding) = session.current_parent_binding_for_child(current).await? else {
            return Ok(None);
        };
        current = binding.parent_inode_id;
        climbed.push(binding);
    };

    // Cache each path reconstructed from the known ancestor to the leaf.
    let mut path = base;
    for binding in climbed.iter().rev() {
        path = path.join(&binding.display_name);
        ancestor_paths.insert(binding.child_inode_id, path.clone());
    }
    Ok(Some(path))
}

#[cfg(test)]
mod tests {
    use super::{ensure_resolve_batch_within_cap, MAX_RESOLVE_CURRENT_FILES};
    use loonfs_api::{ErrorCode, PaginationPolicy};

    #[test]
    fn the_batch_cap_is_the_pagination_maximum() {
        assert_eq!(
            MAX_RESOLVE_CURRENT_FILES,
            PaginationPolicy::default().max_limit().get() as usize,
            "the batch cap is the page limit, not a second number"
        );
    }

    #[test]
    fn a_batch_at_the_cap_is_accepted_and_one_past_it_names_the_cap() {
        assert!(ensure_resolve_batch_within_cap(MAX_RESOLVE_CURRENT_FILES).is_ok());
        let error = ensure_resolve_batch_within_cap(MAX_RESOLVE_CURRENT_FILES + 1)
            .expect_err("one past the cap is refused");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains(&MAX_RESOLVE_CURRENT_FILES.to_string()),
            "the refusal should name the cap: {error}"
        );
    }
}
