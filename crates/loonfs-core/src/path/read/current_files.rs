//! Answering "where is this inode now?" for a batch of inode ids.
//!
//! A consumer that holds inode ids from an earlier enumeration asks this
//! what became of them: whether each is still visible, which revision it
//! carries now, and where it currently sits. Stale ids are ordinary input —
//! a candidate generator routinely holds ids that were deleted since — so
//! an unknown id is answered, not refused.

use super::materialized_view::LoadedMetadataView;
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataViewSession, ResolvedVisiblePath};
use loonfs_api::{AbsolutePath, InodeId, InodeKind, RevisionNo, ROOT_INODE_ID};
use loonfs_objectstore::ObjectStore;
use std::collections::{HashMap, HashSet};

/// The most inode ids [`resolve_current_files`] answers in one call.
///
/// One item costs about what one row of a listing page costs, so the batch
/// is bounded by the same number: the pagination policy's maximum page limit
/// ([`loonfs_api::DEFAULT_MAX_PAGE_LIMIT`]).
pub const MAX_RESOLVE_CURRENT_FILES: usize = loonfs_api::DEFAULT_MAX_PAGE_LIMIT as usize;

/// What one inode looks like in the namespace's current state.
///
/// The shape is file-oriented but tolerant of anything an id can name: a
/// directory answers `visible` with a path and no revision, and an id that
/// names nothing answers not visible with nothing else filled in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentFileState {
    /// The inode this answer is about, echoed so batched answers stay
    /// self-describing.
    pub inode_id: InodeId,
    /// Whether the inode is currently visible: it exists, no subtree
    /// tombstone covers it or an ancestor, and its bindings still reach the
    /// namespace root.
    pub visible: bool,
    /// The current revision number — `Some` only for a visible file that
    /// has one.
    pub current_revision_no: Option<RevisionNo>,
    /// Where the inode currently sits — `Some` exactly when `visible`.
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

/// Resolves every inode id against one loaded view, in input order.
///
/// The whole batch reads one view, so every answer describes the same
/// namespace state. Ancestor walks are shared: the batch memoizes each
/// ancestor directory's path the first time a walk passes through it, so
/// files under one directory pay for that chain once — the walking cost is
/// bounded by the number of distinct ancestors in the batch, not by the
/// number of items.
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

/// Resolves one currently visible inode through the child-keyed binding
/// index, returning the same canonical identity projection a path walk
/// produces. No directory listing is involved.
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

/// Derives the inode's current path by following parent bindings up to the
/// root, then spelling the chain back out.
///
/// This is the same binding relation a path resolution walks downward, read
/// from the other end. `None` means the chain never reaches the root — a
/// binding cycle, or a dead end — which commit validation prevents; the walk
/// reports it as "not visible" rather than inventing a path for state it
/// cannot name.
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

    // `climbed` runs leaf-first; spelling it out from the known base
    // downward names every directory passed on the way, which is what the
    // next item under the same directory reuses.
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
