//! Checks the parent and the name of each inode that a merge creates or
//! moves, against the head of the namespace being merged into.

use super::classify::Classifier;
use super::operations::BindingChange;
use crate::error::{CoreError, Result};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    DisplayName, InodeId, InodeKind, MergeConflictKind, MergeConflictVersion, NameKey,
};
use std::collections::{HashMap, HashSet};

/// The inodes whose binding a merge changes, by what happens to them.
struct Rebound {
    /// The directories that the merge creates or undeletes.
    created_directories: HashSet<InodeId>,
    /// The inodes that the merge deletes or moves, which leave their names.
    leaving: HashSet<InodeId>,
}

/// What the check of one create or move found. On success, it's the inode
/// that has the name and that the merge deletes or moves, if one has the
/// name. On a conflict, it's the conflict's kind and the merged-into
/// namespace's version that the conflict reports.
type Target =
    std::result::Result<Option<InodeId>, (MergeConflictKind, Option<MergeConflictVersion>)>;

impl<S: ObjectStore + ?Sized> Classifier<'_, S> {
    /// Checks each create and move. Its parent has to be a directory that the
    /// merge creates or a visible directory in the merged-into namespace.
    /// Its name has to be free there, or the inode that has the name has to
    /// be one that the merge deletes or moves. Returns, for each inode that
    /// takes such a name, the inode that has the name, so the commit frees
    /// the name first.
    pub(super) async fn check_targets(&mut self) -> Result<HashMap<InodeId, InodeId>> {
        let rebound = self.rebound();
        let mut holders = HashMap::new();
        for index in 0..self.plans.len() {
            let plan = &self.plans[index];
            let inode_id = plan.inode_id;
            let (parent_inode_id, display_name) = match &plan.binding {
                Some(
                    BindingChange::Create {
                        parent_inode_id,
                        display_name,
                        ..
                    }
                    | BindingChange::Move {
                        parent_inode_id,
                        display_name,
                        ..
                    }
                    | BindingChange::Undelete {
                        parent_inode_id,
                        display_name,
                        ..
                    },
                ) => (*parent_inode_id, display_name.clone()),
                _ => continue,
            };
            match self
                .check_target(&rebound, inode_id, parent_inode_id, &display_name)
                .await?
            {
                Ok(None) => {}
                Ok(Some(holder)) => {
                    holders.insert(inode_id, holder);
                }
                Err((kind, theirs)) => self.target_conflict(inode_id, kind, theirs).await?,
            }
        }
        // The commit can't free a name when the change of the inode that has
        // the name conflicts, so the inode that takes the name conflicts too.
        loop {
            let blocked: Vec<_> = holders
                .iter()
                .filter(|(taker, holder)| {
                    self.conflicted.contains(*holder) && !self.conflicted.contains(*taker)
                })
                .map(|(taker, holder)| (*taker, *holder))
                .collect();
            if blocked.is_empty() {
                return Ok(holders);
            }
            for (taker, holder) in blocked {
                let theirs = self
                    .theirs
                    .state(holder)
                    .await?
                    .and_then(|state| state.conflict_version());
                self.target_conflict(taker, MergeConflictKind::Name, theirs)
                    .await?;
            }
        }
    }

    /// Records the conflict of a create or move of `inode_id`.
    async fn target_conflict(
        &mut self,
        inode_id: InodeId,
        kind: MergeConflictKind,
        theirs: Option<MergeConflictVersion>,
    ) -> Result<()> {
        let ours = self.ours.state(inode_id).await?.ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "fork `{}` doesn't have inode `{inode_id}`, which it touched",
                self.record.source_namespace_id
            ))
        })?;
        self.conflict(inode_id, kind, &ours, theirs).await
    }

    fn rebound(&self) -> Rebound {
        let mut rebound = Rebound {
            created_directories: HashSet::new(),
            leaving: HashSet::new(),
        };
        for plan in &self.plans {
            match (&plan.binding, plan.kind) {
                (
                    Some(BindingChange::Create { .. } | BindingChange::Undelete { .. }),
                    InodeKind::Directory,
                ) => {
                    rebound.created_directories.insert(plan.inode_id);
                }
                (Some(BindingChange::Delete { .. } | BindingChange::Move { .. }), _) => {
                    rebound.leaving.insert(plan.inode_id);
                }
                _ => {}
            }
        }
        rebound
    }

    /// Checks one create or move of `inode_id` to `display_name` in
    /// `parent_inode_id`.
    async fn check_target(
        &mut self,
        rebound: &Rebound,
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Result<Target> {
        if rebound.created_directories.contains(&parent_inode_id) {
            return Ok(Ok(None));
        }
        let parent_visible = self
            .theirs
            .session()
            .visible_inode(parent_inode_id)
            .await?
            .is_some_and(|parent| parent.inode_kind == InodeKind::Directory);
        if !parent_visible {
            let theirs = self
                .theirs
                .state(inode_id)
                .await?
                .and_then(|state| state.conflict_version());
            return Ok(Err((MergeConflictKind::Visibility, theirs)));
        }
        let name_key = NameKey::for_display_name(display_name);
        let Some(holder) = self
            .theirs
            .view()
            .visible_child(parent_inode_id, &name_key)
            .await?
        else {
            return Ok(Ok(None));
        };
        let holder = holder.child_inode_id;
        if holder == inode_id {
            return Ok(Ok(None));
        }
        if rebound.leaving.contains(&holder) {
            return Ok(Ok(Some(holder)));
        }
        let theirs = self
            .theirs
            .state(holder)
            .await?
            .and_then(|state| state.conflict_version());
        Ok(Err((MergeConflictKind::Name, theirs)))
    }
}
