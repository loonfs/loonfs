//! Checks that each inode that a merge changes is reachable after the
//! merge's own creates and restores, and that each name it takes is free,
//! against the head of the namespace being merged into. A delete of an inode
//! that a deletion hides there is checked here too: the rows that it replaces
//! when the merge restores a directory above the inode, and nothing when the
//! inode stays hidden, because the delete then has nothing to do and is
//! dropped.

use super::classify::Classifier;
use super::operations::BindingChange;
use crate::error::{CoreError, Result};
use crate::metadata::AncestorWalk;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    DisplayName, InodeId, InodeKind, MergeConflictKind, MergeConflictVersion, NameKey,
};
use std::collections::{BTreeMap, HashMap, HashSet};

/// What the target check found for the merge's plans.
pub(super) struct Targets {
    /// For each inode that takes a name, the inode that has the name and
    /// that the merge deletes or moves.
    pub(super) holders: HashMap<InodeId, InodeId>,
    /// For each inode whose operation needs directories that the merge
    /// creates or restores to exist first, those directories.
    pub(super) depends_on: HashMap<InodeId, Vec<InodeId>>,
}

/// The inodes whose binding a merge changes, by what happens to them.
struct Rebound {
    /// The directories that the merge creates or restores.
    created_directories: HashSet<InodeId>,
    /// The inodes that the merge deletes or moves, which leave their names.
    leaving: HashSet<InodeId>,
}

/// Whether a directory is reachable after the merge's creates and restores.
enum Reach {
    /// The directory and its ancestors are visible at the head.
    Visible,
    /// The directory is reachable once the merge creates or restores this
    /// directory, which is the directory itself or one of its ancestors.
    Through(InodeId),
    /// A deletion that the merge doesn't undo hides the directory, or the
    /// inode isn't a directory.
    Hidden,
}

/// The outcome of the checks of one plan. On success, it's the inode that
/// has the name that the plan takes and that the merge deletes or moves, if
/// one has the name. On a conflict, it's the conflict's kind and the
/// merged-into namespace's version that the conflict reports.
type Target =
    std::result::Result<Option<InodeId>, (MergeConflictKind, Option<MergeConflictVersion>)>;

impl<S: ObjectStore + ?Sized> Classifier<'_, S> {
    /// Checks each plan against the merged-into namespace's head. The
    /// current parent of an inode that's bound there, and the destination
    /// parent of a create, a move, or a restore, each have to be visible
    /// there, or reachable through a directory that the merge creates or
    /// restores. The destination name has to be free there, or the inode
    /// that has the name has to be one that the merge deletes or moves.
    pub(super) async fn check_targets(&mut self) -> Result<Targets> {
        let rebound = self.rebound();
        let mut targets = Targets {
            holders: HashMap::new(),
            depends_on: HashMap::new(),
        };
        for index in 0..self.plans.len() {
            let inode_id = self.plans[index].inode_id;
            let mut directories = Vec::new();
            match self.check_plan(&rebound, index, &mut directories).await? {
                Ok(None) => {}
                Ok(Some(holder)) => {
                    targets.holders.insert(inode_id, holder);
                }
                Err((kind, theirs)) => self.target_conflict(inode_id, kind, theirs).await?,
            }
            if !directories.is_empty() {
                targets.depends_on.insert(inode_id, directories);
            }
        }
        self.block_takers(&targets.holders).await?;
        Ok(targets)
    }

    /// Checks the plan at `index`, first at the inode's current position and
    /// then at its destination, and adds to `directories` the directories
    /// that the merge creates or restores and that the plan needs. A delete
    /// of an inode that a deletion hides at the head runs only after a
    /// restore that the merge makes above the inode, so the check reads the
    /// rows that the delete replaces once it knows that, and drops a delete
    /// that has nothing to do, because the inode stays hidden, instead of
    /// reporting a conflict.
    async fn check_plan(
        &mut self,
        rebound: &Rebound,
        index: usize,
        directories: &mut Vec<InodeId>,
    ) -> Result<Target> {
        let plan = &self.plans[index];
        let inode_id = plan.inode_id;
        let destination = plan
            .binding
            .as_ref()
            .and_then(BindingChange::destination)
            .map(|(parent_inode_id, display_name)| (parent_inode_id, display_name.clone()));
        let deletes = matches!(plan.binding, Some(BindingChange::Delete { .. }));
        let bound_at_head = !matches!(
            plan.binding,
            Some(BindingChange::Create { .. } | BindingChange::RestoreDeleted { .. })
        );
        let hidden = bound_at_head
            && self
                .theirs
                .session()
                .visible_inode(inode_id)
                .await?
                .is_none();
        if hidden {
            let parent = self
                .theirs
                .session()
                .current_parent_binding_for_child(inode_id)
                .await?
                .map(|binding| binding.parent_inode_id);
            let reach = match parent {
                Some(parent) => self.reach(rebound, parent).await?,
                None => Reach::Hidden,
            };
            match reach {
                Reach::Visible => {}
                Reach::Through(directory) => directories.push(directory),
                Reach::Hidden if deletes => {
                    // The inode stays hidden after the merge, so the delete
                    // has nothing to do.
                    self.plans[index].binding = None;
                    return Ok(Ok(None));
                }
                Reach::Hidden => return self.visibility_conflict(inode_id).await,
            }
            if deletes {
                // The delete runs after the restore that shows the inode
                // again, so the rows that it replaces are checked now (see
                // [`Classifier::plan_change`]).
                let theirs = self.theirs.state(inode_id).await?.ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "the merged-into namespace doesn't have inode `{inode_id}`, whose delete \
                         the merge planned"
                    ))
                })?;
                if let Some(kind) = self.replaced_rows_conflict(&theirs, true).await? {
                    return Ok(Err((kind, theirs.conflict_version())));
                }
            }
        }
        let Some((parent_inode_id, display_name)) = destination else {
            return Ok(Ok(None));
        };
        match self.reach(rebound, parent_inode_id).await? {
            Reach::Visible => {}
            Reach::Through(directory) => {
                if !directories.contains(&directory) {
                    directories.push(directory);
                }
            }
            Reach::Hidden => return self.visibility_conflict(inode_id).await,
        }
        self.check_name(rebound, inode_id, parent_inode_id, &display_name)
            .await
    }

    /// Returns whether `directory` is reachable after the merge's creates and
    /// restores. The walk climbs the current parent bindings at the head
    /// from `directory` to the first ancestor that the merge creates or
    /// restores or that's visible. An ancestor whose own row is unbound was
    /// deleted itself, and the merge doesn't restore it, so the walk ends
    /// there.
    async fn reach(&mut self, rebound: &Rebound, directory: InodeId) -> Result<Reach> {
        let mut walk = AncestorWalk::new(directory);
        // The walk reports a binding cycle as corrupt metadata, so it ends
        // within the namespace's inode count.
        while let Some(ancestor) = walk.next(self.theirs.session()).await? {
            if rebound.created_directories.contains(&ancestor) {
                return Ok(Reach::Through(ancestor));
            }
            if let Some(inode) = self.theirs.session().visible_inode(ancestor).await? {
                return Ok(match inode.inode_kind {
                    InodeKind::Directory => Reach::Visible,
                    InodeKind::File => Reach::Hidden,
                });
            }
        }
        Ok(Reach::Hidden)
    }

    /// Returns the `visibility` conflict of `inode_id`, with the inode's
    /// version at the head, which a hidden inode doesn't have.
    async fn visibility_conflict(&mut self, inode_id: InodeId) -> Result<Target> {
        let theirs = self
            .theirs
            .state(inode_id)
            .await?
            .and_then(|state| state.conflict_version());
        Ok(Err((MergeConflictKind::Visibility, theirs)))
    }

    /// Checks that `display_name` is free in `parent_inode_id` for
    /// `inode_id`. An inode below a directory that the merge restores keeps
    /// its name, so the check reads the name's bound row whether or not a
    /// deletion hides it. A directory that the merge creates has no rows at
    /// the head.
    async fn check_name(
        &mut self,
        rebound: &Rebound,
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Result<Target> {
        let name_key = NameKey::for_display_name(display_name);
        let Some(holder) = self
            .theirs
            .session()
            .bound_child(parent_inode_id, &name_key)
            .await?
            .filter(|binding| binding.is_bound())
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

    /// Records a `name` conflict for each inode in `holders` that takes the
    /// name of a conflicting inode, and returns how many it recorded. The
    /// commit can't free a name when the change of the inode that has the
    /// name conflicts, so the inode that takes the name conflicts too.
    ///
    /// Each pass records at least one conflict, so the loop runs at most
    /// once for each inode in `holders`.
    pub(super) async fn block_takers(
        &mut self,
        holders: &HashMap<InodeId, InodeId>,
    ) -> Result<usize> {
        let mut recorded = 0;
        loop {
            // In inode ID order, so that a replay commit's report has one
            // order.
            let blocked: BTreeMap<_, _> = holders
                .iter()
                .filter(|(taker, holder)| {
                    self.conflicted.contains(*holder) && !self.conflicted.contains(*taker)
                })
                .map(|(taker, holder)| (*taker, *holder))
                .collect();
            if blocked.is_empty() {
                return Ok(recorded);
            }
            for (taker, holder) in blocked {
                let theirs = self
                    .theirs
                    .state(holder)
                    .await?
                    .and_then(|state| state.conflict_version());
                self.target_conflict(taker, MergeConflictKind::Name, theirs)
                    .await?;
                recorded += 1;
            }
        }
    }

    /// Records the conflict of the plan of `inode_id`.
    pub(super) async fn target_conflict(
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
                    Some(BindingChange::Create { .. } | BindingChange::RestoreDeleted { .. }),
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
}
