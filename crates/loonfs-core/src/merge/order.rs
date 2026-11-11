//! The order of a merge's binding changes. Each change runs after the
//! changes that free the name it takes, create its parent, or move an inode
//! out of a directory that the merge deletes. When moves wait on each other
//! in a cycle, one of them first moves its inode to a temporary name.

use super::classify::Classifier;
use super::operations::{BindingChange, InodePlan};
use crate::commit::engine::TemporaryMove;
use crate::error::{CoreError, Result};
use crate::metadata::MetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AbsolutePath, CommitPrecondition, DestinationPrecondition, DisplayName, FilesystemOperation,
    InodeId, InodeKind, NameKey, ROOT_INODE_ID,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

/// The operations of a merge's commit, and, by operation index, the fork's
/// ID of the inode that each create makes and the legs of the moves through
/// a temporary name. The preconditions are the revisions of the files that
/// the commit deletes.
pub(super) struct MergeOperations {
    pub(super) operations: Vec<FilesystemOperation>,
    pub(super) created_inode_ids: BTreeMap<usize, InodeId>,
    pub(super) temporary_moves: BTreeMap<usize, TemporaryMove>,
    pub(super) preconditions: Vec<CommitPrecondition>,
}

/// An inode's parent and its name there.
type Edge = (InodeId, DisplayName);

impl<S: ObjectStore + ?Sized> Classifier<'_, S> {
    /// Orders the plans' binding changes, followed by their revision,
    /// attribute, and access updates in inode ID order. `holders` maps each
    /// inode that a create or a move places to the inode that has its name
    /// in the merged-into namespace.
    ///
    /// Fails with `invalid_request` when no order of the changes works, and
    /// never returns an order that it knows to be invalid.
    pub(super) async fn order(
        &mut self,
        holders: &HashMap<InodeId, InodeId>,
    ) -> Result<MergeOperations> {
        let edges = self.edges().await?;
        let mut order = Order::new(&self.plans, holders, edges, self.theirs.view());
        order.run().await?;
        let mut operations = order.operations;
        operations.extend(
            self.plans
                .iter()
                .flat_map(|plan| plan.updates.iter().cloned()),
        );
        Ok(MergeOperations {
            operations,
            created_inode_ids: order.created_inode_ids,
            temporary_moves: order.temporary_moves,
            preconditions: order.preconditions,
        })
    }

    /// Reads the merged-into namespace's edge of each inode that the merge
    /// moves or deletes, of each existing parent that it creates or moves an
    /// inode into, and of their ancestors.
    async fn edges(&mut self) -> Result<HashMap<InodeId, Edge>> {
        let mut starts = Vec::new();
        for plan in &self.plans {
            match &plan.binding {
                Some(BindingChange::Move {
                    parent_inode_id, ..
                }) => starts.extend([plan.inode_id, *parent_inode_id]),
                Some(
                    BindingChange::Create {
                        parent_inode_id, ..
                    }
                    | BindingChange::RestoreDeleted {
                        parent_inode_id, ..
                    },
                ) => starts.push(*parent_inode_id),
                Some(BindingChange::Delete { .. }) => starts.push(plan.inode_id),
                None => {}
            }
        }
        let view = self.theirs.view();
        let mut edges = HashMap::new();
        for start in starts {
            let mut inode_id = start;
            while !edges.contains_key(&inode_id) {
                let Some(binding) = view.current_parent_binding_for_child(inode_id).await? else {
                    break;
                };
                let Some(display_name) = binding.display_name() else {
                    break;
                };
                edges.insert(inode_id, (binding.parent_inode_id, display_name.clone()));
                inode_id = binding.parent_inode_id;
            }
        }
        Ok(edges)
    }
}

/// Whether a binding change can run now.
enum Readiness {
    Ready,
    /// The change waits for a change to this inode.
    Waits(InodeId),
    /// No later change can let this change run.
    Blocked,
}

/// The largest numeric suffix that a temporary name can take before the
/// planner gives up on the directory.
const MAX_TEMPORARY_NAME_SUFFIX: u32 = 8;

/// Returns the error for a merge whose changes the planner can't order.
fn unordered() -> CoreError {
    CoreError::InvalidMerge {
        param: "through_seq",
        message: "the planner couldn't order the merge's operations, so merge a shorter range of \
                  the fork's commits"
            .to_owned(),
    }
}

/// The state of a merge's binding changes as the order emits them.
struct Order<'p, 'v, S: ObjectStore + ?Sized> {
    plans: &'p [InodePlan],
    holders: &'p HashMap<InodeId, InodeId>,
    /// The merged-into namespace at its head, where a temporary name has to
    /// be free.
    theirs: MetadataView<'v, 'v, S>,
    /// Each inode's edge as the operations so far leave it, starting from
    /// the merged-into namespace's edges.
    edges: HashMap<InodeId, Edge>,
    /// The inodes that the merge creates or restores and hasn't yet.
    pending_creates: HashSet<InodeId>,
    /// The inodes that the merge moves and hasn't yet moved to their place,
    /// with the index of each one's plan.
    pending_moves: HashMap<InodeId, usize>,
    /// The inodes that the order moved to a temporary name, with the
    /// directory that each one left for it.
    temporary: HashMap<InodeId, InodeId>,
    /// The inodes that left their edge in the merged-into namespace.
    left: HashSet<InodeId>,
    /// The directories that the merge deletes and hasn't deleted yet.
    pending_deletes: HashSet<InodeId>,
    /// For each directory that the merge deletes, the "survivors": the inodes
    /// below the directory that the merge moves out of it before the delete,
    /// counting only inodes with no moved inode between them and the
    /// directory. With them, the number of survivors that the delete has
    /// already seen leave.
    survivors: HashMap<InodeId, (Vec<InodeId>, usize)>,
    /// The index in `plans` of each binding change that the order emits.
    changes: Vec<usize>,
    done: Vec<bool>,
    /// The changes that wait for a change to each inode.
    waiters: HashMap<InodeId, Vec<usize>>,
    /// The moves that another change waits for, which can take a temporary
    /// name when nothing can run.
    wanted: BTreeSet<InodeId>,
    operations: Vec<FilesystemOperation>,
    created_inode_ids: BTreeMap<usize, InodeId>,
    temporary_moves: BTreeMap<usize, TemporaryMove>,
    preconditions: Vec<CommitPrecondition>,
}

impl<'p, 'v, S: ObjectStore + ?Sized> Order<'p, 'v, S> {
    fn new(
        plans: &'p [InodePlan],
        holders: &'p HashMap<InodeId, InodeId>,
        edges: HashMap<InodeId, Edge>,
        theirs: MetadataView<'v, 'v, S>,
    ) -> Self {
        let mut pending_creates = HashSet::new();
        let mut pending_moves = HashMap::new();
        let mut deleted = HashSet::new();
        for (index, plan) in plans.iter().enumerate() {
            match &plan.binding {
                Some(BindingChange::Create { .. } | BindingChange::RestoreDeleted { .. }) => {
                    pending_creates.insert(plan.inode_id);
                }
                Some(BindingChange::Move { .. }) => {
                    pending_moves.insert(plan.inode_id, index);
                }
                Some(BindingChange::Delete { .. }) => {
                    deleted.insert(plan.inode_id);
                }
                None => {}
            }
        }
        let mut order = Self {
            plans,
            holders,
            theirs,
            edges,
            pending_creates,
            pending_moves,
            temporary: HashMap::new(),
            left: HashSet::new(),
            pending_deletes: HashSet::new(),
            survivors: HashMap::new(),
            changes: Vec::new(),
            done: Vec::new(),
            waiters: HashMap::new(),
            wanted: BTreeSet::new(),
            operations: Vec::new(),
            created_inode_ids: BTreeMap::new(),
            temporary_moves: BTreeMap::new(),
            preconditions: Vec::new(),
        };
        for (index, plan) in plans.iter().enumerate() {
            match &plan.binding {
                // A delete of an inode below a directory that the merge
                // deletes, with no moved inode between the two, adds nothing,
                // because the directory's recursive delete deletes the inode.
                Some(BindingChange::Delete { .. })
                    if order.deleted_with_ancestor(plan.inode_id, &deleted) => {}
                Some(_) => order.changes.push(index),
                None => {}
            }
        }
        order.done = vec![false; order.changes.len()];
        order.pending_deletes = order
            .changes
            .iter()
            .map(|index| &plans[*index])
            .filter(|plan| matches!(plan.binding, Some(BindingChange::Delete { .. })))
            .map(|plan| plan.inode_id)
            .collect();
        for plan in plans {
            if !matches!(plan.binding, Some(BindingChange::Move { .. })) {
                continue;
            }
            let mut ancestor = plan.inode_id;
            while let Some((parent, _)) = order.edges.get(&ancestor) {
                ancestor = *parent;
                if order.pending_moves.contains_key(&ancestor) {
                    break;
                }
                if order.pending_deletes.contains(&ancestor) {
                    order
                        .survivors
                        .entry(ancestor)
                        .or_default()
                        .0
                        .push(plan.inode_id);
                }
            }
        }
        order
    }

    /// Returns whether the recursive delete of a directory in `deleted`
    /// deletes `inode_id`, which it does when the ancestors of `inode_id`
    /// reach that directory before they reach a moved inode.
    fn deleted_with_ancestor(&self, inode_id: InodeId, deleted: &HashSet<InodeId>) -> bool {
        let mut ancestor = inode_id;
        while let Some((parent, _)) = self.edges.get(&ancestor) {
            ancestor = *parent;
            if self.pending_moves.contains_key(&ancestor) {
                return false;
            }
            if deleted.contains(&ancestor) {
                return true;
            }
        }
        false
    }

    /// Emits every change, each as soon as nothing it waits for is left. When
    /// every change left waits, a move that another change waits for takes a
    /// temporary name. When no move can, the merge fails.
    async fn run(&mut self) -> Result<()> {
        let plans = self.plans;
        let mut queue: VecDeque<usize> = (0..self.changes.len()).collect();
        let mut blocked = Vec::new();
        let mut remaining = self.changes.len();
        while remaining > 0 {
            while let Some(change) = queue.pop_front() {
                if self.done[change] {
                    continue;
                }
                let plan = &plans[self.changes[change]];
                match self.readiness(plan) {
                    Readiness::Ready => {
                        self.emit(plan)?;
                        self.done[change] = true;
                        remaining -= 1;
                        queue.extend(self.waiters.remove(&plan.inode_id).unwrap_or_default());
                    }
                    Readiness::Waits(inode_id) => {
                        if self.pending_moves.contains_key(&inode_id)
                            && !self.temporary.contains_key(&inode_id)
                        {
                            self.wanted.insert(inode_id);
                        }
                        self.waiters.entry(inode_id).or_default().push(change);
                    }
                    Readiness::Blocked => blocked.push(change),
                }
            }
            if remaining == 0 {
                break;
            }
            let inode_id = self.temporary_candidate().ok_or_else(unordered)?;
            self.emit_temporary(inode_id).await?;
            queue.extend(self.waiters.remove(&inode_id).unwrap_or_default());
            queue.extend(blocked.drain(..));
        }
        Ok(())
    }

    fn readiness(&mut self, plan: &InodePlan) -> Readiness {
        let inode_id = plan.inode_id;
        match &plan.binding {
            Some(
                BindingChange::Create {
                    parent_inode_id, ..
                }
                | BindingChange::Move {
                    parent_inode_id, ..
                }
                | BindingChange::RestoreDeleted {
                    parent_inode_id, ..
                },
            ) => {
                if self.pending_creates.contains(parent_inode_id) {
                    return Readiness::Waits(*parent_inode_id);
                }
                if let Some(holder) = self.holders.get(&inode_id) {
                    if !self.left.contains(holder) {
                        return Readiness::Waits(*holder);
                    }
                }
                if plan.kind == InodeKind::Directory
                    && (*parent_inode_id == inode_id || self.is_below(*parent_inode_id, inode_id))
                {
                    return self.moved_between(*parent_inode_id, inode_id);
                }
                Readiness::Ready
            }
            Some(BindingChange::Delete { .. }) => self.survivors_out(inode_id),
            None => Readiness::Ready,
        }
    }

    /// Returns whether the deleted directory's survivors have left it, or the
    /// first survivor that hasn't.
    fn survivors_out(&mut self, directory: InodeId) -> Readiness {
        let Some((survivors, seen)) = self.survivors.get(&directory) else {
            return Readiness::Ready;
        };
        let mut seen = *seen;
        let mut readiness = Readiness::Ready;
        while let Some(survivor) = survivors.get(seen) {
            let placed = !self.pending_moves.contains_key(survivor);
            let away =
                self.temporary.contains_key(survivor) && !self.is_below(*survivor, directory);
            if !placed && !away {
                readiness = Readiness::Waits(*survivor);
                break;
            }
            seen += 1;
        }
        if let Some(entry) = self.survivors.get_mut(&directory) {
            entry.1 = seen;
        }
        readiness
    }

    /// Returns the first inode from `inode_id` up to `ancestor` that the
    /// merge still moves, which a move of `ancestor` into `inode_id` waits
    /// for.
    fn moved_between(&self, inode_id: InodeId, ancestor: InodeId) -> Readiness {
        let mut current = inode_id;
        while current != ancestor {
            if self.pending_moves.contains_key(&current) {
                return Readiness::Waits(current);
            }
            match self.edges.get(&current) {
                Some((parent, _)) => current = *parent,
                None => break,
            }
        }
        Readiness::Blocked
    }

    /// Returns whether `inode_id` is below `ancestor` now.
    fn is_below(&self, inode_id: InodeId, ancestor: InodeId) -> bool {
        let mut current = inode_id;
        // An edge map with a cycle would be corrupt, and the bound ends the
        // walk.
        for _ in 0..=self.edges.len() {
            match self.edges.get(&current) {
                Some((parent, _)) if *parent == ancestor => return true,
                Some((parent, _)) => current = *parent,
                None => return false,
            }
        }
        false
    }

    /// Returns the smallest move that another change waits for and that
    /// hasn't taken a temporary name.
    fn temporary_candidate(&mut self) -> Option<InodeId> {
        while let Some(inode_id) = self.wanted.pop_first() {
            if self.pending_moves.contains_key(&inode_id) && !self.temporary.contains_key(&inode_id)
            {
                return Some(inode_id);
            }
        }
        None
    }

    /// Moves `inode_id` to a temporary name, which is its inode ID with a
    /// suffix when that name is taken. The temporary name goes in the parent
    /// of the outermost directory above the inode that the merge deletes, so
    /// the inode survives the delete. Otherwise it goes in the directory that
    /// the inode's move goes to, when that directory exists and isn't below
    /// the inode, and otherwise in the inode's current directory. The move
    /// frees the inode's name and takes the inode out of any directory that
    /// it leaves.
    async fn emit_temporary(&mut self, inode_id: InodeId) -> Result<()> {
        let plans = self.plans;
        let (expected_binding_version, parent_inode_id) =
            match &plans[self.pending_moves[&inode_id]].binding {
                Some(BindingChange::Move {
                    expected_binding_version,
                    parent_inode_id,
                    ..
                }) => Some((expected_binding_version, *parent_inode_id)),
                _ => None,
            }
            .expect("a pending move's plan is a move");
        let target_exists = !self.pending_creates.contains(&parent_inode_id)
            && parent_inode_id != inode_id
            && !self.is_below(parent_inode_id, inode_id);
        let deleted_ancestor = self.outermost_pending_delete(inode_id);
        let current = self
            .edges
            .get(&inode_id)
            .map(|(parent, _)| *parent)
            .ok_or_else(unordered)?;
        let destination = match deleted_ancestor {
            Some(directory) => self.edges.get(&directory).map(|(parent, _)| *parent),
            None if target_exists => Some(parent_inode_id),
            None => Some(current),
        }
        .ok_or_else(unordered)?;
        let name = self.temporary_name(inode_id, destination).await?;
        self.temporary_moves
            .insert(self.operations.len(), TemporaryMove::Away);
        self.operations.push(FilesystemOperation::MoveByInode {
            inode_id,
            expected_binding_version: expected_binding_version.clone(),
            destination_parent_inode_id: destination,
            destination_display_name: name.clone(),
            precondition: DestinationPrecondition::default(),
        });
        self.edges.insert(inode_id, (destination, name));
        self.temporary.insert(inode_id, current);
        self.left.insert(inode_id);
        Ok(())
    }

    /// Returns the outermost directory above `inode_id` that the merge
    /// deletes and hasn't deleted yet.
    fn outermost_pending_delete(&self, inode_id: InodeId) -> Option<InodeId> {
        let mut outermost = None;
        let mut current = inode_id;
        for _ in 0..=self.edges.len() {
            let Some((parent, _)) = self.edges.get(&current) else {
                break;
            };
            if self.pending_deletes.contains(parent) {
                outermost = Some(*parent);
            }
            current = *parent;
        }
        outermost
    }

    /// Returns a temporary name for `inode_id` in `directory`: the inode's ID,
    /// or the ID with the first numeric suffix that no inode in the directory
    /// has in the merged-into namespace or in the order so far.
    async fn temporary_name(&self, inode_id: InodeId, directory: InodeId) -> Result<DisplayName> {
        // Each name that the search tries reads a child of the directory, so
        // the search stops after the inode ID and a few suffixes.
        for suffix in 0..=MAX_TEMPORARY_NAME_SUFFIX {
            let name = match suffix {
                0 => inode_id.to_string(),
                _ => format!("{inode_id}-{suffix}"),
            };
            let name = DisplayName::parse(name).expect("an inode ID is a valid display name");
            let key = NameKey::for_display_name(&name);
            let placed = self.edges.values().any(|(parent, placed)| {
                *parent == directory && NameKey::for_display_name(placed) == key
            });
            if !placed && self.theirs.visible_child(directory, &key).await?.is_none() {
                return Ok(name);
            }
        }
        Err(unordered())
    }

    fn emit(&mut self, plan: &InodePlan) -> Result<()> {
        let inode_id = plan.inode_id;
        let Some(binding) = &plan.binding else {
            return Ok(());
        };
        match binding {
            BindingChange::Create {
                parent_inode_id,
                display_name,
                ..
            } => {
                self.created_inode_ids
                    .insert(self.operations.len(), inode_id);
                self.operations.push(binding.operation(inode_id, plan.kind));
                self.edges
                    .insert(inode_id, (*parent_inode_id, display_name.clone()));
                self.pending_creates.remove(&inode_id);
            }
            BindingChange::RestoreDeleted {
                parent_inode_id,
                display_name,
                ..
            } => {
                self.operations.push(binding.operation(inode_id, plan.kind));
                self.edges
                    .insert(inode_id, (*parent_inode_id, display_name.clone()));
                self.pending_creates.remove(&inode_id);
            }
            BindingChange::Move {
                parent_inode_id,
                display_name,
                ..
            } => {
                let operation = match self.temporary.get(&inode_id) {
                    // The temporary move rewrote the binding in this commit,
                    // so this move identifies the inode by its path.
                    Some(from_parent) => {
                        self.temporary_moves.insert(
                            self.operations.len(),
                            TemporaryMove::Back {
                                from_parent: *from_parent,
                            },
                        );
                        FilesystemOperation::MovePath {
                            source_path: self.path(inode_id)?,
                            destination_path: self.path(*parent_inode_id)?.join(display_name),
                            precondition: DestinationPrecondition::default(),
                        }
                    }
                    None => binding.operation(inode_id, plan.kind),
                };
                self.operations.push(operation);
                self.edges
                    .insert(inode_id, (*parent_inode_id, display_name.clone()));
                self.pending_moves.remove(&inode_id);
                self.left.insert(inode_id);
            }
            BindingChange::Delete { .. } => {
                self.operations.push(binding.operation(inode_id, plan.kind));
                self.preconditions.extend(binding.precondition(inode_id));
                self.left.insert(inode_id);
                self.pending_deletes.remove(&inode_id);
            }
        }
        Ok(())
    }

    /// Returns `inode_id`'s path now, or fails when the order doesn't know
    /// an edge on the way to the root, such as the edge of a directory that
    /// the merge hasn't created yet.
    fn path(&self, inode_id: InodeId) -> Result<AbsolutePath> {
        let mut names = Vec::new();
        let mut current = inode_id;
        while current != ROOT_INODE_ID {
            let (parent, name) = self.edges.get(&current).ok_or_else(unordered)?;
            names.push(name);
            current = *parent;
            if names.len() > self.edges.len() {
                return Err(unordered());
            }
        }
        Ok(names
            .into_iter()
            .rev()
            .fold(AbsolutePath::root(), |path, name| path.join(name)))
    }
}
