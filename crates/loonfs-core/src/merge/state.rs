//! One inode's state at one sequence, as a merge compares it, and the reads
//! that build it.

use super::memory::PlanningMemory;
use crate::error::Result;
use crate::heap_bytes::HeapBytes;
use crate::metadata::{
    AccessRevisionRecord, AttributesRevisionRecord, MetadataView, MetadataViewSession,
    RevisionRecord,
};
use crate::path::read::{resolve_visible_inode, AncestorPaths};
use crate::segment::BlocksPerRefill;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::{
    AbsolutePath, AccessGrants, Attributes, InodeId, InodeKind, MergeConflictRevision,
    MergeConflictVersion,
};
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

/// The estimated bytes of one path cache entry beyond its path's heap bytes.
///
/// The estimate is the entry's slot in the hash table, twice over, because
/// a hash table that just grew has about twice as many slots as entries.
const PATH_ENTRY_BYTES: usize = 2 * size_of::<(InodeId, AbsolutePath)>();

/// One inode's rows at one sequence: its own binding row, its path when it's
/// visible, and its newest revision, attribute, and access rows.
#[derive(Debug, Clone)]
pub(super) struct InodeState {
    pub(super) kind: InodeKind,
    /// The inode's own binding row: its parent, its name there, and the
    /// row's position. The root has none, and neither does an inode whose
    /// own delete unbound its row.
    pub(super) edge: Option<Edge>,
    /// The inode's path, when the inode is visible: no tombstone on it or
    /// on an ancestor hides it.
    pub(super) path: Option<AbsolutePath>,
    /// A file's newest revision. A directory has none.
    pub(super) revision: Option<RevisionRecord>,
    pub(super) attributes: Option<AttributesRevisionRecord>,
    pub(super) access: Option<AccessRevisionRecord>,
}

/// An inode's parent, its name there, and the position of the binding row
/// that bound it.
#[derive(Debug, Clone)]
pub(super) struct Edge {
    pub(super) parent_inode_id: InodeId,
    pub(super) display_name: String,
    pub(super) binding: DeltaPosition,
}

impl InodeState {
    /// Returns the inode's own binding row, or `None` for the root and for
    /// an inode whose own delete unbound the row.
    pub(super) fn edge(&self) -> Option<&Edge> {
        self.edge.as_ref()
    }

    /// Returns whether the inode is visible.
    pub(super) fn is_visible(&self) -> bool {
        self.path.is_some()
    }

    /// Returns whether the inode's own binding row has the same parent and
    /// name in both states, or is unbound in both. A name that changes only
    /// its case changes the edge.
    pub(super) fn same_binding(&self, other: &Self) -> bool {
        match (&self.edge, &other.edge) {
            (Some(left), Some(right)) => {
                left.parent_inode_id == right.parent_inode_id
                    && left.display_name == right.display_name
            }
            (None, None) => true,
            _ => false,
        }
    }

    /// Returns whether the file has the same content in both states.
    pub(super) fn same_content(&self, other: &Self) -> bool {
        self.revision.as_ref().map(|row| &row.content_ref)
            == other.revision.as_ref().map(|row| &row.content_ref)
    }

    /// Returns the inode's attributes, which are empty without a row.
    pub(super) fn attributes(&self) -> Attributes {
        self.attributes
            .as_ref()
            .map(|row| row.attributes.clone())
            .unwrap_or_default()
    }

    /// Returns whether the inode has the same attributes in both states.
    pub(super) fn same_attributes(&self, other: &Self) -> bool {
        self.attributes() == other.attributes()
    }

    /// Returns the inode's boundary flag and direct grants, which are off
    /// and empty without a row.
    pub(super) fn access(&self) -> (bool, AccessGrants) {
        self.access.as_ref().map_or_else(
            || (false, AccessGrants::default()),
            |row| (row.boundary, row.grants.clone()),
        )
    }

    /// Returns whether the inode has the same access in both states.
    pub(super) fn same_access(&self, other: &Self) -> bool {
        self.access() == other.access()
    }

    /// Returns this state as a version of a merge conflict, or `None` when
    /// the inode is hidden.
    pub(super) fn conflict_version(&self) -> Option<MergeConflictVersion> {
        Some(MergeConflictVersion {
            path: self.path.clone()?,
            revision: self.revision.as_ref().map(|row| MergeConflictRevision {
                revision_no: row.revision_no,
                content_ref: row.content_ref.clone(),
            }),
        })
    }
}

/// Reads inode states at one sequence, and keeps the paths it resolved so
/// that each directory's walk to the root runs once.
pub(super) struct StateReader<'a, 'store, S: ObjectStore + ?Sized> {
    view: MetadataView<'a, 'store, S>,
    session: MetadataViewSession<'a, 'store, S>,
    /// The paths resolved so far, which their descendants' paths start
    /// from. It grows with the inodes that a merge reads, which
    /// [`MAX_MERGED_INODES`](crate::commit::wal_size::MAX_MERGED_INODES)
    /// bounds, and with their ancestors.
    paths: MeasuredPaths,
}

/// A path cache that adds the bytes of each entry to the plan's memory as
/// the entry is inserted.
struct MeasuredPaths {
    paths: HashMap<InodeId, AbsolutePath>,
    memory: Arc<PlanningMemory>,
}

impl AncestorPaths for MeasuredPaths {
    fn get(&self, inode_id: &InodeId) -> Option<&AbsolutePath> {
        self.paths.get(inode_id)
    }

    fn insert(&mut self, inode_id: InodeId, path: AbsolutePath) {
        self.memory
            .add_path_bytes(PATH_ENTRY_BYTES.saturating_add(path.heap_bytes()));
        self.paths.insert(inode_id, path);
    }
}

impl<'a, 'store, S: ObjectStore + ?Sized> StateReader<'a, 'store, S> {
    /// Returns a reader of `view` whose path cache reports to `memory`.
    pub(super) fn new(view: MetadataView<'a, 'store, S>, memory: &Arc<PlanningMemory>) -> Self {
        Self {
            view,
            session: view.session_with(BlocksPerRefill::ReadAhead),
            paths: MeasuredPaths {
                paths: HashMap::new(),
                memory: Arc::clone(memory),
            },
        }
    }

    pub(super) fn view(&self) -> MetadataView<'a, 'store, S> {
        self.view
    }

    pub(super) fn session(&mut self) -> &mut MetadataViewSession<'a, 'store, S> {
        &mut self.session
    }

    /// Reads `inode_id`'s state, or `None` when the inode doesn't exist at
    /// this sequence. The read checks the merge allowance when the plan's
    /// path caches have grown by 1 MiB since the last check.
    ///
    /// A plan reads the state of every inode that the fork's commits touch,
    /// in inode ID order. The inodes that consecutive commits touch often
    /// have their rows in the blocks that follow, so each lookup reads ahead
    /// past the inode's rows (see [`BlocksPerRefill::ReadAhead`]). That
    /// includes the revision, attribute, and access lookups in this function,
    /// and the inode, parent binding, and subtree tombstone lookups through
    /// the session that [`Self::new`] opens. If each lookup fetched only the
    /// block with the inode's rows, the plan would send a GET for each block
    /// of rows that it reaches.
    pub(super) async fn state(&mut self, inode_id: InodeId) -> Result<Option<InodeState>> {
        let Some(inode) = self.session.inode_at_seq(inode_id).await? else {
            return Ok(None);
        };
        let resolved = resolve_visible_inode(&mut self.session, &mut self.paths, inode_id).await?;
        self.paths.memory.check_paths()?;
        // A delete unbinds the inode's own row before it records the
        // tombstone, so a hidden inode whose row is still bound is hidden by
        // an ancestor's deletion.
        let (path, edge) = match resolved {
            Some(resolved) => {
                let edge = resolved.parent_inode_id.zip(resolved.binding_version).map(
                    |(parent_inode_id, binding)| Edge {
                        parent_inode_id,
                        display_name: resolved.display_name,
                        binding,
                    },
                );
                (Some(resolved.absolute_path), edge)
            }
            None => {
                let edge = self
                    .session
                    .current_parent_binding_for_child(inode_id)
                    .await?
                    .and_then(|binding| {
                        Some(Edge {
                            parent_inode_id: binding.parent_inode_id,
                            display_name: binding.display_name()?.to_string(),
                            binding: binding.bind_position,
                        })
                    });
                (None, edge)
            }
        };
        let refill = BlocksPerRefill::ReadAhead;
        let revision = match inode.inode_kind {
            InodeKind::File => {
                self.view
                    .latest_revision_record_with(inode_id, refill)
                    .await?
            }
            InodeKind::Directory => None,
        };
        Ok(Some(InodeState {
            kind: inode.inode_kind,
            edge,
            path,
            revision,
            attributes: self
                .view
                .latest_attributes_revision_with(inode_id, refill)
                .await?,
            access: self
                .view
                .latest_access_revision_with(inode_id, refill)
                .await?,
        }))
    }
}
