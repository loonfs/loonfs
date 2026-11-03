//! One inode's state at one sequence, as a merge compares it, and the reads
//! that build it.

use crate::error::Result;
use crate::metadata::{
    AccessRevisionRecord, AttributesRevisionRecord, MetadataView, MetadataViewSession,
    RevisionRecord,
};
use crate::path::read::resolve_visible_inode;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::{
    AbsolutePath, AccessGrants, Attributes, InodeId, InodeKind, MergeConflictRevision,
    MergeConflictVersion,
};
use std::collections::HashMap;

/// One inode's rows at one sequence: whether and where it's visible, and its
/// newest revision, attribute, and access rows.
#[derive(Debug, Clone)]
pub(super) struct InodeState {
    pub(super) kind: InodeKind,
    /// The inode's path and binding, or `None` when a deletion hides the
    /// inode or one of its ancestors.
    pub(super) visible: Option<Visible>,
    /// A file's newest revision. A directory has none.
    pub(super) revision: Option<RevisionRecord>,
    pub(super) attributes: Option<AttributesRevisionRecord>,
    pub(super) access: Option<AccessRevisionRecord>,
}

/// Where a visible inode is.
#[derive(Debug, Clone)]
pub(super) struct Visible {
    pub(super) path: AbsolutePath,
    /// The inode's parent and name, which the root doesn't have.
    pub(super) edge: Option<Edge>,
}

/// A visible inode's parent, its name there, and the binding row that bound
/// it.
#[derive(Debug, Clone)]
pub(super) struct Edge {
    pub(super) parent_inode_id: InodeId,
    pub(super) display_name: String,
    pub(super) binding: DeltaPosition,
}

impl InodeState {
    /// Returns the inode's parent and name, which a move or a create sets,
    /// or `None` when it's hidden or is the root.
    pub(super) fn edge(&self) -> Option<&Edge> {
        self.visible
            .as_ref()
            .and_then(|visible| visible.edge.as_ref())
    }

    /// Returns whether the inode has the same parent and name in both
    /// states, or is hidden in both. A name that changes only its case
    /// changes the edge.
    pub(super) fn same_binding(&self, other: &Self) -> bool {
        match (&self.visible, &other.visible) {
            (Some(left), Some(right)) => match (&left.edge, &right.edge) {
                (Some(left), Some(right)) => {
                    left.parent_inode_id == right.parent_inode_id
                        && left.display_name == right.display_name
                }
                (None, None) => true,
                _ => false,
            },
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
        let visible = self.visible.as_ref()?;
        Some(MergeConflictVersion {
            path: visible.path.clone(),
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
    paths: HashMap<InodeId, AbsolutePath>,
}

impl<'a, 'store, S: ObjectStore + ?Sized> StateReader<'a, 'store, S> {
    pub(super) fn new(view: MetadataView<'a, 'store, S>) -> Self {
        Self {
            view,
            session: view.session(),
            paths: HashMap::new(),
        }
    }

    pub(super) fn view(&self) -> MetadataView<'a, 'store, S> {
        self.view
    }

    pub(super) fn session(&mut self) -> &mut MetadataViewSession<'a, 'store, S> {
        &mut self.session
    }

    /// Reads `inode_id`'s state, or `None` when the inode doesn't exist at
    /// this sequence.
    pub(super) async fn state(&mut self, inode_id: InodeId) -> Result<Option<InodeState>> {
        let Some(inode) = self.session.inode_at_seq(inode_id).await? else {
            return Ok(None);
        };
        let visible = resolve_visible_inode(&mut self.session, &mut self.paths, inode_id)
            .await?
            .map(|resolved| Visible {
                path: resolved.absolute_path,
                edge: resolved.parent_inode_id.zip(resolved.binding_version).map(
                    |(parent_inode_id, binding)| Edge {
                        parent_inode_id,
                        display_name: resolved.display_name,
                        binding,
                    },
                ),
            });
        let revision = match inode.inode_kind {
            InodeKind::File => self.view.latest_revision_record(inode_id).await?,
            InodeKind::Directory => None,
        };
        Ok(Some(InodeState {
            kind: inode.inode_kind,
            visible,
            revision,
            attributes: self.view.latest_attributes_revision(inode_id).await?,
            access: self.view.latest_access_revision(inode_id).await?,
        }))
    }
}
