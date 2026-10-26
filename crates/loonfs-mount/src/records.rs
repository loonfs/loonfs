//! The record that `tree.db` keeps for each item: its Committed,
//! server-observed, and Local columns, and the rules that move one copy of
//! a field group into another.

use crate::attributes::{effective_file_mtime_ms, FsAttributes, MtimeAttribute};
use crate::item::{ItemId, ItemKind};
use loonfs_types::api::v0::{PathEntry, PathEntryKind};
use loonfs_types::{
    AttributesRevisionNo, BindingVersion, ChangeSeq, ContentRef, InodeId, RevisionNo,
};

/// The bit of `dirty_groups` that's set while the binding has a local change.
pub(crate) const BINDING_DIRTY: u8 = 1 << 0;
/// The bit of `dirty_groups` that's set while the content has a local change.
pub(crate) const CONTENT_DIRTY: u8 = 1 << 1;
/// The bit of `dirty_groups` that's set while the attributes have a local
/// change.
pub(crate) const ATTRIBUTES_DIRTY: u8 = 1 << 2;

/// An item's parent directory, name, and binding version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Binding {
    pub(crate) parent: ItemId,
    pub(crate) name: String,
    pub(crate) binding_version: BindingVersion,
}

/// A file's current revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileContent {
    pub(crate) revision_no: RevisionNo,
    pub(crate) content_ref: ContentRef,
    /// When the revision was committed, which a file shows as its
    /// modification time unless `fs.mtime_ms` describes the revision.
    pub(crate) committed_at_ms: u64,
}

/// An item's mode, modification time, and symlink target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttributeState {
    pub(crate) revision_no: AttributesRevisionNo,
    pub(crate) mode: u32,
    /// The modification time that the item shows.
    pub(crate) mtime_ms: u64,
    pub(crate) target: Option<String>,
    /// The raw `fs.mtime_ms` and `fs.mtime_revision_no`, which the engine
    /// uses to recompute `mtime_ms` when the content changes.
    pub(crate) mtime_attribute: Option<MtimeAttribute>,
}

/// One copy of an item's published state: its Committed columns, or its
/// server-observed columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteState {
    /// The binding, or `None` for the mount's root.
    pub(crate) binding: Option<Binding>,
    /// The current revision of a file or symlink, or `None` for a directory.
    pub(crate) content: Option<FileContent>,
    pub(crate) attributes: AttributeState,
}

impl RemoteState {
    /// Returns the kind and state of an entry that a listing or a read
    /// returned, or `None` for an inode kind that this version doesn't
    /// support.
    ///
    /// A directory shows its creation time until the mount applies a change
    /// to the directory's entries.
    pub(crate) fn from_entry(
        entry: &PathEntry,
        binding: Option<Binding>,
    ) -> Option<(ItemKind, Self)> {
        let fs = entry
            .attributes
            .as_ref()
            .map(|projection| FsAttributes::parse(&projection.attributes))
            .unwrap_or_default();
        let attributes_revision_no = entry
            .attributes
            .as_ref()
            .map_or(AttributesRevisionNo(0), |projection| {
                projection.attributes_revision_no
            });
        let (kind, content, mtime_ms) = match &entry.kind {
            PathEntryKind::Directory {} => (ItemKind::Directory, None, entry.created_at_ms),
            PathEntryKind::File {
                revision_no,
                content_ref,
                revision_committed_at_ms,
                ..
            } => (
                fs.kind(false),
                Some(FileContent {
                    revision_no: *revision_no,
                    content_ref: content_ref.clone(),
                    committed_at_ms: *revision_committed_at_ms,
                }),
                effective_file_mtime_ms(fs.mtime, *revision_no, *revision_committed_at_ms),
            ),
            PathEntryKind::Unknown => return None,
        };
        Some((
            kind,
            Self {
                binding,
                content,
                attributes: AttributeState {
                    revision_no: attributes_revision_no,
                    mode: fs.effective_mode(kind),
                    mtime_ms,
                    target: fs.symlink_target.clone(),
                    mtime_attribute: fs.mtime,
                },
            },
        ))
    }

    /// Sets a file's current revision, and the modification time it shows.
    pub(crate) fn set_content(&mut self, content: FileContent) {
        self.attributes.mtime_ms = effective_file_mtime_ms(
            self.attributes.mtime_attribute,
            content.revision_no,
            content.committed_at_ms,
        );
        self.content = Some(content);
    }

    /// Sets the attributes that an attribute revision recorded, and returns
    /// the item's kind with the new attributes.
    pub(crate) fn set_attributes(
        &mut self,
        kind: ItemKind,
        revision_no: AttributesRevisionNo,
        fs: &FsAttributes,
    ) -> ItemKind {
        let kind = fs.kind(kind == ItemKind::Directory);
        self.attributes.revision_no = revision_no;
        self.attributes.mode = fs.effective_mode(kind);
        self.attributes.target = fs.symlink_target.clone();
        self.attributes.mtime_attribute = fs.mtime;
        if let Some(content) = &self.content {
            self.attributes.mtime_ms =
                effective_file_mtime_ms(fs.mtime, content.revision_no, content.committed_at_ms);
        }
        kind
    }

    /// Returns the size of the current revision, or zero for a directory.
    pub(crate) fn size_bytes(&self) -> u64 {
        self.content
            .as_ref()
            .map_or(0, |content| content.content_ref.size_bytes)
    }
}

/// What the Local tree shows for one item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalState {
    /// The parent and name, or `None` for the root, an item that was unlinked
    /// locally, and an item that the engine withholds from the Local tree.
    pub(crate) binding: Option<(ItemId, String)>,
    pub(crate) size_bytes: u64,
    pub(crate) mode: u32,
    pub(crate) mtime_ms: u64,
    pub(crate) target: Option<String>,
}

/// Whether the Local tree shows an item's binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Visibility {
    Shown,
    /// The engine keeps the item out of the Local tree, for example a
    /// directory that entered the mount before the feed reached its listing.
    Withheld,
}

/// A remote deletion that hides an item with local changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HiddenBy {
    pub(crate) inode_id: InodeId,
    pub(crate) deleted_seq: ChangeSeq,
}

/// The columns that only the write path changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WriteState {
    pub(crate) mtime_explicit: bool,
    pub(crate) materialized: bool,
    pub(crate) generation: u64,
    pub(crate) published_generation: u64,
    pub(crate) lowest_write_offset: Option<u64>,
    pub(crate) truncated_below_base: bool,
    pub(crate) fingerprint: Option<Vec<u8>>,
    /// The field groups with local changes, as `*_DIRTY` bits.
    pub(crate) dirty_groups: u8,
}

/// One item's row in `tree.db`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ItemRecord {
    pub(crate) item_id: ItemId,
    pub(crate) remote_inode_id: Option<InodeId>,
    pub(crate) kind: ItemKind,
    pub(crate) committed: RemoteState,
    pub(crate) server: RemoteState,
    pub(crate) hidden_by: Option<HiddenBy>,
    /// Whether every remote revision since the content's base was an append
    /// that started at the previous size.
    pub(crate) append_only: bool,
    pub(crate) local: LocalState,
    pub(crate) write: WriteState,
}

impl ItemRecord {
    /// Returns the record of an item without local changes, whose three
    /// copies of its state agree.
    pub(crate) fn clean(
        item_id: ItemId,
        remote_inode_id: Option<InodeId>,
        kind: ItemKind,
        state: RemoteState,
        visibility: Visibility,
    ) -> Self {
        let local = LocalState {
            binding: None,
            size_bytes: 0,
            mode: state.attributes.mode,
            mtime_ms: state.attributes.mtime_ms,
            target: None,
        };
        let mut record = Self {
            item_id,
            remote_inode_id,
            kind,
            committed: state.clone(),
            server: state,
            hidden_by: None,
            append_only: true,
            local,
            write: WriteState::default(),
        };
        record.show_content();
        record.show_attributes();
        record.show_binding(visibility);
        record
    }

    fn is_clean(&self, dirty_bit: u8) -> bool {
        self.write.dirty_groups & dirty_bit == 0
    }

    /// Moves the server-observed binding into the Committed and Local
    /// columns when the binding has no local change, and returns whether the
    /// binding moved.
    pub(crate) fn adopt_binding(&mut self, visibility: Visibility) -> bool {
        if !self.is_clean(BINDING_DIRTY) {
            return false;
        }
        self.committed.binding = self.server.binding.clone();
        self.show_binding(visibility);
        true
    }

    /// Moves the server-observed content into the Committed and Local columns
    /// when the content has no local change, and returns whether the content
    /// moved.
    pub(crate) fn adopt_content(&mut self) -> bool {
        if !self.is_clean(CONTENT_DIRTY) {
            return false;
        }
        self.committed.content = self.server.content.clone();
        self.committed.attributes.mtime_ms = self.server.attributes.mtime_ms;
        self.append_only = true;
        self.show_content();
        true
    }

    /// Moves the server-observed attributes into the Committed and Local
    /// columns when the attributes have no local change, and returns whether
    /// the attributes moved.
    pub(crate) fn adopt_attributes(&mut self) -> bool {
        if !self.is_clean(ATTRIBUTES_DIRTY) {
            return false;
        }
        self.committed.attributes = self.server.attributes.clone();
        self.show_attributes();
        self.show_content();
        true
    }

    fn show_binding(&mut self, visibility: Visibility) {
        self.local.binding = match visibility {
            Visibility::Shown => self
                .committed
                .binding
                .as_ref()
                .map(|binding| (binding.parent, binding.name.clone())),
            Visibility::Withheld => None,
        };
    }

    fn show_content(&mut self) {
        self.local.size_bytes = match self.kind {
            ItemKind::Directory => 0,
            ItemKind::File => self.committed.size_bytes(),
            ItemKind::Symlink => self
                .committed
                .attributes
                .target
                .as_ref()
                .map_or(0, |target| target.len() as u64),
        };
        if self.kind != ItemKind::Directory {
            self.local.mtime_ms = self.committed.attributes.mtime_ms;
        }
    }

    fn show_attributes(&mut self) {
        self.local.mode = self.committed.attributes.mode;
        self.local.target = self.committed.attributes.target.clone();
    }
}
