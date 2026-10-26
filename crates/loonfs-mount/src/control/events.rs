//! The rule that applies each kind of feed event, as "Following the change
//! feed" in the design note lists them.

use super::changes::Changes;
use super::RootLocation;
use crate::attributes::FsAttributes;
use crate::error::Result;
use crate::item::ItemKind;
use crate::item_table::{find_item_by_inode, find_item_id_by_inode};
use crate::records::{AttributeState, Binding, FileContent, RemoteState};
use crate::status::PublicationStop;
use crate::workers::{EnteringItem, RootAncestry};
use loonfs_types::api::v0::{Commit, FilesystemChange};
use loonfs_types::{
    Attributes, AttributesRevisionNo, BindingVersion, ContentRef, DisplayName, InodeId, RevisionNo,
};
use std::collections::HashMap;

/// A read that a commit needs before it can be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum ReadKey {
    /// An item that a move or an undelete brought into the mount.
    Item(InodeId),
    /// The root's ancestors, after the root or an ancestor moved.
    Ancestors,
}

/// What a read returned.
#[derive(Debug)]
pub(super) enum ReadResult {
    Item(EnteringItem),
    /// The root's new location.
    Ancestors(Box<RootAncestry>),
}

/// The reads that one commit needed.
pub(super) type ReadResults = HashMap<ReadKey, ReadResult>;

/// Whether a commit was applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommitOutcome {
    Applied,
    /// The commit needs this read first, so its transaction was rolled back.
    NeedsRead(ReadKey),
}

/// Applies each event of a commit in order. Returns the read that an event
/// needs, in which case the caller rolls the transaction back.
pub(super) fn apply_commit_events(
    changes: &mut Changes<'_>,
    root: &mut RootLocation,
    stop: &mut Option<PublicationStop>,
    commit: &Commit,
    reads: &ReadResults,
) -> Result<Option<ReadKey>> {
    for event in &commit.events {
        let needed = match event {
            FilesystemChange::DirectoryCreated {
                inode_id,
                parent_inode_id,
                display_name,
                binding_version,
            } => created(
                changes,
                *inode_id,
                *parent_inode_id,
                display_name,
                binding_version,
                None,
            )?,
            FilesystemChange::FileCreated {
                inode_id,
                parent_inode_id,
                display_name,
                binding_version,
                revision_no,
                content_ref,
            } => created(
                changes,
                *inode_id,
                *parent_inode_id,
                display_name,
                binding_version,
                Some((*revision_no, content_ref)),
            )?,
            FilesystemChange::ContentChanged {
                inode_id,
                revision_no,
                content_ref,
                append_offset,
            } => content_changed(
                changes,
                *inode_id,
                *revision_no,
                content_ref,
                *append_offset,
            )?,
            FilesystemChange::AttributesChanged {
                inode_id,
                attributes_revision_no,
                attributes,
            } => attributes_changed(changes, *inode_id, *attributes_revision_no, attributes)?,
            FilesystemChange::Moved {
                inode_id,
                destination_parent_inode_id,
                destination_display_name,
                binding_version,
                ..
            } => {
                if root.contains(*inode_id) {
                    root_moved(root, reads)
                } else {
                    bound(
                        changes,
                        *inode_id,
                        *destination_parent_inode_id,
                        destination_display_name,
                        binding_version,
                        reads,
                    )?
                }
            }
            FilesystemChange::Deleted { inode_id, .. } => {
                if root.contains(*inode_id) {
                    *stop = Some(PublicationStop::RootDeleted {
                        inode_id: *inode_id,
                        deleted_seq: changes.seq,
                    });
                } else if let Some(item) = find_item_id_by_inode(changes.connection, *inode_id)? {
                    changes.remove_subtree(item, *inode_id)?;
                }
                None
            }
            FilesystemChange::Undeleted {
                inode_id,
                parent_inode_id,
                display_name,
                binding_version,
            } => bound(
                changes,
                *inode_id,
                *parent_inode_id,
                display_name,
                binding_version,
                reads,
            )?,
            FilesystemChange::DeletionExpired { .. }
            | FilesystemChange::AccessChanged { .. }
            | FilesystemChange::Unknown => None,
        };
        if needed.is_some() {
            return Ok(needed);
        }
    }
    Ok(None)
}

/// `directory_created` and `file_created`: adds the item if its parent is in
/// the mount.
fn created(
    changes: &mut Changes<'_>,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    binding_version: &BindingVersion,
    file: Option<(RevisionNo, &ContentRef)>,
) -> Result<Option<ReadKey>> {
    let Some(parent) = find_item_id_by_inode(changes.connection, parent_inode_id)? else {
        return Ok(None);
    };
    let committed_at_ms = changes
        .committed_at_ms()
        .expect("a feed event belongs to a commit");
    let kind = match file {
        Some(_) => ItemKind::File,
        None => ItemKind::Directory,
    };
    let fs = FsAttributes::default();
    let state = RemoteState {
        binding: Some(Binding {
            parent,
            name: display_name.as_str().to_owned(),
            binding_version: binding_version.clone(),
        }),
        content: file.map(|(revision_no, content_ref)| FileContent {
            revision_no,
            content_ref: content_ref.clone(),
            committed_at_ms,
        }),
        attributes: AttributeState {
            revision_no: AttributesRevisionNo(0),
            mode: fs.effective_mode(kind),
            mtime_ms: committed_at_ms,
            target: None,
            mtime_attribute: None,
        },
    };
    changes.upsert_state(inode_id, kind, state)?;
    Ok(None)
}

/// `content_changed`: an append extends the item's revision, and any other
/// revision replaces the item with a new one.
fn content_changed(
    changes: &mut Changes<'_>,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: &ContentRef,
    append_offset: Option<u64>,
) -> Result<Option<ReadKey>> {
    let Some(mut record) = find_item_by_inode(changes.connection, inode_id)? else {
        return Ok(None);
    };
    let Some(current) = record.server.content.clone() else {
        return Ok(None);
    };
    if revision_no <= current.revision_no {
        // A read of an item that entered the mount is newer than the
        // commits that the feed applies after it.
        return Ok(None);
    }
    let committed_at_ms = changes
        .committed_at_ms()
        .expect("a feed event belongs to a commit");
    let content = FileContent {
        revision_no,
        content_ref: content_ref.clone(),
        committed_at_ms,
    };
    if append_offset == Some(current.content_ref.size_bytes) {
        changes.extend(&mut record, content);
        changes.write(&record)?;
        return Ok(None);
    }
    let mut state = record.server.clone();
    state.set_content(content);
    let kind = record.kind;
    changes.replace(record, kind, state)?;
    Ok(None)
}

/// `attributes_changed`: updates the mode, time, and symlink target from
/// the `fs.` keys. A change between a file and a symlink makes a new item.
fn attributes_changed(
    changes: &mut Changes<'_>,
    inode_id: InodeId,
    revision_no: AttributesRevisionNo,
    attributes: &Attributes,
) -> Result<Option<ReadKey>> {
    let Some(mut record) = find_item_by_inode(changes.connection, inode_id)? else {
        return Ok(None);
    };
    if revision_no <= record.server.attributes.revision_no {
        return Ok(None);
    }
    let mut state = record.server.clone();
    let kind = state.set_attributes(record.kind, revision_no, &FsAttributes::parse(attributes));
    if kind != record.kind {
        changes.replace(record, kind, state)?;
        return Ok(None);
    }
    record.server = state;
    changes.adopt_attributes(&mut record);
    changes.write(&record)?;
    Ok(None)
}

/// `moved` and `undeleted`: rebinds an item that's in the mount, removes one
/// that left it, and reads one that entered it.
fn bound(
    changes: &mut Changes<'_>,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    binding_version: &BindingVersion,
    reads: &ReadResults,
) -> Result<Option<ReadKey>> {
    let parent = find_item_id_by_inode(changes.connection, parent_inode_id)?;
    let record = find_item_by_inode(changes.connection, inode_id)?;
    let binding = parent.map(|parent| Binding {
        parent,
        name: display_name.as_str().to_owned(),
        binding_version: binding_version.clone(),
    });
    match (record, binding) {
        (Some(mut record), Some(binding)) => {
            record.server.binding = Some(binding);
            changes.adopt_binding(&mut record)?;
            changes.write(&record)?;
            Ok(None)
        }
        (Some(record), None) => {
            changes.remove_subtree(record.item_id, inode_id)?;
            Ok(None)
        }
        (None, Some(binding)) => match reads.get(&ReadKey::Item(inode_id)) {
            None => Ok(Some(ReadKey::Item(inode_id))),
            Some(ReadResult::Item(EnteringItem::File(entry))) => {
                changes.upsert_entry(entry, Some(binding))?;
                Ok(None)
            }
            Some(ReadResult::Item(EnteringItem::Gone | EnteringItem::Listed))
            | Some(ReadResult::Ancestors(_)) => Ok(None),
        },
        (None, None) => Ok(None),
    }
}

/// A move of the root or of a directory above it: the mount follows the
/// root by inode, so only the root's path and ancestors change.
fn root_moved(root: &mut RootLocation, reads: &ReadResults) -> Option<ReadKey> {
    match reads.get(&ReadKey::Ancestors) {
        None => Some(ReadKey::Ancestors),
        Some(ReadResult::Ancestors(location)) => {
            root.path = location.root.path.clone();
            root.ancestors = location.ancestors.clone();
            None
        }
        Some(ReadResult::Item(_)) => None,
    }
}
