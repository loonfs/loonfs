//! Directory listings: the engine's entries, after `.`, `..`, and, at the
//! mount's root, the control directory.

use crate::control_directory::ControlItem;
use loonfs_mount::{DirEntry, Engine, ItemId, ItemKind};

/// The number of offsets before the engine's entries. `.` is at offset 1,
/// `..` at 2, and `.loonfs` (at the root) at 3, and an engine entry's
/// offset is the engine's offset plus this.
const ADDED_OFFSETS: u64 = 3;

/// Which of a directory's two dot entries an entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dot {
    /// `.`, the directory itself.
    Current,
    /// `..`, the directory that contains it.
    Parent,
}

impl Dot {
    /// Returns the entry's name.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Current => ".",
            Self::Parent => "..",
        }
    }
}

/// One entry of a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Listed {
    /// `.` or `..`, which the kernel doesn't count as a lookup.
    Dot { dot: Dot, offset: u64 },
    /// An item of the control directory.
    Control { control: ControlItem, offset: u64 },
    /// An entry of the engine's tree.
    Engine(DirEntry),
}

impl Listed {
    /// Returns the offset that continues a listing after this entry.
    pub(crate) fn offset(&self) -> u64 {
        match self {
            Self::Dot { offset, .. } | Self::Control { offset, .. } => *offset,
            Self::Engine(entry) => entry.offset,
        }
    }
}

/// Lists `directory` after `offset`, which is zero for the first entry or an
/// earlier entry's offset.
pub(crate) fn list(
    engine: &Engine,
    directory: ItemId,
    offset: u64,
) -> impl Iterator<Item = Listed> {
    let engine_entries = ControlItem::of(directory)
        .is_none()
        .then(|| engine.readdir(directory, offset.saturating_sub(ADDED_OFFSETS)))
        .into_iter()
        .flatten()
        .map(|entry| {
            Listed::Engine(DirEntry {
                offset: entry
                    .offset
                    .checked_add(ADDED_OFFSETS)
                    .expect("an engine offset is far below the largest offset"),
                ..entry
            })
        });
    added_entries(directory)
        .into_iter()
        .filter(move |entry| entry.offset() > offset)
        .chain(engine_entries)
}

/// Returns the entries that the adapter adds to `directory`'s listing.
fn added_entries(directory: ItemId) -> Vec<Listed> {
    let mut entries = vec![
        Listed::Dot {
            dot: Dot::Current,
            offset: 1,
        },
        Listed::Dot {
            dot: Dot::Parent,
            offset: 2,
        },
    ];
    if directory == ItemId::ROOT {
        entries.push(Listed::Control {
            control: ControlItem::Directory,
            offset: ADDED_OFFSETS,
        });
    }
    if ControlItem::of(directory) == Some(ControlItem::Directory) {
        entries.extend(
            (ADDED_OFFSETS + 1..)
                .zip(ControlItem::FILES)
                .map(|(offset, control)| Listed::Control { control, offset }),
        );
    }
    entries
}

/// Returns the kind of a listed entry. `.` and `..` are always directories.
pub(crate) fn listed_kind(entry: &Listed) -> ItemKind {
    match entry {
        Listed::Dot { .. } => ItemKind::Directory,
        Listed::Control { control, .. } => control.kind(),
        Listed::Engine(entry) => entry.kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(entries: &[Listed]) -> Vec<String> {
        entries
            .iter()
            .map(|entry| match entry {
                Listed::Dot { dot, .. } => dot.name().to_owned(),
                Listed::Control { control, .. } => control.name().to_owned(),
                Listed::Engine(entry) => entry.name.to_string_lossy().into_owned(),
            })
            .collect()
    }

    /// Checks that only the root lists `.loonfs`, that the control directory
    /// lists its files, and that the added entries' offsets come before
    /// every engine entry's.
    #[test]
    fn listings_add_dots_and_the_control_directory_before_the_engines_entries() {
        let root = added_entries(ItemId::ROOT);
        assert_eq!(names(&root), [".", "..", ".loonfs"]);
        assert_eq!(names(&added_entries(ItemId(9))), [".", ".."]);
        let control = added_entries(ControlItem::Directory.item());
        assert_eq!(names(&control), [".", "..", "status.json", "seq"]);
        let offsets: Vec<u64> = control.iter().map(Listed::offset).collect();
        assert_eq!(offsets, [1, 2, 4, 5]);
        assert!(root.iter().all(|entry| entry.offset() <= ADDED_OFFSETS));
        assert_eq!(listed_kind(&control[2]), ItemKind::File);
    }
}
