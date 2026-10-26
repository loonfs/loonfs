//! The `fs.` attributes in which a mount stores what a LoonFS inode doesn't
//! have: a mode, an explicit modification time, and a symlink's target.

use crate::item::ItemKind;
use loonfs_types::{Attributes, RevisionNo};

const MODE_KEY: &str = "fs.mode";
const MTIME_KEY: &str = "fs.mtime_ms";
const MTIME_REVISION_KEY: &str = "fs.mtime_revision_no";
const TYPE_KEY: &str = "fs.type";
const TARGET_KEY: &str = "fs.target";
const SYMLINK_TYPE: &str = "symlink";

/// The mode of a file without `fs.mode`.
pub(crate) const DEFAULT_FILE_MODE: u32 = 0o644;
/// The mode of a directory without `fs.mode`.
pub(crate) const DEFAULT_DIRECTORY_MODE: u32 = 0o755;
/// The mode that every symlink shows, as on a local filesystem.
const SYMLINK_MODE: u32 = 0o777;
const PERMISSION_BITS: u32 = 0o7777;

/// The `fs.` attributes of one inode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FsAttributes {
    /// `fs.mode`, when it's a valid octal mode.
    pub(crate) mode: Option<u32>,
    /// `fs.mtime_ms` and `fs.mtime_revision_no`, when both are valid.
    pub(crate) mtime: Option<MtimeAttribute>,
    /// `fs.target` of an inode whose `fs.type` is `symlink`.
    pub(crate) symlink_target: Option<String>,
}

/// A modification time that a program set, and the content revision that it
/// describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MtimeAttribute {
    pub(crate) mtime_ms: u64,
    pub(crate) revision_no: RevisionNo,
}

impl FsAttributes {
    /// Reads the `fs.` keys of an attribute map. A value that doesn't parse
    /// counts as absent, because another client can write any string under
    /// these keys.
    pub(crate) fn parse(attributes: &Attributes) -> Self {
        let value = |key: &str| attributes.as_map().get(key).map(|value| value.as_str());
        let mode = value(MODE_KEY)
            .and_then(|mode| u32::from_str_radix(mode, 8).ok())
            .map(|mode| mode & PERMISSION_BITS);
        let mtime = value(MTIME_KEY)
            .and_then(|mtime_ms| mtime_ms.parse().ok())
            .zip(value(MTIME_REVISION_KEY).and_then(|revision| revision.parse().ok()))
            .map(|(mtime_ms, revision_no)| MtimeAttribute {
                mtime_ms,
                revision_no: RevisionNo(revision_no),
            });
        let symlink_target = (value(TYPE_KEY) == Some(SYMLINK_TYPE))
            .then(|| value(TARGET_KEY).unwrap_or_default().to_owned());
        Self {
            mode,
            mtime,
            symlink_target,
        }
    }

    /// Returns the kind of an inode with these attributes.
    pub(crate) fn kind(&self, directory: bool) -> ItemKind {
        match (directory, &self.symlink_target) {
            (true, _) => ItemKind::Directory,
            (false, Some(_)) => ItemKind::Symlink,
            (false, None) => ItemKind::File,
        }
    }

    /// Returns the mode that an item of `kind` with these attributes shows.
    pub(crate) fn effective_mode(&self, kind: ItemKind) -> u32 {
        match kind {
            ItemKind::Symlink => SYMLINK_MODE,
            ItemKind::File => self.mode.unwrap_or(DEFAULT_FILE_MODE),
            ItemKind::Directory => self.mode.unwrap_or(DEFAULT_DIRECTORY_MODE),
        }
    }
}

/// Returns the modification time that a file shows: `fs.mtime_ms` when it
/// describes the current revision, and the revision's commit time otherwise,
/// so a revision that another client wrote without the attribute shows when
/// it was committed.
pub(crate) fn effective_file_mtime_ms(
    mtime: Option<MtimeAttribute>,
    revision_no: RevisionNo,
    committed_at_ms: u64,
) -> u64 {
    match mtime {
        Some(mtime) if mtime.revision_no == revision_no => mtime.mtime_ms,
        _ => committed_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_types::{AttributeKey, AttributeValue};
    use std::collections::BTreeMap;

    fn attributes(entries: &[(&str, &str)]) -> Attributes {
        Attributes::new(
            entries
                .iter()
                .map(|(key, value)| {
                    (
                        AttributeKey::parse(key).expect("attribute key"),
                        AttributeValue::parse(value).expect("attribute value"),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("attributes")
    }

    #[test]
    fn fs_attributes_set_the_kind_mode_and_time_and_ignore_values_that_dont_parse() {
        let symlink = FsAttributes::parse(&attributes(&[
            ("fs.type", "symlink"),
            ("fs.target", "../target"),
            ("fs.mode", "600"),
        ]));
        assert_eq!(symlink.kind(false), ItemKind::Symlink);
        assert_eq!(symlink.symlink_target.as_deref(), Some("../target"));
        assert_eq!(symlink.effective_mode(ItemKind::Symlink), 0o777);

        let file = FsAttributes::parse(&attributes(&[
            ("fs.mode", "755"),
            ("fs.mtime_ms", "1000"),
            ("fs.mtime_revision_no", "3"),
            ("owner", "platform"),
        ]));
        assert_eq!(file.kind(false), ItemKind::File);
        assert_eq!(file.effective_mode(ItemKind::File), 0o755);
        assert_eq!(effective_file_mtime_ms(file.mtime, RevisionNo(3), 9), 1000);
        assert_eq!(effective_file_mtime_ms(file.mtime, RevisionNo(4), 9), 9);

        let invalid =
            FsAttributes::parse(&attributes(&[("fs.mode", "rwx"), ("fs.mtime_ms", "1000")]));
        assert_eq!(invalid, FsAttributes::default());
        assert_eq!(invalid.effective_mode(ItemKind::Directory), 0o755);
    }
}
