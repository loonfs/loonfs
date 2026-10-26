//! The errors that the kernel returns: for each engine error, and for each
//! request that the mount refuses.

use fuser::Errno;
use loonfs_mount::MountError;

/// Returns the error that the kernel returns for an engine error.
pub(crate) fn errno(error: &MountError) -> Errno {
    match error {
        MountError::ReadOnly { .. } => Errno::EROFS,
        MountError::ItemNotFound { .. } => Errno::ENOENT,
        MountError::NotAFile { .. } => Errno::EINVAL,
        MountError::NoSpace { .. } | MountError::InsufficientFreeSpace { .. } => Errno::ENOSPC,
        // The rest are failures of the server, the cache, or the engine,
        // which a program sees as an I/O error. The engine logs them.
        _ => Errno::EIO,
    }
}

/// A request that the mount refuses, as "The FUSE adapter" in the design note
/// lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// A change to the tree or to a file's bytes, which a read-only mount
    /// accepts none of.
    Change,
    /// `link`, as `link(2)` fails on a filesystem without hard links.
    HardLink,
    /// `mknod` of anything but a regular file, such as a socket or a FIFO.
    SpecialFile,
    /// `chown`, because every item shows the mount's owner.
    Chown,
    /// `ioctl`.
    Ioctl,
    /// `fallocate`.
    Fallocate,
    /// `copy_file_range`, so the kernel falls back to reads and writes.
    CopyFileRange,
    /// Getting, setting, listing, or removing an extended attribute.
    ExtendedAttribute,
}

impl Refusal {
    /// Returns the error that the kernel returns for the request.
    pub(crate) fn errno(self) -> Errno {
        match self {
            Self::Change => Errno::EROFS,
            Self::HardLink | Self::SpecialFile | Self::Chown => Errno::EPERM,
            Self::Ioctl | Self::Fallocate | Self::CopyFileRange => Errno::ENOSYS,
            Self::ExtendedAttribute => Errno::ENOTSUP,
        }
    }

    /// Returns the refusal of a `mknod` of `mode`: a regular file is a
    /// change, and anything else is a special file.
    pub(crate) fn of_mknod(mode: u32) -> Self {
        match rustix::fs::FileType::from_raw_mode(mode) {
            rustix::fs::FileType::RegularFile => Self::Change,
            _ => Self::SpecialFile,
        }
    }

    /// Returns the refusal of a `setattr`: one that changes the owner is a
    /// `chown`, and any other is a change.
    pub(crate) fn of_setattr(uid: Option<u32>, gid: Option<u32>) -> Self {
        if uid.is_some() || gid.is_some() {
            Self::Chown
        } else {
            Self::Change
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_mount::ItemId;

    /// Checks the design note's table of requests that the mount doesn't
    /// support, and that every change to a read-only mount is `EROFS`.
    #[test]
    fn refused_requests_follow_the_unsupported_request_table() {
        let table = [
            (Refusal::Change, Errno::EROFS),
            (Refusal::HardLink, Errno::EPERM),
            (Refusal::SpecialFile, Errno::EPERM),
            (Refusal::Chown, Errno::EPERM),
            (Refusal::Ioctl, Errno::ENOSYS),
            (Refusal::Fallocate, Errno::ENOSYS),
            (Refusal::CopyFileRange, Errno::ENOSYS),
            (Refusal::ExtendedAttribute, Errno::ENOTSUP),
        ];
        for (refusal, expected) in table {
            assert_eq!(refusal.errno(), expected, "{refusal:?}");
        }
        assert_eq!(Refusal::of_mknod(0o100_644), Refusal::Change);
        assert_eq!(Refusal::of_mknod(0o010_644), Refusal::SpecialFile);
        assert_eq!(Refusal::of_mknod(0o140_755), Refusal::SpecialFile);
        assert_eq!(Refusal::of_setattr(Some(0), None), Refusal::Chown);
        assert_eq!(Refusal::of_setattr(None, Some(0)), Refusal::Chown);
        assert_eq!(Refusal::of_setattr(None, None), Refusal::Change);
    }

    /// Checks the error that each kind of engine failure becomes.
    #[test]
    fn engine_errors_become_the_matching_errno() {
        assert_eq!(
            errno(&MountError::ReadOnly { operation: "mkdir" }),
            Errno::EROFS
        );
        assert_eq!(
            errno(&MountError::ItemNotFound { item: ItemId(3) }),
            Errno::ENOENT
        );
        assert_eq!(
            errno(&MountError::NoSpace {
                reason: "pinned bytes".to_owned()
            }),
            Errno::ENOSPC
        );
        assert_eq!(errno(&MountError::Stopped), Errno::EIO);
    }
}
