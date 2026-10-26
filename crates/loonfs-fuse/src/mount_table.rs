//! The host's mount table, which shows whether a directory has a mount that
//! a LoonFS daemon made.

use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt as _;
use std::path::{Path, PathBuf};

/// The source name of every LoonFS mount, which `mount` shows.
pub(crate) const MOUNT_SOURCE: &str = "loonfs";

const MOUNT_INFO_PATH: &str = "/proc/self/mountinfo";

/// One line of `/proc/self/mountinfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MountInfo {
    mount_point: PathBuf,
    filesystem_type: String,
    source: String,
}

/// Returns whether `directory` has a mount that a LoonFS daemon made.
pub(crate) fn has_loonfs_mount(directory: &Path) -> std::io::Result<bool> {
    let table = std::fs::read_to_string(MOUNT_INFO_PATH)?;
    Ok(table
        .lines()
        .filter_map(parse_line)
        .any(|mount| mount.mount_point == directory && is_loonfs(&mount)))
}

fn is_loonfs(mount: &MountInfo) -> bool {
    (mount.filesystem_type == "fuse" || mount.filesystem_type.starts_with("fuse."))
        && mount.source == MOUNT_SOURCE
}

/// Parses one line: the mount point is the fifth field, and the filesystem
/// type and the source are the two fields after the `-` separator.
fn parse_line(line: &str) -> Option<MountInfo> {
    let mut fields = line.split(' ');
    let mount_point = fields.nth(4)?;
    let mut after_separator = fields.skip_while(|field| *field != "-").skip(1);
    let filesystem_type = after_separator.next()?;
    let source = after_separator.next()?;
    Some(MountInfo {
        mount_point: PathBuf::from(unescape(mount_point)),
        filesystem_type: unescape(filesystem_type).to_string_lossy().into_owned(),
        source: unescape(source).to_string_lossy().into_owned(),
    })
}

/// Decodes the octal escapes (such as `\040` for a space) that the kernel
/// writes for whitespace and backslashes in a field.
fn unescape(field: &str) -> OsString {
    let bytes = field.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let escape = bytes
            .get(index + 1..index + 4)
            .filter(|_| bytes[index] == b'\\')
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        match escape {
            Some(byte) => {
                decoded.push(byte);
                index += 4;
            }
            None => {
                decoded.push(bytes[index]);
                index += 1;
            }
        }
    }
    OsString::from_vec(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that a line's mount point is unescaped and that only a FUSE
    /// mount with the `loonfs` source counts as a LoonFS mount.
    #[test]
    fn mount_info_lines_identify_loonfs_mounts() {
        let ours = parse_line(
            "61 26 0:52 / /tmp/my\\040mount rw,nosuid,nodev,relatime shared:31 - fuse loonfs \
             ro,user_id=0,group_id=0,default_permissions,allow_other",
        )
        .expect("a mount line");
        assert_eq!(ours.mount_point, Path::new("/tmp/my mount"));
        assert!(is_loonfs(&ours));

        let helper = parse_line("62 26 0:53 / /mnt/a rw - fuse.loonfs loonfs rw,user_id=1000")
            .expect("a mount line");
        assert!(is_loonfs(&helper));

        for line in [
            "63 26 0:54 / /mnt/b rw - fuse.other host:/ rw",
            "64 26 0:55 / /mnt/c rw - ext4 loonfs rw",
        ] {
            let other = parse_line(line).expect("a mount line");
            assert!(!is_loonfs(&other), "{line}");
        }
        assert_eq!(parse_line("too short"), None);
    }
}
