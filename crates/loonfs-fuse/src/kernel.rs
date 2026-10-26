//! [`FuseKernel`], which mounts engines through FUSE for the daemon, and
//! detaches the mounts.

use crate::filesystem::MountFilesystem;
use crate::invalidations;
use crate::mount_table::{has_loonfs_mount, MOUNT_SOURCE};
use fuser::{MountOption, Session, SessionACL, SessionUnmounter};
use loonfs_mount::daemon::{Kernel, KernelMount, StaleMountOutcome};
use loonfs_mount::{Engine, MountAccess, MountMethod};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const FUSE_DEVICE: &str = "/dev/fuse";
const FUSE_CONFIG: &str = "/etc/fuse.conf";
const MOUNT_HELPER: &str = "fusermount3";

/// The bit of `CAP_SYS_ADMIN` in a capability set.
const CAP_SYS_ADMIN_BIT: u32 = 21;

/// The FUSE kernel interface of the daemon on Linux.
#[derive(Debug, Default)]
pub struct FuseKernel;

impl FuseKernel {
    /// Returns the interface.
    pub fn new() -> Self {
        Self
    }
}

impl Kernel for FuseKernel {
    fn check_host(&self) -> io::Result<()> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(FUSE_DEVICE)
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("`{FUSE_DEVICE}` can't be opened: {error}"),
                )
            })?;
        match mount_method()? {
            MountMethod::MountSyscall => Ok(()),
            MountMethod::Fusermount3 => check_mount_helper(),
        }
    }

    fn detach_stale(&self, directory: &Path) -> io::Result<StaleMountOutcome> {
        if !has_loonfs_mount(directory)? {
            return Ok(StaleMountOutcome::NotMounted);
        }
        tracing::info!(directory = %directory.display(), "detaching a stale mount");
        detach(directory)?;
        Ok(StaleMountOutcome::Detached)
    }

    fn mount(&self, engine: Arc<Engine>, directory: &Path) -> io::Result<Box<dyn KernelMount>> {
        let method = mount_method()?;
        let mut options = fuser::Config::default();
        options.mount_options = vec![
            MountOption::FSName(MOUNT_SOURCE.to_owned()),
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::NoSuid,
        ];
        if engine.config().access == MountAccess::ReadOnly {
            options.mount_options.push(MountOption::RO);
        }
        // The session sets `allow_other`, so users other than the daemon's
        // can use the mount. Without it, `fuser` also refuses their requests
        // itself.
        options.acl = SessionACL::All;
        let invalidations = engine.subscribe_invalidations();
        let filesystem = MountFilesystem::new(engine.clone(), tokio::runtime::Handle::current());
        let mut session = Session::new(filesystem, directory, &options)?;
        engine.record_mount_method(method);
        let unmounter = session.unmount_callable();
        invalidations::start(invalidations, session.notifier())?;
        let thread = std::thread::Builder::new()
            .name("loonfs-fuse-session".to_owned())
            .spawn(move || session.run())?;
        Ok(Box::new(FuseMount {
            directory: directory.to_path_buf(),
            unmounter,
            thread,
        }))
    }
}

/// One mount that [`FuseKernel`] started, served by a session thread.
struct FuseMount {
    directory: PathBuf,
    unmounter: SessionUnmounter,
    thread: std::thread::JoinHandle<io::Result<()>>,
}

impl KernelMount for FuseMount {
    fn detach(&mut self) -> io::Result<()> {
        // `fuser` unmounts once, as root with `umount(2)` and otherwise with
        // a lazy `fusermount3 -u`, and a second call does nothing. A mount
        // that programs still use fails with `EBUSY`, and then the mount is
        // detached instead, which leaves their files open.
        match self.unmounter.unmount() {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(rustix::io::Errno::BUSY.raw_os_error()) => {
                detach(&self.directory)?;
            }
            Err(error) => return Err(error),
        }
        // `fuser` ignores a failure of `fusermount3`, so the adapter reads the
        // mount table to check that the mount is gone.
        if has_loonfs_mount(&self.directory)? {
            return Err(io::Error::other(format!(
                "`{}` is still mounted",
                self.directory.display()
            )));
        }
        Ok(())
    }

    fn wait(self: Box<Self>) {
        match self.thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "a FUSE session ended with an error"),
            Err(_) => tracing::error!("a FUSE session's thread panicked"),
        }
    }
}

/// Detaches the mount at `directory`, with `umount2(MNT_DETACH)` as root and
/// with `fusermount3 -u -z` otherwise.
fn detach(directory: &Path) -> io::Result<()> {
    match rustix::mount::unmount(directory, rustix::mount::UnmountFlags::DETACH) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::PERM) => {
            let output = Command::new(MOUNT_HELPER)
                .arg("-u")
                .arg("-z")
                .arg("--")
                .arg(directory)
                .output()?;
            if output.status.success() {
                Ok(())
            } else {
                Err(io::Error::other(format!(
                    "`{MOUNT_HELPER} -u -z` failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Returns how `fuser` mounts: with `mount(2)` when the daemon has
/// `CAP_SYS_ADMIN`, and with `fusermount3` after `mount(2)` fails with
/// `EPERM` otherwise.
fn mount_method() -> io::Result<MountMethod> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let effective = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
        .ok_or_else(|| io::Error::other("`/proc/self/status` has no effective capabilities"))?;
    Ok(if effective & (1 << CAP_SYS_ADMIN_BIT) != 0 {
        MountMethod::MountSyscall
    } else {
        MountMethod::Fusermount3
    })
}

/// Checks what a daemon that isn't root needs: `fusermount3`, and
/// `user_allow_other` in `/etc/fuse.conf` for `allow_other`.
fn check_mount_helper() -> io::Result<()> {
    let found = Command::new(MOUNT_HELPER).arg("--version").output().is_ok();
    if !found {
        return Err(io::Error::other(format!(
            "the daemon isn't root, and `{MOUNT_HELPER}` isn't installed, so install \
             `{MOUNT_HELPER}` or run the daemon as root"
        )));
    }
    let config = std::fs::read_to_string(FUSE_CONFIG).unwrap_or_default();
    if !allows_other_users(&config) {
        return Err(io::Error::other(format!(
            "the daemon isn't root, so it mounts with `{MOUNT_HELPER}`, which allows other users \
             only with `user_allow_other` in `{FUSE_CONFIG}`, so add that line to `{FUSE_CONFIG}` \
             or run the daemon as root"
        )));
    }
    Ok(())
}

fn allows_other_users(config: &str) -> bool {
    config
        .lines()
        .map(|line| line.split('#').next().unwrap_or_default().trim())
        .any(|line| line == "user_allow_other")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuse_config_allows_other_users_only_when_the_option_is_set() {
        assert!(allows_other_users("# comment\nuser_allow_other\n"));
        assert!(allows_other_users("  user_allow_other  # for loonfs\n"));
        assert!(!allows_other_users("#user_allow_other\nmount_max = 1000\n"));
        assert!(!allows_other_users(""));
    }
}
