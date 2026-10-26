//! The host checks that admission runs before it reads the namespace: the
//! state directory's filesystem, its free space, and whether it has
//! reflinks.

use crate::config::MountConfig;
use crate::error::{MountError, Result};
use crate::status::ReflinkSupport;
use std::fs::{self, File};
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;

/// The size of the file that the sparse-file check creates without writing
/// it.
const SPARSE_CHECK_BYTES: u64 = 16 << 20;

/// Reports the free space of a filesystem.
pub(crate) trait DiskSpace: std::fmt::Debug + Send + Sync {
    /// Returns the bytes that the daemon's user can still write on the
    /// filesystem that contains `path`.
    fn available_bytes(&self, path: &Path) -> std::io::Result<u64>;
}

/// The host's filesystems.
#[derive(Debug, Default)]
pub(crate) struct HostDiskSpace;

impl DiskSpace for HostDiskSpace {
    fn available_bytes(&self, path: &Path) -> std::io::Result<u64> {
        fs4::available_space(path)
    }
}

/// What the host checks found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HostReport {
    pub(crate) reflinks: ReflinkSupport,
}

/// Creates the mount's state directories and checks that they're on one
/// filesystem that supports sparse files and has the free space that
/// `--cache-min-free` sets.
pub(crate) fn check_host(config: &MountConfig, disk: &dyn DiskSpace) -> Result<HostReport> {
    let root = &config.state_directory;
    let directories = [
        root.clone(),
        config.cache_directory(),
        root.join("staging"),
        root.join("captures"),
    ];
    for directory in &directories {
        fs::create_dir_all(directory).map_err(|error| MountError::io(directory, &error))?;
    }
    let device = |path: &Path| {
        fs::metadata(path)
            .map(|metadata| metadata.dev())
            .map_err(|error| MountError::io(path, &error))
    };
    let root_device = device(root)?;
    for directory in &directories[1..] {
        if device(directory)? != root_device {
            return Err(MountError::HostUnsupported {
                reason: format!(
                    "`{}` is on another filesystem than `{}`, and the cache, working files, and \
                     captures have to be on one filesystem",
                    directory.display(),
                    root.display()
                ),
            });
        }
    }
    check_sparse_files(&config.cache_directory())?;
    let available_bytes = disk
        .available_bytes(root)
        .map_err(|error| MountError::io(root, &error))?;
    if available_bytes < config.min_free_bytes {
        return Err(MountError::InsufficientFreeSpace {
            path: root.display().to_string(),
            available_bytes,
            min_free_bytes: config.min_free_bytes,
        });
    }
    Ok(HostReport {
        reflinks: check_reflinks(&config.cache_directory())?,
    })
}

/// Checks that extending a file without writing it allocates no blocks.
fn check_sparse_files(directory: &Path) -> Result<()> {
    let path = directory.join(".sparse-check");
    let file = File::create(&path).map_err(|error| MountError::io(&path, &error))?;
    let allocated = file
        .set_len(SPARSE_CHECK_BYTES)
        .and_then(|()| file.metadata())
        .map(|metadata| metadata.blocks() * 512);
    drop(file);
    let removed = fs::remove_file(&path);
    let allocated = allocated.map_err(|error| MountError::io(&path, &error))?;
    removed.map_err(|error| MountError::io(&path, &error))?;
    if allocated >= SPARSE_CHECK_BYTES {
        return Err(MountError::HostUnsupported {
            reason: format!(
                "the filesystem of `{}` doesn't support sparse files",
                directory.display()
            ),
        });
    }
    Ok(())
}

/// Checks whether the filesystem clones a file with `FICLONE`.
#[cfg(target_os = "linux")]
fn check_reflinks(directory: &Path) -> Result<ReflinkSupport> {
    use std::io::Write as _;
    let source_path = directory.join(".reflink-check-source");
    let clone_path = directory.join(".reflink-check-clone");
    let cloned = File::create(&source_path)
        .and_then(|mut source| {
            source.write_all(&[0; 4096])?;
            source.sync_all()?;
            let clone = File::create(&clone_path)?;
            Ok(rustix::fs::ioctl_ficlone(&clone, &source).is_ok())
        })
        .map_err(|error| MountError::io(&source_path, &error));
    for path in [&source_path, &clone_path] {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(MountError::io(path, &error)),
        }
    }
    Ok(if cloned? {
        ReflinkSupport::Supported
    } else {
        ReflinkSupport::Unsupported
    })
}

/// Reports no reflinks, because mounts run on Linux only.
#[cfg(not(target_os = "linux"))]
fn check_reflinks(_directory: &Path) -> Result<ReflinkSupport> {
    Ok(ReflinkSupport::Unsupported)
}
