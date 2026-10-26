//! The FUSE adapter of `loonfs mount`, which serves the kernel's requests for
//! a mounted directory from the mount's [`Engine`](loonfs_mount::Engine).
//!
//! [`FuseKernel`] implements the daemon's
//! [`Kernel`](loonfs_mount::daemon::Kernel) interface with `fuser`: it
//! mounts with `default_permissions` and `allow_other`, read-only when the
//! mount is, serves metadata from the engine's tree and content through the
//! engine's read handles, shows the control directory `.loonfs`, and sends
//! the kernel the engine's invalidations. "The FUSE adapter" in
//! `docs/design/mount.md` describes it.
//!
//! The adapter is built on Linux only. On every other platform the crate is
//! empty.

#![cfg(target_os = "linux")]

mod attributes;
mod control_directory;
mod directory;
mod errors;
mod filesystem;
mod invalidations;
mod kernel;
mod mount_table;
mod open_files;

pub use kernel::FuseKernel;
