//! The interface through which the daemon starts and stops kernel mounts.
//!
//! The FUSE adapter implements it on Linux, and a simulation can implement it
//! with a fake kernel, so the daemon itself contains no kernel code.

use crate::engine::Engine;
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Starts kernel mounts of engines.
///
/// The daemon calls every method from a blocking thread of its Tokio
/// runtime, so an implementation can block, and can read the runtime's
/// handle with `tokio::runtime::Handle::current`.
pub trait Kernel: Send + Sync + 'static {
    /// Checks the host checks that belong to the kernel: that the daemon can
    /// open the device that serves mounts, and that it can mount.
    ///
    /// The error's message says what's missing, and the daemon refuses the
    /// mount with it before it reads the namespace.
    fn check_host(&self) -> io::Result<()>;

    /// Detaches a mount of `directory` that a daemon which stopped left
    /// behind, and does nothing when `directory` has no such mount.
    fn detach_stale(&self, directory: &Path) -> io::Result<StaleMountOutcome>;

    /// Mounts `engine` at `directory`, serves the kernel's requests from it
    /// until the mount is released, and records in the engine how it
    /// mounted.
    fn mount(&self, engine: Arc<Engine>, directory: &Path) -> io::Result<Box<dyn KernelMount>>;
}

/// What [`Kernel::detach_stale`] found at a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleMountOutcome {
    /// The directory had a mount that a stopped daemon left, and it's
    /// detached now.
    Detached,
    /// The directory had no such mount.
    NotMounted,
}

/// One kernel mount that [`Kernel::mount`] started.
pub trait KernelMount: Send + 'static {
    /// Detaches the mount from its directory, so no new program can reach
    /// it. Programs that have files open in it keep using them. A second
    /// call does nothing.
    fn detach(&mut self) -> io::Result<()>;

    /// Waits until the kernel releases the mount, which happens once it's
    /// detached and the last program closes its files in it, and drops
    /// every reference to the engine that the mount kept.
    fn wait(self: Box<Self>);
}
