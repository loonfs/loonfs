//! The daemon that serves one host user's mounts, and the protocol of its
//! socket.
//!
//! `loonfs mount` starts the daemon (as the hidden `loonfs daemon` command)
//! when none is running, and sends it requests over a Unix socket, one JSON
//! object per line (see [`protocol`]). The daemon admits each mount with an
//! [`Engine`](crate::Engine), and starts and stops kernel mounts through a
//! [`Kernel`], which the FUSE adapter implements. "Crates and processes" in
//! the design note describes the daemon.

mod client;
mod error;
mod kernel;
mod mounts;
mod paths;
pub mod protocol;
mod server;

pub use client::DaemonClient;
pub use error::DaemonError;
pub use kernel::{Kernel, KernelMount, StaleMountOutcome};
pub use paths::DaemonPaths;
pub use server::Daemon;
