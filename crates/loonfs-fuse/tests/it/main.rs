//! Mounts a namespace on a real kernel, through the CLI and the daemon, and
//! checks what programs see.
//!
//! The tests are ignored by default, because they mount a FUSE filesystem.
//! To run them on a Linux host with `/dev/fuse`, build the two binaries that
//! they run, then run the ignored tests:
//!
//! ```text
//! cargo build -p loonfs-cli -p loonfs-server
//! cargo test -p loonfs-fuse --test it -- --ignored
//! ```
//!
//! The daemon mounts with `allow_other`, which a daemon that isn't root can
//! do only with `user_allow_other` in `/etc/fuse.conf`. So when the tests
//! don't run as root, they run the CLI, and with it the daemon, through
//! `sudo -n`, and the test's own reads go through `allow_other`.

#![cfg(target_os = "linux")]
#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod harness;
mod mount;
