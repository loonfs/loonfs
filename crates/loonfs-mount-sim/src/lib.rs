//! A simulated LoonFS server for the mount's tests.
//!
//! [`SimulatedServer`] serves the part of the v0 HTTP API that `loonfs mount` uses, as a `tower`
//! service that `loonfs_client::Client::with_transport` accepts, so the real client code runs
//! against it unchanged ("Testing" in the mount's design note, `docs/design/mount.md`, says what
//! the simulation is for).
//! Each namespace's metadata is a `loonfs-model` [`MetadataState`]. The server turns every request
//! operation into the format's WAL deltas (create an inode, bind and unbind a name, append a
//! revision, set or revoke a tombstone, append an attribute revision) and applies them with the
//! model, so bindings, visibility, and listings follow the model's rules instead of a copy of
//! them.
//!
//! The simulation adds what the model doesn't have: commit receipts and replays, preconditions
//! and their errors, the change feed, snapshots, a content store with uploads, ranged reads, and
//! download grants, and a retention floor. Time comes from the virtual clock of `loonfs-sim`,
//! which only a test advances. A test drives the server through [`SimulatedServer`]'s own methods
//! (commits without HTTP, the retention floor, the capability switches, and fault hooks), and it
//! reads every request the client sent from the server's trace ([`RequestRecord`]).
//!
//! # What the simulation doesn't model
//!
//! The simulation keeps the request and response shapes, the error codes, and the rules that the
//! mount depends on, and it leaves out the rest of the real server:
//!
//! - There's one process and no storage. A commit is visible to every read as soon as it's
//!   accepted, so `eventual` and `strong` reads both read the newest head (the server still
//!   validates the consistency parameters), and creating a snapshot folds nothing.
//! - Every namespace is unrestricted. The server checks the shape of the subject headers and puts
//!   the subject in the commit fingerprint, but it checks no rights, and an access update returns
//!   `namespace_unrestricted`. The bearer token isn't checked.
//! - The operations that the mount doesn't send return `not_supported`:
//!   `create_directory_by_inode`, `create_file_by_inode`, `copy_path`, `restore_revision`, and
//!   `restore_revision_by_inode`. A route that the mount doesn't call returns `route_not_found`.
//! - A commit is checked against the limits that the capability document advertises, but not
//!   against the size of the WAL object it would write, or the number of distinct external
//!   content references.
//! - The content store keeps each chain's bytes whole under its owner and content ID, and every
//!   checksum is CRC-64/NVME. A delta never includes a layout, and a download grant splits a
//!   revision into ranges of [`SimulatedServerConfig::max_download_range_bytes`] instead of one
//!   range for each extent.
//! - Uploads use the `service_proxied` transport only. A completed session issues a new content
//!   token on every read, with no window after which it stops.
//! - The test driver moves the retention floor to a sequence directly, without the rule that the
//!   floor stops at the newest commit that's at least an hour old. A floor advance removes the
//!   receipts at or below it and expires the deletions at or below it in one commit, but it keeps
//!   every file revision, so a read of a revision below the floor still succeeds.
//! - Nothing collects garbage, so an expired snapshot stays retrievable by its ID, although a read
//!   at it returns `snapshot_not_found`.
//! - The change feed's page budget counts the encoded JSON bytes of each commit instead of the
//!   commit's memory.
//! - Binding versions and content tokens are the simulation's own opaque strings.
//! - The model scans its rows for every query, so a commit costs time in proportion to the square
//!   of the namespace's row count. A simulated namespace should stay within a few thousand
//!   entries.
//!
//! [`MetadataState`]: loonfs_model::metadata::MetadataState

#![warn(missing_docs)]

mod commits;
mod config;
mod content_reads;
mod content_store;
mod downloads;
mod entries;
mod error;
mod faults;
mod feed;
mod listing;
mod model_view;
mod namespace;
mod query;
mod reply;
mod retention;
mod routes;
mod server;
mod service;
mod snapshots;
mod state;
mod tokens;
mod trace;
mod uploads;

pub use config::{CapabilitySwitches, InlineContent, SimulatedServerConfig};
pub use error::{MountSimError, Result};
pub use faults::{ContentReadFault, EchoOrder, Hold};
pub use server::{SimulatedServer, SIMULATED_SERVER_URL};
pub use service::SimulatedService;
pub use trace::{ByteRange, InjectedFault, Operation, RequestRecord};
