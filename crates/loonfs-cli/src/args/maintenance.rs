//! The `maintenance` commands.

use super::flags::{ActorSelectorArgs, PaginationArgs};
use super::parse::parse_change_seq;
use clap::{Args, Subcommand, ValueHint};
use loonfs_types::{ChangeSeq, PrincipalId};
use std::num::NonZeroU64;

#[derive(Debug, Subcommand)]
pub(crate) enum MaintenanceCommand {
    /// Restore a principal's administrator grant on the root.
    RecoverAdministrator(MaintenanceRecoverAdministratorArgs),
    /// Fold the WAL tail if it's due, and compact metadata until nothing is due.
    ///
    /// A compaction unit can take minutes, so the command can too. It
    /// stops after 16 compaction units. A `due` outcome means that work
    /// remains, and a later run continues it.
    Metadata(MaintenanceMetadataArgs),
    /// Fold the WAL tail whatever its length, and compact metadata until
    /// nothing is due.
    Fold,
    /// List the checkpoint inventory: every pin, snapshots and fork pins.
    Checkpoint {
        #[command(subcommand)]
        command: MaintenanceCheckpointCommand,
    },
    /// Inspect and manage the gram content index.
    Index {
        #[command(subcommand)]
        command: MaintenanceIndexCommand,
    },
    /// Manage retained replay history.
    Retention {
        #[command(subcommand)]
        command: MaintenanceRetentionCommand,
    },
    /// Collect aged, unreferenced objects, including gram index objects when the deployment
    /// maintains the index.
    Gc(MaintenanceGcArgs),
    /// Inspect and manage the profile's object store.
    Store {
        #[command(subcommand)]
        command: MaintenanceStoreCommand,
    },
}

#[derive(Debug, Args)]
pub(crate) struct MaintenanceRecoverAdministratorArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub principal_id: PrincipalId,
}

#[derive(Debug, Subcommand)]
pub(crate) enum MaintenanceCheckpointCommand {
    /// List every pin in checkpoint ID order, snapshots and fork pins.
    ///
    /// Expired snapshots remain listed until garbage collection deletes them.
    List(MaintenanceCheckpointListArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum MaintenanceIndexCommand {
    /// Enable the gram content index and wait for its backfill to reach the
    /// sequence the namespace was at when this command started.
    Enable(MaintenanceIndexEnableArgs),
    /// Disable the gram content index.
    Disable,
    /// Show whether the gram content index is disabled, backfilling, or active.
    Status,
}

#[derive(Debug, Subcommand)]
pub(crate) enum MaintenanceRetentionCommand {
    /// Advance the retention floor and discard older replay history.
    ///
    /// The floor moves to the folded manifest head unless a target is given.
    /// Whatever the target, the floor doesn't pass a commit that's less than
    /// one hour old, and the output shows the floor that the advance reached.
    /// A later base compaction keeps every file revision above the floor and
    /// each file's newest revision at or below it.
    Advance(MaintenanceRetentionAdvanceArgs),
    /// Set or clear the namespace's retention window.
    ///
    /// The server's maintenance sweep keeps this much of the namespace's
    /// history instead of applying the server's own window. Once the
    /// namespace's window is cleared, the server's window applies again.
    Window(MaintenanceRetentionWindowArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum MaintenanceStoreCommand {
    /// Test the object-store operations LoonFS requires.
    Probe,
}

#[derive(Debug, Args)]
pub(crate) struct MaintenanceIndexEnableArgs {
    /// Return as soon as the index is enabled, without waiting for the
    /// backfill.
    #[arg(long)]
    pub no_wait: bool,
    /// Stop after this many steps (one bounded index step on an embedded
    /// profile, or one status check on a remote profile). The command exits
    /// nonzero and reports how far the index got.
    #[arg(long)]
    pub max_steps: Option<u64>,
    /// Stop after this many milliseconds. The command exits nonzero and
    /// reports how far the index got.
    #[arg(long)]
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct MaintenanceRetentionAdvanceArgs {
    /// Advance the floor to this sequence, or to the folded manifest head
    /// when that's lower.
    #[arg(long, conflicts_with = "before", value_parser = parse_change_seq)]
    pub to_seq: Option<ChangeSeq>,
    /// Advance the floor to the last commit made at or before this Unix
    /// time, in milliseconds.
    #[arg(long, value_name = "UNIX_MS")]
    pub before: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct MaintenanceRetentionWindowArgs {
    /// The namespace's new retention window, in milliseconds.
    #[arg(long, required_unless_present = "clear", conflicts_with = "clear")]
    pub retention_window_ms: Option<NonZeroU64>,
    /// Clear the namespace's window, so that the server's setting applies.
    #[arg(long)]
    pub clear: bool,
}

#[derive(Debug, Args)]
pub(crate) struct MaintenanceCheckpointListArgs {
    #[command(flatten)]
    pub pagination: PaginationArgs,
}

#[derive(Debug, Args)]
pub(crate) struct MaintenanceMetadataArgs {
    /// Fold the visible WAL tail into metadata segments when it reaches this
    /// many WAL objects (server default when omitted).
    #[arg(long)]
    pub max_wal_tail_objects: Option<NonZeroU64>,
}

#[derive(Debug, Args)]
pub(crate) struct MaintenanceGcArgs {
    /// Objects younger than this are never deleted (server default when
    /// omitted).
    #[arg(long)]
    pub grace_window_ms: Option<u64>,
}
