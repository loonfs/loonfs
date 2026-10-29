//! The `namespace`, `use`, and `snapshot` commands.

use super::flags::{ActorSelectorArgs, PaginationArgs};
use super::parse::parse_change_seq;
use clap::{Args, Subcommand, ValueEnum, ValueHint};
use loonfs_types::{ChangeSeq, NamespaceId, PinId, PrincipalId};
use std::num::NonZeroU64;

#[derive(Debug, Args)]
pub(crate) struct NamespaceUseArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: NamespaceId,
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespaceCommand {
    /// Create a new empty namespace.
    Create(NamespaceCreateArgs),
    /// Show a namespace's current status.
    Show(NamespaceShowArgs),
    /// Delete a namespace. The deletion can't be undone.
    Delete(NamespaceDeleteArgs),
    /// Fork a namespace into a new one.
    ///
    /// The fork shares the source's existing content and metadata objects
    /// without copying the filesystem. Forking the current head might first
    /// fold the source's outstanding WAL tail.
    Fork(NamespaceForkArgs),
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceShowArgs {
    /// Namespace to show. When omitted, the usual namespace selection applies.
    #[arg(value_hint = ValueHint::Other, conflicts_with = "namespace")]
    pub namespace_id: Option<NamespaceId>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum NamespaceAccessArg {
    Unrestricted,
    Acl,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceCreateArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: NamespaceId,
    /// Access mode. It can't be changed after the namespace is created.
    #[arg(long, value_enum, default_value_t = NamespaceAccessArg::Unrestricted)]
    pub access: NamespaceAccessArg,
    /// A principal to grant `admin` on the root at creation. Repeat the flag
    /// for more principals.
    #[arg(long = "administrator", required_if_eq("access", "acl"), value_hint = ValueHint::Other)]
    pub administrators: Vec<PrincipalId>,
    /// The namespace's retention window, in milliseconds. The server's
    /// maintenance sweep keeps this many milliseconds of the namespace's
    /// history, in place of the server's `retention_window_ms`.
    /// `loonfs maintenance retention window` can change or clear the window
    /// later.
    #[arg(long)]
    pub retention_window_ms: Option<NonZeroU64>,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceDeleteArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: NamespaceId,
    /// Delete only if the namespace head is still at this sequence.
    #[arg(long, value_parser = parse_change_seq)]
    pub expected_head_seq: Option<ChangeSeq>,
    /// Skip the interactive confirmation.
    #[arg(long)]
    pub yes: bool,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceForkArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub source_namespace_id: NamespaceId,
    #[arg(value_hint = ValueHint::Other)]
    pub new_namespace_id: NamespaceId,
    /// Fork from an unexpired snapshot instead of the current head.
    #[arg(long = "snapshot", value_name = "id")]
    pub snapshot_id: Option<PinId>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SnapshotCommand {
    /// Save the namespace's current state as a new snapshot.
    ///
    /// Every call creates a new snapshot. The name is a label, not a key.
    Create(SnapshotCreateArgs),
    /// List snapshots in snapshot ID order.
    ///
    /// Expired snapshots remain listed until garbage collection deletes them.
    List(SnapshotListArgs),
    /// Keep an unexpired snapshot available for longer.
    Extend(SnapshotExtendArgs),
    /// Delete the snapshot record. Deleting a missing snapshot succeeds.
    Delete(SnapshotDeleteArgs),
}

#[derive(Debug, Args)]
pub(crate) struct SnapshotTargetArgs {
    /// Namespace whose snapshots to manage.
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: NamespaceId,
}

#[derive(Debug, Args)]
pub(crate) struct SnapshotCreateArgs {
    #[command(flatten)]
    pub target: SnapshotTargetArgs,
    /// Snapshot label. Labels don't need to be unique.
    #[arg(long)]
    pub name: String,
    /// How long the snapshot remains available, in milliseconds. Without
    /// it, the snapshot lasts until it's deleted.
    #[arg(long)]
    pub ttl_ms: Option<NonZeroU64>,
}

#[derive(Debug, Args)]
pub(crate) struct SnapshotListArgs {
    #[command(flatten)]
    pub target: SnapshotTargetArgs,
    #[command(flatten)]
    pub pagination: PaginationArgs,
}

#[derive(Debug, Args)]
pub(crate) struct SnapshotExtendArgs {
    #[command(flatten)]
    pub target: SnapshotTargetArgs,
    /// Snapshot ID to extend.
    #[arg(value_hint = ValueHint::Other)]
    pub snapshot_id: PinId,
    /// New lifetime from now, in milliseconds. The expiry never moves
    /// earlier.
    #[arg(long)]
    pub ttl_ms: NonZeroU64,
}

#[derive(Debug, Args)]
pub(crate) struct SnapshotDeleteArgs {
    #[command(flatten)]
    pub target: SnapshotTargetArgs,
    /// Snapshot ID to delete.
    #[arg(value_hint = ValueHint::Other)]
    pub snapshot_id: PinId,
}
