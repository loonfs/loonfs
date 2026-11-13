//! The `namespace`, `use`, `merge`, and `snapshot` commands.

use super::flags::{ActorSelectorArgs, PaginationArgs};
use super::parse::parse_change_seq;
use clap::{Args, Subcommand, ValueEnum, ValueHint};
use loonfs_types::{CommitSeq, NamespaceId, PrincipalId, SnapshotId};
use std::num::NonZeroU64;
use std::path::PathBuf;

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
    /// ID of the new namespace. When omitted, the CLI generates an ID.
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: Option<NamespaceId>,
    /// Access mode. It can't be changed after the namespace is created.
    #[arg(long, value_enum, default_value_t = NamespaceAccessArg::Unrestricted)]
    pub access: NamespaceAccessArg,
    /// A principal to grant `admin` on the root at creation. Repeat the flag
    /// for more principals.
    #[arg(long = "administrator", required_if_eq("access", "acl"), value_hint = ValueHint::Other)]
    pub administrators: Vec<PrincipalId>,
    /// The namespace's retention window, in milliseconds. The server's
    /// maintenance sweep keeps this many milliseconds of the namespace's
    /// history. Without this option, the namespace's window is the server's
    /// `retention_window_ms` setting, if the server has that setting.
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
    pub expected_head_seq: Option<CommitSeq>,
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
    /// ID of the new namespace. When omitted, the CLI generates an ID.
    #[arg(value_hint = ValueHint::Other)]
    pub new_namespace_id: Option<NamespaceId>,
    /// Fork from an unexpired snapshot instead of the current head.
    #[arg(long = "snapshot", value_name = "id")]
    pub snapshot_id: Option<SnapshotId>,
}

#[derive(Debug, Args)]
pub(crate) struct MergeArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    /// Namespace to merge into.
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: NamespaceId,
    /// The fork whose commits to merge. It has to be a fork of the namespace.
    #[arg(long = "source", value_name = "fork", value_hint = ValueHint::Other)]
    pub source_namespace_id: NamespaceId,
    /// Merge the fork's commits through this sequence instead of the fork's
    /// head when the session starts.
    #[arg(long = "through", value_name = "seq", value_parser = parse_change_seq)]
    pub through_seq: Option<CommitSeq>,
    /// Record file that keeps the session between runs. Defaults to a file
    /// under $XDG_STATE_HOME/loonfs/merges for this profile, namespace, and
    /// fork.
    #[arg(long, value_name = "path", value_hint = ValueHint::FilePath)]
    pub record: Option<PathBuf>,
    /// Abandon the failed session that the record keeps, and delete the new
    /// forks that its rebases created.
    #[arg(long, conflicts_with = "through_seq")]
    pub abandon: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SnapshotCommand {
    /// Save the namespace's current state as a new snapshot.
    ///
    /// Every call creates a new snapshot. The name is an optional label, not a
    /// key.
    Create(SnapshotCreateArgs),
    /// List snapshots in snapshot ID order.
    ///
    /// Expired and deleted snapshots are left out.
    List(SnapshotListArgs),
    /// Keep an unexpired snapshot available for longer.
    Extend(SnapshotExtendArgs),
    /// Delete the snapshot. Deleting a missing snapshot succeeds.
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
    /// Snapshot label. Labels don't need to be unique. Without it, the
    /// snapshot has no label.
    #[arg(long)]
    pub name: Option<String>,
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
    pub snapshot_id: SnapshotId,
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
    pub snapshot_id: SnapshotId,
}
