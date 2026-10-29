//! The filesystem commands, `access`, `trash`, and `changes`.

use super::flags::{ActorSelectorArgs, CommitArgs, PaginationArgs, SeqPaginationArgs};
use super::parse::{
    parse_access_revision_no, parse_attributes_revision_no, parse_change_seq,
    parse_public_inode_id, parse_revision_no,
};
use clap::{ArgGroup, Args, Subcommand, ValueHint};
use loonfs_types::{AccessRevisionNo, AttributesRevisionNo, ChangeSeq, InodeId, PinId, RevisionNo};

#[derive(Debug, Args)]
pub(crate) struct FilesystemLsArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub path: Option<String>,
    #[command(flatten)]
    pub pagination: PaginationArgs,
    /// Read from this snapshot instead of the current state.
    #[arg(long, value_hint = ValueHint::Other)]
    pub snapshot_id: Option<PinId>,
}

#[derive(Debug, Args)]
#[command(
    override_usage = "loonfs stat <PATH>\n       loonfs stat --inode <INODE_ID>",
    group(
        ArgGroup::new("stat_target")
            .required(true)
            .multiple(false)
            .args(["path", "inode"])
    )
)]
pub(crate) struct FilesystemStatArgs {
    /// Absolute path to describe.
    #[arg(value_hint = ValueHint::Other)]
    pub path: Option<String>,
    /// Visible inode to describe instead of a path.
    #[arg(long, value_name = "INODE_ID", value_parser = parse_public_inode_id)]
    pub inode: Option<InodeId>,
    /// Read from this snapshot instead of the current state.
    #[arg(long, value_hint = ValueHint::Other)]
    pub snapshot_id: Option<PinId>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum AccessCommand {
    /// Replace the access row of a path with the grants given.
    Set(AccessSetArgs),
}

#[derive(Debug, Args)]
pub(crate) struct AccessSetArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// A grant as `principal=right[,right...]`. Repeat the flag for more
    /// principals. Rights are read, history, write, create, remove, share,
    /// manage, and admin (root only).
    #[arg(long = "grant")]
    pub grants: Vec<String>,
    /// Stop inheritance at this directory.
    #[arg(long)]
    pub boundary: bool,
    /// Replace only while the path still resolves to this inode. If the path
    /// now points to another inode, the command fails instead of changing
    /// that inode's access.
    #[arg(long, value_parser = parse_public_inode_id)]
    pub expected_inode_id: Option<InodeId>,
    /// Replace only while the row is still at this access revision. Needs
    /// --expected-inode-id, which identifies the row.
    #[arg(long, value_hint = ValueHint::Other, requires = "expected_inode_id", value_parser = parse_access_revision_no)]
    pub expected_revision: Option<AccessRevisionNo>,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemAnnotateArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Attribute to write, as `key=value`. The key ends at the first `=`, so
    /// the value can contain more of them. Repeat the flag to write more.
    #[arg(long = "set", conflicts_with = "attributes_json")]
    pub sets: Vec<String>,
    /// Attribute key to remove. Repeat the flag to remove more.
    #[arg(long = "remove", conflicts_with = "attributes_json")]
    pub removes: Vec<String>,
    /// The whole update as one JSON object, `{"set": {...}, "remove": [...]}`,
    /// with values as strings, for example `{"set": {"owner": "ada"}}`.
    /// Scripts use it to pass an update they built. It can't be combined with
    /// --set or --remove. (It isn't called --json, because --json is the
    /// global output-format flag.)
    #[arg(long)]
    pub attributes_json: Option<String>,
    /// Update only while the path still resolves to this inode. If the path
    /// now points to another inode, the command fails instead of annotating
    /// that inode.
    #[arg(long, value_parser = parse_public_inode_id)]
    pub expected_inode_id: Option<InodeId>,
    /// Update only while the inode's attribute revision is still this one.
    /// Requires --expected-inode-id.
    #[arg(long, requires = "expected_inode_id", value_parser = parse_attributes_revision_no)]
    pub expected_attributes_revision: Option<AttributesRevisionNo>,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRmArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Delete a directory and everything under it, as one commit. The whole
    /// subtree stays recoverable through the printed undelete handle.
    #[arg(short, long)]
    pub recursive: bool,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemMkdirArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Create missing parent directories as well.
    #[arg(short = 'p', long)]
    pub parents: bool,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRevisionsArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    #[command(flatten)]
    pub pagination: PaginationArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemCatArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Print this revision instead of the current content.
    #[arg(long, value_parser = parse_revision_no)]
    pub revision: Option<RevisionNo>,
    /// Read from this snapshot instead of the current state.
    #[arg(long, value_hint = ValueHint::Other, conflicts_with = "revision")]
    pub snapshot_id: Option<PinId>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemGrepArgs {
    /// Pattern in the Rust regex dialect. `^` and `$` match at line boundaries.
    #[arg(value_hint = ValueHint::Other)]
    pub pattern: String,
    /// Restrict matches to files under this absolute path prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub path_prefix: Option<String>,
    /// Match ASCII letters without regard to case.
    #[arg(short = 'i', long)]
    pub ignore_case: bool,
    #[command(flatten)]
    pub pagination: PaginationArgs,
    /// Permit a capped exhaustive scan for patterns with no literal bytes.
    #[arg(long)]
    pub allow_scan: bool,
    /// Accept indexed-only results when the unindexed tail exceeds the
    /// scan budget.
    #[arg(long)]
    pub allow_stale: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemGetArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub remote_path: String,
    /// Where to write the download. Defaults to the remote file or directory
    /// name. Use `-` to stream one file to stdout. File downloads are streamed
    /// to a temporary path and moved into place after verification. Stdout
    /// can't be rolled back if verification fails after bytes are written.
    #[arg(value_hint = ValueHint::AnyPath)]
    pub local_destination: Option<String>,
    /// Download a directory and its contents. The destination is the root of
    /// the downloaded tree. For example, `loonfs get -r /reports ./download`
    /// writes the contents of `/reports` directly under `./download`. If the
    /// destination is omitted, the command uses `./reports`.
    #[arg(short, long)]
    pub recursive: bool,
    /// Download this revision instead of the current content.
    #[arg(long, value_parser = parse_revision_no)]
    pub revision: Option<RevisionNo>,
    /// Read from this snapshot instead of the current state.
    #[arg(long, value_hint = ValueHint::Other, conflicts_with = "revision")]
    pub snapshot_id: Option<PinId>,
    /// Overwrite existing local files.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemPutArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    /// Local file to upload, or `-` to read standard input. A large file
    /// and a pipe are both read once and never kept in memory whole, so a
    /// put's memory use doesn't grow with the size of the upload. Reading
    /// `-` requires an explicit remote path.
    #[arg(value_hint = ValueHint::AnyPath)]
    pub local_path: String,
    #[arg(value_hint = ValueHint::Other)]
    pub remote_path: Option<String>,
    /// Upload the directory tree rooted at `local_path`. Each file is its
    /// own commit, uploaded with bounded concurrency, so progress is
    /// reported per file, and a rerun after a partial failure works file by
    /// file.
    #[arg(short, long)]
    pub recursive: bool,
    /// Replace the remote destination if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Replacement requires that the path still points to this inode. This precondition implies --force.
    #[arg(long, value_parser = parse_public_inode_id, conflicts_with = "recursive")]
    pub expected_inode_id: Option<InodeId>,
    /// Replacement requires that the file still has this revision and inode. This precondition implies --force.
    #[arg(long, conflicts_with = "recursive", requires = "expected_inode_id", value_parser = parse_revision_no)]
    pub expected_revision: Option<RevisionNo>,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemAppendArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    /// Local file whose bytes to append, or `-` to read standard input. One
    /// append can add from 1 byte to 256 KiB.
    #[arg(value_hint = ValueHint::AnyPath)]
    pub local_path: String,
    /// The file to append to. It must already exist.
    #[arg(value_hint = ValueHint::Other)]
    pub remote_path: String,
    /// The append requires that the path still points to this inode.
    #[arg(long, value_parser = parse_public_inode_id)]
    pub expected_inode_id: Option<InodeId>,
    /// The append requires that the file still has this revision and inode.
    #[arg(long, requires = "expected_inode_id", value_parser = parse_revision_no)]
    pub expected_revision: Option<RevisionNo>,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemTransferArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub source_path: String,
    #[arg(value_hint = ValueHint::Other)]
    pub destination_path: String,
    /// Copy the directory tree rooted at `source_path`. Only cp accepts -r
    /// (mv moves a directory in one commit without it).
    #[arg(short, long)]
    pub recursive: bool,
    /// Replace the destination if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Replacement requires that the destination still points to this inode. This precondition implies --force.
    #[arg(long, value_parser = parse_public_inode_id, conflicts_with = "recursive")]
    pub expected_destination_inode_id: Option<InodeId>,
    /// Replacement requires that the destination still has this revision and inode. This precondition implies --force.
    #[arg(
        long,
        conflicts_with = "recursive",
        requires = "expected_destination_inode_id",
        value_parser = parse_revision_no
    )]
    pub expected_destination_revision: Option<RevisionNo>,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRestoreArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    #[arg(long, value_parser = parse_revision_no)]
    pub revision: RevisionNo,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemUndeleteArgs {
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    /// Destination path for the recovered file or directory. Omit it to
    /// restore the item in place, under the parent and name recorded with
    /// its deletion. That works even if the enclosing directories were
    /// renamed since.
    #[arg(value_hint = ValueHint::Other)]
    pub destination_path: Option<String>,
    /// Inode ID of the deleted item, as reported by `rm` and the change
    /// feed.
    #[arg(long, value_parser = parse_public_inode_id)]
    pub inode: InodeId,
    /// Committed sequence of the delete being recovered, as reported by
    /// `rm` and the change feed. Scopes recovery to that exact deletion,
    /// so a stale command can't cancel a later delete.
    #[arg(long, value_parser = parse_change_seq)]
    pub deletion_seq: ChangeSeq,
    #[command(flatten)]
    pub commit: CommitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct TrashArgs {
    #[command(flatten)]
    pub pagination: PaginationArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ChangesArgs {
    /// Return committed changes after this sequence (defaults to 0, the
    /// start of retained history).
    #[arg(long, value_parser = parse_change_seq)]
    pub after: Option<ChangeSeq>,
    #[command(flatten)]
    pub pagination: SeqPaginationArgs,
    /// End the change feed at this snapshot.
    #[arg(long, value_hint = ValueHint::Other)]
    pub snapshot_id: Option<PinId>,
}
