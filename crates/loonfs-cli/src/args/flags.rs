//! Flag groups that several commands share: the actor, pagination, and
//! commit flags.

use clap::{Args, ValueHint};
use loonfs_types::{ActorId, CommitId};

#[derive(Debug, Args, Clone)]
pub(crate) struct ActorSelectorArgs {
    /// Actor ID for this mutation.
    /// Overrides the environment and profile. Defaults to loonfs-cli.
    #[arg(long)]
    pub actor_id: Option<ActorId>,
}

#[derive(Debug, Args)]
pub(crate) struct PaginationArgs {
    /// Resume from a cursor returned by a previous listing.
    #[arg(long, value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
    #[command(flatten)]
    pub page_limits: PageLimitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct SeqPaginationArgs {
    #[command(flatten)]
    pub page_limits: PageLimitArgs,
}

#[derive(Debug, Args)]
pub(crate) struct PageLimitArgs {
    /// Return at most this many items.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Request this many items per page.
    #[arg(long)]
    pub page_size: Option<u32>,
    /// Continue until the limit is reached or no pages remain.
    #[arg(long)]
    pub all: bool,
    /// Write one JSON item per line until the limit is reached or no pages remain.
    #[arg(long)]
    pub jsonl: bool,
}

#[derive(Debug, Args)]
pub(crate) struct CommitArgs {
    /// Annotation recorded on the commit and shown by `loonfs changes`.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit. One is generated when omitted.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<CommitId>,
}
