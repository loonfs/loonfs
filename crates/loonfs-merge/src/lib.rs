//! The loop that merges a LoonFS fork into the namespace that it was forked
//! from.
//!
//! A merge can need more than one request. A merge that's too large for one
//! commit is merged in pieces, a merge that conflicts is rebased into a new
//! fork where a resolver writes resolutions, and the new fork then merges in
//! its place. A [`MergeSession`] runs those steps, one or two requests at a
//! time, through a [`Mechanics`] implementation (the HTTP [`Client`] in the
//! CLI and the lab) and a [`Resolver`]. After every step, the caller saves the
//! session's [`MergeSessionState`], so a run that crashed resumes where it
//! stopped.
//!
//! The server does the merging, the splitting, and the rebasing, so this
//! crate keeps only the session's progress. The design note
//! `docs/design/merge.md` describes the loop.
//!
//! [`Client`]: loonfs_client::Client

#![warn(missing_docs)]

mod error;
mod mechanics;
mod outcome;
mod recovery;
mod refusal;
mod resolver;
mod session;
mod state;

pub use error::{MergeError, Result};
pub use mechanics::Mechanics;
pub use outcome::MergeSessionOutcome;
pub use resolver::{DefaultResolver, Resolver};
pub use session::{MergeSession, MergeSessionOptions, MAX_NEW_FORKS};
pub use state::{
    ConflictReport, MergeFailure, MergeSessionState, MergeSessionStatus, PendingCommit,
};
