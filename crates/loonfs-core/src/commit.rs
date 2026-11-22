//! Commit admission, planning, validation, and publication.
//!
//! A batch of commits is published as one WAL object.
//! [`NamespaceCommitEngine`](crate::publish::NamespaceCommitEngine) is the
//! entry point. Path planning plans each operation of a commit and calls the
//! writes of the commit's [`CommitSession`] as it goes, producing a commit
//! plan containing WAL deltas. The result is framed for publication.

pub(crate) mod batch;
pub(crate) mod candidates;
pub(crate) mod engine;
mod error;
mod plan;
pub(crate) mod publish_view;
mod session;
mod wal_payload;
pub(crate) mod wal_size;

pub use self::error::{CommitOperand, CommitValidationError};
pub(crate) use self::plan::{AppendedContent, CommitPlan, ResolvedBinding};
pub(crate) use self::session::{CommitSession, NewChild};
pub(crate) use self::wal_payload::wal_payload_from_commit_plan;
pub use loonfs_types::CommitFingerprint;
