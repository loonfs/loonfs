//! Commit admission, planning, validation, and publication.
//!
//! A batch of commits is published as one WAL object.
//! [`NamespaceCommitEngine`](crate::publish::NamespaceCommitEngine) is the
//! entry point. Path planning calls the steps in `validate` for each
//! operation as it goes, producing a commit plan containing WAL deltas. The
//! result is framed for publication.

pub(crate) mod batch;
pub(crate) mod candidates;
pub(crate) mod engine;
mod plan;
pub(crate) mod publish_view;
mod validate;
mod wal_payload;
pub(crate) mod wal_size;

pub(crate) use self::plan::{AppendedContent, CommitPlan, ResolvedBinding};
pub use self::validate::{CommitOperand, CommitValidationError};
pub(crate) use self::validate::{CommitValidationSession, NewChild};
pub(crate) use self::wal_payload::wal_payload_from_commit_plan;
pub use loonfs_types::CommitFingerprint;
