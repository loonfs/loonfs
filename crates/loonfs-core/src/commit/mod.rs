//! Converts planned commits into durable WAL payloads.
//!
//! Planning validates each compiled operation as it goes, producing a
//! commit plan containing WAL deltas. The result is framed for publication.
//! `ops` defines the inode-level operations produced by path planning.

mod inode_allocator;
mod ops;
mod plan;
mod validate;
mod wal_payload;

pub(crate) use self::inode_allocator::{next_inode_after, CandidateAllocation};
pub(crate) use self::ops::{CommitOperation, NewChild};
pub(crate) use self::plan::{AppendedContent, CommitPlan, ResolvedBinding};
pub(crate) use self::validate::CommitValidationSession;
pub use self::validate::{CommitOperand, CommitValidationError};
pub(crate) use self::wal_payload::wal_payload_from_commit_plan;
pub use loonfs_types::CommitFingerprint;
