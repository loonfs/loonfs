//! Converts planned commits into durable WAL payloads.
//!
//! Planning validates each compiled operation as it goes, producing a
//! commit plan containing WAL deltas. The result is framed for publication.
//! `ops` defines the inode-level operations produced by path planning.

mod inode_allocator;
mod ops;
mod plan;
mod publish_error;
mod validate;
mod wal_payload;

pub(crate) use self::inode_allocator::{next_inode_after, CandidateAllocation, InodeAllocator};
pub(crate) use self::ops::CommitOp;
pub(crate) use self::plan::{CommitPlan, PreparedCommit, ResolvedBinding, ValidatedCommitPlan};
pub use self::publish_error::WalPublishError;
pub(crate) use self::validate::{validate_ops, CommitNumbering, PublishValidationView};
pub use self::validate::{CommitOperand, CommitValidationError};
pub(crate) use self::wal_payload::wal_payload_from_prepared_commit;
pub use loonfs_api::CommitFingerprint;
