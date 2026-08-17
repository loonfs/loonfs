//! Planned commits, from planner output to durable WAL payload.
//!
//! Planning validates each compiled operation as it goes, producing a
//! validated commit plan; the plan is materialized into WAL deltas, and the
//! result is framed for publication. Submodules follow that pipeline;
//! `identity` names the fingerprint that makes reused commit ids safe to
//! compare, and `ops` holds the inode-level vocabulary path operations
//! compile into.

mod identity;
mod inode_allocator;
mod materialize;
mod metadata_overlay;
mod ops;
mod plan;
mod publish;
mod publish_error;
mod validate;
mod wal_payload;

pub use self::identity::CommitFingerprint;
pub(crate) use self::inode_allocator::{CandidateAllocation, InodeAllocator};
pub use self::materialize::MaterializedCommitDelta;
pub(crate) use self::materialize::{materialize_commit, MaterializedCommit};
pub(crate) use self::ops::{CommitOp, CommitPrecondition, PlannedOp};
pub use self::plan::{CommitPlan, ResolvedBinding};
pub(crate) use self::plan::{ValidatedCommitPlan, ValidatedOp};
pub(crate) use self::publish::{
    prepare_commit_head_publish, publish_commit_head, PreparedCommitHeadPublish,
};
pub use self::publish_error::CommitHeadPublishError;
pub use self::validate::CommitValidationError;
pub(crate) use self::validate::{validate_ops, PublishValidationView};
pub(crate) use self::wal_payload::wal_payload_from_materialized_commit;
