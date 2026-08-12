//! Planned commits, from planner output to durable WAL frame.
//!
//! A planned commit is validated against a metadata view into a commit plan,
//! the plan is materialized into WAL deltas, and the result is framed for
//! publication. Submodules follow that pipeline; `identity` names the
//! fingerprint that makes reused commit ids safe to compare, and `ops` holds
//! the inode-level vocabulary path operations compile into.

mod durable_adapter;
mod frame;
mod identity;
mod inode_allocator;
mod ir;
mod materialize;
mod metadata_overlay;
mod ops;
mod plan;
mod publish;
mod publish_error;
mod validate;
mod validate_error;

pub(crate) use self::durable_adapter::wal_payload_from_materialized_commit;
pub use self::identity::CommitFingerprint;
pub(crate) use self::inode_allocator::{CandidateAllocation, InodeAllocator};
pub(crate) use self::ir::CommitIr;
pub use self::materialize::MaterializedCommitDelta;
pub(crate) use self::materialize::{materialize_commit, MaterializedCommit};
pub(crate) use self::ops::{CommitOp, CommitPrecondition, PlannedOp};
pub use self::plan::{CommitPlan, ResolvedBinding};
pub(crate) use self::plan::{ValidatedCommitPlan, ValidatedOp};
pub(crate) use self::publish::PreparedCommitHeadPublish;
pub use self::publish::{prepare_commit_head_publish, publish_commit_head};
pub use self::publish_error::CommitHeadPublishError;
pub(crate) use self::validate::{
    validate_commit_for_publish, validate_ops, OpValidationCursor, PublishCommitValidationContext,
    PublishValidationView,
};
pub use self::validate_error::CommitValidationError;
