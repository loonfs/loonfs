//! Planned commits, from planner output to durable WAL frame.
//!
//! A planned commit is validated against a metadata view into a commit plan,
//! the plan is materialized into WAL deltas, and the result is framed for
//! publication. Submodules follow that pipeline; `identity` defines the
//! fingerprint that makes reused commit ids safe to compare, and `ops` holds
//! the inode-level vocabulary path operations compile into.

mod durable_adapter;
mod frame;
mod identity;
mod materialize;
mod metadata_overlay;
mod ops;
mod plan;
mod prepared;
mod publish;
mod publish_error;
mod request;
mod validate;
mod validate_error;

pub(crate) use self::durable_adapter::wal_payload_from_materialized_commit;
pub use self::identity::MutationFingerprint;
pub(crate) use self::identity::{fingerprint_digest, MUTATION_FINGERPRINT_DOMAIN};
pub(crate) use self::materialize::{materialize_commit, MaterializedCommit};
pub use self::materialize::{CommitOpResult, MaterializedCommitDelta};
pub(crate) use self::ops::{CommitOp, CommitPrecondition, PlannedOp};
#[cfg(test)]
pub(crate) use self::plan::CommitValidationContext;
pub(crate) use self::plan::ValidatedOp;
pub use self::plan::{CommitPlan, ResolvedBinding};
pub use self::prepared::CommitPrepareError;
pub(crate) use self::prepared::PreparedCommit;
pub(crate) use self::publish::PreparedCommitHeadPublish;
pub use self::publish::{prepare_commit_head_publish, publish_commit_head};
pub use self::publish_error::CommitHeadPublishError;
pub(crate) use self::request::CommitRequest;
pub(crate) use self::validate::{
    allocates_inode, build_commit_plan_for_publish, validate_ops, OpValidationCursor,
    PublishCommitValidationContext, PublishValidationView,
};
pub use self::validate_error::CommitValidationError;
