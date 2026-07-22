//! Explicit semantic commits, from request to durable WAL frame.
//!
//! A commit request is validated against a metadata view into a commit
//! plan, the plan is materialized into WAL deltas, and the result is framed
//! for publication. Submodules follow that pipeline; `identity` defines
//! the fingerprints that make reused commit ids safe to compare.

mod durable_adapter;
mod frame;
mod identity;
mod materialize;
mod metadata_overlay;
mod plan;
mod prepared;
mod publish;
mod publish_error;
mod request;
mod validate;
mod validate_error;

use crate::invariants::InvariantId;

pub(crate) use self::durable_adapter::wal_payload_from_materialized_commit;
pub use self::identity::{
    core_commit_fingerprint, core_commit_fingerprint_for_v0_request, CommitFingerprintError,
    CoreCommitFingerprint, PathIntentFingerprint, SemanticMutationIdentity,
};
pub(crate) use self::identity::{fingerprint_digest, PATH_INTENT_FINGERPRINT_DOMAIN};
pub use self::materialize::{
    materialize_commit, CommitOpResult, MaterializedCommit, MaterializedCommitDelta,
};
pub(crate) use self::plan::ValidatedOp;
pub use self::plan::{CommitPlan, CommitValidationContext, ResolvedBinding};
pub(crate) use self::prepared::CommitIdentitySource;
pub use self::prepared::{CommitExecutionContext, CommitPrepareError, PreparedCommit};
pub(crate) use self::publish::PreparedCommitHeadPublish;
pub use self::publish::{prepare_commit_head_publish, publish_commit_head};
pub use self::publish_error::CommitHeadPublishError;
pub use self::request::CommitRequest;
pub use self::validate::build_commit_plan;
pub(crate) use self::validate::{build_commit_plan_for_publish, PublishCommitValidationContext};
pub use self::validate_error::CommitValidationError;

pub(crate) fn push_unique_invariant(invariants: &mut Vec<InvariantId>, id: InvariantId) {
    if !invariants.contains(&id) {
        invariants.push(id);
    }
}
