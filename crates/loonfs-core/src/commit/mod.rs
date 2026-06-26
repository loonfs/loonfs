mod api_adapter;
mod durable_adapter;
mod frame;
mod identity;
mod materialize;
mod metadata_preview;
mod operation;
mod plan;
mod precondition;
mod prepared;
mod publish;
mod publish_error;
mod request;
mod validate;
mod validate_error;

use crate::invariants::InvariantId;

pub use self::api_adapter::{commit_request_from_v0, CommitConversionError};
pub(crate) use self::durable_adapter::wal_payload_from_materialized_commit;
pub use self::identity::{
    core_commit_fingerprint, core_commit_fingerprint_for_v0_request, CommitFingerprintError,
    CoreCommitFingerprint, PathIntentFingerprint, SemanticMutationIdentity,
};
pub(crate) use self::identity::{fingerprint_digest, PATH_INTENT_FINGERPRINT_DOMAIN};
pub use self::materialize::{
    materialize_commit, CommitOpResult, MaterializedCommit, MaterializedCommitDelta,
};
pub(crate) use self::metadata_preview::PublishMetadataPreview;
pub use self::operation::CommitOp;
pub(crate) use self::plan::ValidatedOp;
pub use self::plan::{CommitPlan, CommitValidationContext};
pub use self::precondition::{Precondition, ResolvedBinding};
pub(crate) use self::prepared::CommitIdentitySource;
pub use self::prepared::{CommitExecutionContext, CommitPrepareError, PreparedCommit};
pub use self::publish::{prepare_commit_head_publish, publish_commit_head};
pub use self::publish_error::CommitHeadPublishError;
pub use self::request::CommitRequest;
pub use self::validate::build_commit_plan;
pub(crate) use self::validate::{
    build_commit_plan_for_publish, resolve_restore_content_refs_for_publish,
    PublishCommitValidationContext,
};
pub use self::validate_error::CommitValidationError;

pub(crate) fn push_unique_invariant(invariants: &mut Vec<InvariantId>, id: InvariantId) {
    if !invariants.contains(&id) {
        invariants.push(id);
    }
}
