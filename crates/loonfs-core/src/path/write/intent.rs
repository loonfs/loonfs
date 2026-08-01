//! [`CommitRequest`]: the one filesystem commit language, before planning.

use loonfs_api::CommitId;

/// The operation language a commit is written in, owned by `loonfs-api` and
/// used here unchanged.
///
/// There is one spelling of a pre-planning operation across the workspace:
/// what a client sends is what the planner matches on, so no surface
/// translates between two versions of the same language. The durable
/// fingerprint preimage is a separate, frozen spelling, and the planner's
/// `operation_fingerprint_input` is the only place this one is renamed
/// into it.
pub use loonfs_api::FilesystemOperation;

/// One logical filesystem commit: an idempotency key, an optional caller
/// annotation, and an ordered, non-empty list of operations.
///
/// The whole request is one commit. Operations resolve in order, each seeing
/// the effects of the ones before it, and either all of them commit or none
/// of them do. A convenience one-operation request is this same type with a
/// single-element list, so it plans, fingerprints, and replays identically.
///
/// This is the wire [`CommitRequest`](loonfs_api::CommitRequest) minus the
/// content tokens: proofs are checked at the surface that accepts them, and
/// what reaches the planner is the commit itself.
///
/// Paths are parsed once at the surface that accepts the raw string
/// ([`parse_mutation_path`]); a request always carries validated, normalized
/// paths.
///
/// [`parse_mutation_path`]: crate::path::parse_mutation_path
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    /// Client idempotency key for the whole request.
    pub commit_id: CommitId,
    /// Caller annotation recorded on the commit. Part of the request's
    /// identity: reusing a commit id with a different message conflicts.
    pub message: Option<String>,
    /// Ordered operations. Must be non-empty.
    pub operations: Vec<FilesystemOperation>,
}

impl CommitRequest {
    /// A request carrying exactly one operation.
    pub fn single(
        commit_id: CommitId,
        message: Option<String>,
        operation: FilesystemOperation,
    ) -> Self {
        Self {
            commit_id,
            message,
            operations: vec![operation],
        }
    }
}
