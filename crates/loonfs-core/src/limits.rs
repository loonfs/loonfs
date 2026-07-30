//! One source of truth for publication, verification, provider, and
//! garbage-collection timing bounds.
//!
//! The GC grace window's safety proof (format spec, "Garbage collection",
//! rule 1) is an inequality over these constants: every publication measures
//! itself against a budget here and refuses to publish its root once the
//! budget is spent, provider operations consume one deadline across retries,
//! and the minimum grace window is derived — not tuned — from those bounds
//! plus a margin for provider-timestamp skew. Callers may configure a larger
//! grace window, never a smaller one.

use loonfs_objectstore::{PROVIDER_ATTEMPT_TIMEOUT, PROVIDER_OPERATION_DEADLINE};

/// Maximum semantic operations in one explicit commit, bounding how long one
/// request can occupy the serialized publisher during planning and
/// materialization.
pub const MAX_COMMIT_OPERATIONS: usize = 4096;

/// Maximum content-token or prepared-proof entries carried by one explicit
/// commit, bounding preparation work for a new primary. An oversized
/// candidate may occupy a publisher queue slot until candidate preparation
/// rejects it.
pub const MAX_COMMIT_CONTENT_TOKENS: usize = 4096;

/// Maximum distinct new external content refs in one explicit commit, bounding
/// its in-memory coverage work while it occupies the serialized publisher.
pub const MAX_COMMIT_EXTERNAL_CONTENT_REFS: usize = 4096;

/// Maximum byte length of a commit's optional `message` annotation, which is
/// stored in every durable WAL record, hashed into the mutation fingerprint,
/// and replayed by the change feed. This is the only bound on the message:
/// no transport-level body limit is relied on.
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 4096;

/// Maximum attempts for a bounded compare-and-swap or allocation contention loop.
pub const CONTENTION_RETRY_LIMIT: usize = 8;

/// Provider operation deadline, in milliseconds (`loonfs-objectstore`
/// consumes it across every retry of one single-request operation).
/// Multipart transfers of large immutable payloads carry no
/// whole-operation clock — their parts are individually time- and
/// retry-bounded — which leaves the floor derivation below untouched:
/// every object it times (WAL segments inside the publish budget,
/// checkpoint records, the root compare-and-swap) is a small control
/// object on the single-request path, and publications self-enforce their
/// budgets by wall clock regardless of per-operation deadlines.
pub const PROVIDER_OPERATION_DEADLINE_MS: u64 = PROVIDER_OPERATION_DEADLINE.as_millis() as u64;

/// One control-plane provider HTTP attempt's request timeout, in
/// milliseconds. An operation's total wall time is bounded by
/// `PROVIDER_OPERATION_DEADLINE_MS + PROVIDER_ATTEMPT_TIMEOUT_MS`, because the
/// deadline gates starting an attempt rather than preempting one.
pub const PROVIDER_ATTEMPT_TIMEOUT_MS: u64 = PROVIDER_ATTEMPT_TIMEOUT.as_millis() as u64;

/// Self-enforced budget between starting a WAL segment PUT and initiating
/// the head compare-and-swap. Overrunning it abandons the segment instead of
/// publishing a stale-timed one. Local monotonic elapsed time only — never a
/// validity input (format spec, "WAL head").
pub const WAL_PUBLISH_BUDGET_MS: u64 = 60_000;

/// Self-enforced budget between writing a checkpoint record and completing
/// its post-write basis verification. Overrunning it counts as verification
/// failure: the record may have raced the grace window, so it must not stand
/// as a root.
pub const CHECKPOINT_VERIFY_BUDGET_MS: u64 = 60_000;

/// Self-enforced budget for one metadata publication — WAL flush or
/// reorganization — measured from before the first table object is written
/// until the root compare-and-swap is initiated. A publication that exceeds
/// it aborts without publishing; its immutable outputs remain unreachable
/// garbage-collection candidates.
pub const METADATA_PUBLICATION_BUDGET_MS: u64 = 15 * 60 * 1000;

/// Margin absorbing provider-timestamp skew against the GC caller's clock,
/// plus scheduling slop around the budget checks.
pub const GC_SAFETY_MARGIN_MS: u64 = 3 * 60 * 1000;

/// Default candidate budget for one step-driven garbage-collection pass.
pub const DEFAULT_GC_MAX_OBJECTS: u64 = 1024;

const fn max_u64(left: u64, right: u64) -> u64 {
    if left > right {
        left
    } else {
        right
    }
}

/// The derived minimum GC grace window and explicit namespace-repair safety
/// window (format spec, "Garbage collection", rule 1). Every acknowledged
/// publication starts its final compare-and-swap within a publication budget
/// measured from its first object write, and that compare-and-swap completes
/// within one provider operation bound. So an object older than this window
/// that is still unreachable at delete time cannot belong to a publication
/// that might yet succeed. `GcConfig::validate` rejects smaller windows, and
/// namespace repair uses the same bound before reaping non-completable install
/// debris.
pub const GC_MIN_GRACE_WINDOW_MS: u64 = max_u64(
    max_u64(WAL_PUBLISH_BUDGET_MS, CHECKPOINT_VERIFY_BUDGET_MS),
    METADATA_PUBLICATION_BUDGET_MS,
) + PROVIDER_OPERATION_DEADLINE_MS
    + PROVIDER_ATTEMPT_TIMEOUT_MS
    + GC_SAFETY_MARGIN_MS;

/// Lease every fork attempt takes on the fork-owned source checkpoint it
/// creates. An attempt that never installs its target head lets the lease
/// pass, and garbage collection releases the record on that alone — no
/// provider timestamp, no fork-specific age rule.
///
/// Two grace floors, because a fork attempt is two of the things that floor
/// already bounds. The first covers creating the record: the WAL flush and
/// manifest publication it may perform, the post-write basis verification,
/// and the provider bounds and clock skew around them — which is exactly
/// what `GC_MIN_GRACE_WINDOW_MS` is the bound for. The second covers
/// everything after: reading the pinned manifest and the source head,
/// installing the target head, and the guard read below. Each half is one
/// publication plus provider bounds plus skew, so each is one floor.
pub const FORK_CHECKPOINT_LEASE_MS: u64 = 2 * GC_MIN_GRACE_WINDOW_MS;

/// Margin the post-publish fork guard requires between now and the source
/// record's lease expiry before it lets a target stand.
///
/// The guard's evidence is one record read, and one provider operation's
/// total wall time is `PROVIDER_OPERATION_DEADLINE_MS +
/// PROVIDER_ATTEMPT_TIMEOUT_MS` (the deadline gates starting an attempt
/// rather than preempting one). A record observed with more lease than that
/// left cannot have been legally expiry-released while the guard was
/// looking at it, so the observation still holds when the guard acts on it.
pub const FORK_GUARD_MARGIN_MS: u64 = PROVIDER_OPERATION_DEADLINE_MS + PROVIDER_ATTEMPT_TIMEOUT_MS;

// The fork lease has two jobs, and both are inequalities over the constants
// above rather than judgement calls, so they are checked where a broken
// derivation is a compile error instead of a test failure: it must cover a
// whole fork attempt, and it must leave the guard something to check.
const _: () = assert!(
    FORK_CHECKPOINT_LEASE_MS >= GC_MIN_GRACE_WINDOW_MS,
    "a fork attempt may take as long as any other publication"
);
const _: () = assert!(
    FORK_GUARD_MARGIN_MS < FORK_CHECKPOINT_LEASE_MS,
    "a fork that finishes promptly must still clear the guard margin"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::GcConfig;

    #[test]
    fn derived_minimum_grace_window_sits_below_the_default() {
        // 15 min publication + 2 min provider deadline + 30 s attempt
        // timeout + 3 min margin = 20.5 minutes.
        assert_eq!(GC_MIN_GRACE_WINDOW_MS, 1_230_000);
        assert!(
            GC_MIN_GRACE_WINDOW_MS < GcConfig::default().grace_window_ms,
            "the conservative default grace window must satisfy its own floor"
        );
        assert!(
            GC_MIN_GRACE_WINDOW_MS
                > max_u64(
                    max_u64(WAL_PUBLISH_BUDGET_MS, CHECKPOINT_VERIFY_BUDGET_MS),
                    METADATA_PUBLICATION_BUDGET_MS,
                ) + PROVIDER_OPERATION_DEADLINE_MS,
            "the floor keeps a margin above budget plus provider deadline"
        );
    }
}
