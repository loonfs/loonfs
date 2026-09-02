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

/// Longest visible WAL tail, in segments, a namespace may carry unflushed.
/// Every publish surface rejects at this length with `maintenance_required`,
/// so a landed publication never leaves more than this behind.
pub const MAX_UNFLUSHED_WAL_SEGMENTS: u64 = 128;

/// Segment pointers the head carries as a replay accelerator, newest first
/// and always including the tip. Sized so the head describes the whole
/// legal unflushed tail rather than to keep the head small: a full list
/// encodes to roughly 68 KiB at realistic identifier lengths and 107 KiB at
/// the worst case the grammars allow.
pub(crate) use loonfs_api::wire::control::RECENT_SEGMENTS_LIMIT;

/// The head-coverage inequality, shared by the compile-time assertion below
/// and the test that proves the assertion has teeth.
const fn covers_every_unflushed_segment(pointers: usize) -> bool {
    pointers >= MAX_UNFLUSHED_WAL_SEGMENTS as usize
}

// A reader that wants a segment the head does not name walks predecessor
// links to reach it, one round trip per segment, so an accelerator shorter
// than the tail the rejection bound admits is a latency cliff the write
// path can legally produce. The two constants have to move together, and
// that is an inequality rather than a judgement call, so it is checked
// where a broken derivation is a compile error instead of a test failure.
const _: () = assert!(
    covers_every_unflushed_segment(RECENT_SEGMENTS_LIMIT),
    "the head must describe every legal unflushed WAL segment"
);

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
/// reorganization — measured from before the first segment object is written
/// until the root compare-and-swap is initiated. A publication that exceeds
/// it aborts without publishing; its immutable outputs remain unreachable
/// garbage-collection candidates.
pub const METADATA_PUBLICATION_BUDGET_MS: u64 = 15 * 60 * 1000;

/// Margin absorbing provider-timestamp skew against the GC caller's clock,
/// plus scheduling slop around the budget checks.
pub const GC_SAFETY_MARGIN_MS: u64 = 3 * 60 * 1000;

/// Default candidate budget for one step-driven garbage-collection pass.
pub const DEFAULT_GC_MAX_OBJECTS: u64 = 1024;

/// Interval between streaming-compaction lease heartbeats.
///
/// The interval must exceed one provider operation bound so a healthy job can
/// refresh its lease before the next heartbeat is due.
pub const METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS: u64 = 5 * 60 * 1000;

/// Heartbeats a lease may miss before its job is considered inactive.
pub const METADATA_COMPACTION_LEASE_MISSED_HEARTBEATS: u64 = 5;

/// Time after the last heartbeat before a compaction lease expires.
///
/// This must outlast one publication so garbage collection cannot claim a
/// job's prefix while its final compare-and-swap is in progress.
pub const METADATA_COMPACTION_LEASE_EXPIRY_MS: u64 =
    METADATA_COMPACTION_LEASE_MISSED_HEARTBEATS * METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS;

/// Minimum age of staged compaction output before it may be collected after
/// its lease expires.
pub const METADATA_COMPACTION_STAGING_GRACE_MS: u64 =
    METADATA_COMPACTION_LEASE_EXPIRY_MS + GC_MIN_GRACE_WINDOW_MS;

/// Returns whether a heartbeat can complete before the next is due.
const fn heartbeats_land_before_the_next_one_is_due(interval_ms: u64) -> bool {
    interval_ms > PROVIDER_OPERATION_DEADLINE_MS + PROVIDER_ATTEMPT_TIMEOUT_MS
}

// Keep the heartbeat interval above the maximum provider operation time.
const _: () = assert!(
    heartbeats_land_before_the_next_one_is_due(METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS),
    "a heartbeat that spends its whole provider budget must still land before the next is due"
);

/// Returns whether a lease can cover one metadata publication.
const fn outlasts_one_publication(expiry_ms: u64) -> bool {
    expiry_ms >= GC_MIN_GRACE_WINDOW_MS
}

// The lease must remain valid through a final publication attempt.
const _: () = assert!(
    outlasts_one_publication(METADATA_COMPACTION_LEASE_EXPIRY_MS),
    "a compaction lease must outlast the publication that ends the job holding it"
);

const fn max_u64(left: u64, right: u64) -> u64 {
    if left > right {
        left
    } else {
        right
    }
}

/// Minimum age of an unreachable object before garbage collection or repair
/// may remove it. The value covers the longest publication budget, provider
/// operation time, and clock skew.
pub const GC_MIN_GRACE_WINDOW_MS: u64 = max_u64(
    max_u64(WAL_PUBLISH_BUDGET_MS, CHECKPOINT_VERIFY_BUDGET_MS),
    METADATA_PUBLICATION_BUDGET_MS,
) + PROVIDER_OPERATION_DEADLINE_MS
    + PROVIDER_ATTEMPT_TIMEOUT_MS
    + GC_SAFETY_MARGIN_MS;

/// Most parts one direct multipart upload may cut into. This is the
/// S3-compatible ceiling, so with the session's part size it fixes the
/// largest object that session can carry: `part_size_bytes × 10_000`.
pub const MAX_MULTIPART_PARTS: u32 = 10_000;

/// Smallest part size a `direct_multipart` session may be opened with.
/// Every supported provider refuses a non-final part below 5 MiB.
pub const MIN_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;

/// Largest part size a `direct_multipart` session may be opened with.
/// Every supported provider refuses a part above 5 GiB.
pub const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Most part-upload capabilities one request may ask for. A client asks in
/// waves as it works through a file, so this bounds one response rather than
/// one upload.
pub const MAX_SIGNED_PARTS_PER_REQUEST: usize = 1_000;

/// Lease for the source checkpoint created by a fork attempt.
///
/// Two GC grace windows cover checkpoint creation and target installation.
pub const FORK_CHECKPOINT_LEASE_MS: u64 = 2 * GC_MIN_GRACE_WINDOW_MS;

/// Time reserved for the target-head write after renewing a fork checkpoint.
pub const FORK_INSTALL_MARGIN_MS: u64 =
    PROVIDER_OPERATION_DEADLINE_MS + PROVIDER_ATTEMPT_TIMEOUT_MS;

/// Lease duration for an upload session.
pub const UPLOAD_SESSION_LEASE_MS: u64 = 24 * 60 * 60 * 1000;

/// Time during which a content receipt may be used in a commit.
pub const CONTENT_RECEIPT_TTL_MS: u64 = 60 * 60 * 1000;

/// Time during which a completed upload may issue new content receipts.
pub const COMPLETED_UPLOAD_RECEIPT_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Latest admission horizon for proof derived from one completed upload.
/// This covers its receipt window and the lifetime of the final token.
pub const COMPLETED_UPLOAD_ADMISSION_WINDOW_MS: u64 =
    COMPLETED_UPLOAD_RECEIPT_WINDOW_MS + CONTENT_RECEIPT_TTL_MS;

/// Minimum age of unreferenced content from a completed upload before
/// collection. The value covers the receipt window, receipt lifetime, and a
/// final publication.
pub const CONTENT_RECLAMATION_GRACE_MS: u64 =
    COMPLETED_UPLOAD_ADMISSION_WINDOW_MS + GC_MIN_GRACE_WINDOW_MS;

/// The grace floor's inequality, shared by the compile-time assertion below
/// and the test that proves the assertion has teeth.
const fn outlasts_every_receipt(grace_ms: u64) -> bool {
    grace_ms >= COMPLETED_UPLOAD_ADMISSION_WINDOW_MS + GC_MIN_GRACE_WINDOW_MS
}

// Content reclamation is the one sweep that deletes bytes a user handed us,
// and its safety is an inequality over the constants above rather than a
// judgement call, so it is checked where a broken derivation is a compile
// error instead of a test failure.
const _: () = assert!(
    outlasts_every_receipt(CONTENT_RECLAMATION_GRACE_MS),
    "content reclamation must outlast the last receipt a completed session can mint, \
     the commit that receipt admits, and that commit's publication"
);

// The fork lease must cover a whole fork attempt, which is an inequality over
// the constants above rather than a judgement call, so it is checked where a
// broken derivation is a compile error instead of a test failure.
const _: () = assert!(
    FORK_CHECKPOINT_LEASE_MS >= GC_MIN_GRACE_WINDOW_MS,
    "a fork attempt may take as long as any other publication"
);
const _: () = assert!(
    FORK_INSTALL_MARGIN_MS < FORK_CHECKPOINT_LEASE_MS,
    "a renewed fork checkpoint must outlast target installation"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::GcConfig;

    #[test]
    fn the_content_grace_floor_rejects_a_window_one_receipt_short() {
        assert!(outlasts_every_receipt(CONTENT_RECLAMATION_GRACE_MS));
        assert!(!outlasts_every_receipt(CONTENT_RECLAMATION_GRACE_MS - 1));
        assert_eq!(
            COMPLETED_UPLOAD_ADMISSION_WINDOW_MS,
            COMPLETED_UPLOAD_RECEIPT_WINDOW_MS + CONTENT_RECEIPT_TTL_MS
        );
        assert!(!outlasts_every_receipt(
            COMPLETED_UPLOAD_ADMISSION_WINDOW_MS
        ));
        // 7 days of re-minting + 1 hour of receipt life + 20.5 minutes of
        // publication.
        assert_eq!(CONTENT_RECLAMATION_GRACE_MS, 608_400_000 + 1_230_000);
    }

    #[test]
    fn the_head_coverage_floor_rejects_a_list_one_segment_short() {
        assert!(covers_every_unflushed_segment(RECENT_SEGMENTS_LIMIT));
        assert!(covers_every_unflushed_segment(
            MAX_UNFLUSHED_WAL_SEGMENTS as usize
        ));
        assert!(!covers_every_unflushed_segment(
            MAX_UNFLUSHED_WAL_SEGMENTS as usize - 1
        ));
    }

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

    #[test]
    fn the_compaction_staging_grace_is_the_lease_plus_one_publication() {
        // 25 minutes of lease + 20.5 minutes of publication.
        assert_eq!(METADATA_COMPACTION_LEASE_EXPIRY_MS, 25 * 60 * 1000);
        assert_eq!(METADATA_COMPACTION_STAGING_GRACE_MS, 1_500_000 + 1_230_000);
        assert_eq!(
            METADATA_COMPACTION_STAGING_GRACE_MS,
            METADATA_COMPACTION_LEASE_EXPIRY_MS + GC_MIN_GRACE_WINDOW_MS
        );
        // Same bargain as every other derived floor here: the compile-time
        // assertion is only worth having if its predicate can fail.
        assert!(outlasts_one_publication(
            METADATA_COMPACTION_LEASE_EXPIRY_MS
        ));
        assert!(!outlasts_one_publication(GC_MIN_GRACE_WINDOW_MS - 1));
    }

    #[test]
    fn the_heartbeat_interval_floor_rejects_an_interval_one_provider_bound_short() {
        assert!(heartbeats_land_before_the_next_one_is_due(
            METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS
        ));
        assert!(!heartbeats_land_before_the_next_one_is_due(
            PROVIDER_OPERATION_DEADLINE_MS + PROVIDER_ATTEMPT_TIMEOUT_MS
        ));
        assert_eq!(
            METADATA_COMPACTION_LEASE_EXPIRY_MS,
            METADATA_COMPACTION_LEASE_MISSED_HEARTBEATS * METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS
        );
    }
}
