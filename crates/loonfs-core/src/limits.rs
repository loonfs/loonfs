//! One source of truth for publication, verification, provider, and
//! garbage-collection timing bounds.
//!
//! The GC grace window's safety proof (format spec, Appendix C) is an
//! inequality over these constants: every publication measures itself
//! against a budget here and refuses to publish its root once the
//! budget is spent, provider operations consume one deadline across retries,
//! and the minimum grace window is derived — not tuned — from those bounds
//! plus a margin for clock error and scheduling delay. Callers may configure
//! a larger grace window, never a smaller one.

use loonfs_objectstore::{PROVIDER_ATTEMPT_TIMEOUT, PROVIDER_OPERATION_DEADLINE};

/// Maximum semantic operations in one explicit commit, bounding how long one
/// request can occupy the serialized publisher during planning and
/// materialization.
pub const MAX_COMMIT_OPERATIONS: usize = 4096;

/// Bounds the admission work for one commit.
pub const MAX_COMMIT_ASSERTIONS: usize = 1024;

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

/// Bounds the WAL probes a cold open makes above the hinted number.
pub const HINT_RAISE_SEGMENTS: u64 = 8;

/// Visible WAL-tail length, in segments, at which maintenance publishes a
/// checkpoint.
pub const CHECKPOINT_AT_WAL_SEGMENTS: u64 = 32;

const _: () = assert!(
    0 < CHECKPOINT_AT_WAL_SEGMENTS && CHECKPOINT_AT_WAL_SEGMENTS < MAX_UNFLUSHED_WAL_SEGMENTS
);

/// Provider operation deadline, in milliseconds (`loonfs-objectstore`
/// consumes it across every retry of one single-request operation).
/// Multipart transfers of large immutable payloads carry no
/// whole-operation clock — their parts are individually time- and
/// retry-bounded — which leaves the floor derivation below untouched:
/// every object it times (WAL segments inside the publish budget,
/// checkpoint records, and numbered manifests) is a small control
/// object on the single-request path, and publications self-enforce their
/// budgets by local monotonic elapsed time regardless of provider deadlines.
pub const PROVIDER_OPERATION_DEADLINE_MS: u64 = PROVIDER_OPERATION_DEADLINE.as_millis() as u64;

/// One control-plane provider HTTP attempt's request timeout, in
/// milliseconds. An operation's total wall time is bounded by
/// `PROVIDER_OPERATION_DEADLINE_MS + PROVIDER_ATTEMPT_TIMEOUT_MS`, because the
/// deadline gates starting an attempt rather than preempting one.
pub const PROVIDER_ATTEMPT_TIMEOUT_MS: u64 = PROVIDER_ATTEMPT_TIMEOUT.as_millis() as u64;

/// Maximum elapsed time from observing a tip to initiating its next numbered put.
pub const WAL_PUBLISH_BUDGET_MS: u64 = 60_000;

/// Self-enforced budget between writing a checkpoint record and completing
/// its post-write basis verification. Overrunning it counts as verification
/// failure: the record may have raced the grace window, so it must not stand
/// as a root.
pub const CHECKPOINT_VERIFY_BUDGET_MS: u64 = 60_000;

/// Self-enforced budget for one metadata publication — WAL flush or
/// reorganization — measured from before the first segment object is written
/// until the manifest put-if-absent is initiated. A publication that exceeds
/// it aborts without publishing; its immutable outputs remain unreachable
/// garbage-collection candidates.
pub const METADATA_PUBLICATION_BUDGET_MS: u64 = 15 * 60 * 1000;

/// Bounds elapsed time from the collection call clock to retirement publication.
pub const RETIREMENT_PUBLICATION_BUDGET_MS: u64 = METADATA_PUBLICATION_BUDGET_MS;

/// Combined allowance for age overstatement from host-to-provider or
/// host-to-host clock error and scheduling delay around publication checks.
/// Direct expiry comparisons do not add this margin to stored deadlines.
pub const GC_SAFETY_MARGIN_MS: u64 = 3 * 60 * 1000;

const fn max_u64(left: u64, right: u64) -> u64 {
    if left > right {
        left
    } else {
        right
    }
}

/// Minimum age of an unreachable object before garbage collection or repair
/// may remove it. The value covers the longest publication budget, provider
/// operation time, and the combined clock-error and scheduling allowance.
pub const GC_MIN_GRACE_WINDOW_MS: u64 = max_u64(
    max_u64(WAL_PUBLISH_BUDGET_MS, CHECKPOINT_VERIFY_BUDGET_MS),
    METADATA_PUBLICATION_BUDGET_MS,
) + PROVIDER_OPERATION_DEADLINE_MS
    + PROVIDER_ATTEMPT_TIMEOUT_MS
    + GC_SAFETY_MARGIN_MS;

const _: () = assert!(
    RETIREMENT_PUBLICATION_BUDGET_MS
        <= max_u64(
            max_u64(WAL_PUBLISH_BUDGET_MS, CHECKPOINT_VERIFY_BUDGET_MS),
            METADATA_PUBLICATION_BUDGET_MS,
        )
);

/// Minimum provider age of a metadata segment no root manifest lists before
/// garbage collection may delete it. A streaming compaction writes its
/// output under `segments/` as it goes and publishes at the end, so its
/// earliest segment is unreferenced for the whole run. One day is the bound
/// on a run; the difference between this age and the grace window below is
/// how long a job may keep merging before it gives up.
pub const UNREFERENCED_SEGMENT_MIN_AGE_MS: u64 = 24 * 60 * 60 * 1000;

/// Elapsed monotonic time after which a streaming compaction abandons
/// instead of publishing. A job that starts its last publication attempt at
/// this bound finishes within one grace window, which the minimum age above
/// reserves after the budget. The subtraction fails to compile if the grace
/// window ever exceeds the minimum age.
pub const METADATA_COMPACTION_BUDGET_MS: u64 =
    UNREFERENCED_SEGMENT_MIN_AGE_MS - GC_MIN_GRACE_WINDOW_MS;

/// Lifetime of a direct download or upload capability.
pub const DIRECT_TRANSFER_URL_TTL_MS: u64 = 15 * 60 * 1000;

/// Covers outstanding reads and direct capabilities after retirement.
///
/// The deadline is measured from the collection call's clock, and the call
/// may spend up to the retirement publication budget before it publishes,
/// so the budget is added in front of the longest thing the grace covers.
pub const NAMESPACE_RETIREMENT_GRACE_MS: u64 = RETIREMENT_PUBLICATION_BUDGET_MS
    + max_u64(
        GC_MIN_GRACE_WINDOW_MS,
        DIRECT_TRANSFER_URL_TTL_MS
            + PROVIDER_OPERATION_DEADLINE_MS
            + PROVIDER_ATTEMPT_TIMEOUT_MS
            + GC_SAFETY_MARGIN_MS,
    );

const fn covers_namespace_retirement(grace_ms: u64) -> bool {
    grace_ms >= RETIREMENT_PUBLICATION_BUDGET_MS + GC_MIN_GRACE_WINDOW_MS
        && grace_ms
            >= RETIREMENT_PUBLICATION_BUDGET_MS
                + DIRECT_TRANSFER_URL_TTL_MS
                + PROVIDER_OPERATION_DEADLINE_MS
                + PROVIDER_ATTEMPT_TIMEOUT_MS
                + GC_SAFETY_MARGIN_MS
}

const _: () = assert!(covers_namespace_retirement(NAMESPACE_RETIREMENT_GRACE_MS));

/// Default age of an unreachable object before garbage collection may remove it.
pub const GC_DEFAULT_GRACE_WINDOW_MS: u64 = 60 * 60 * 1000;

const _: () = assert!(GC_DEFAULT_GRACE_WINDOW_MS >= GC_MIN_GRACE_WINDOW_MS);

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

/// Bounds installation before an absent target's pin can be collected.
pub const FORK_INSTALL_BUDGET_MS: u64 = GC_MIN_GRACE_WINDOW_MS
    - PROVIDER_OPERATION_DEADLINE_MS
    - PROVIDER_ATTEMPT_TIMEOUT_MS
    - GC_SAFETY_MARGIN_MS;

/// Lifetime resolved on the creating host; expiry checks add no clock-error margin.
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
/// final publication with the clock-error and scheduling allowance. Every mint
/// checks the upload's receipt issuance window. Immediately before the head
/// swap, content proofs are checked against the request clock plus the whole
/// attempt's elapsed monotonic time.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::GcConfig;

    #[test]
    fn retirement_grace_covers_capabilities_and_rejects_a_shorter_window() {
        assert!(covers_namespace_retirement(NAMESPACE_RETIREMENT_GRACE_MS));
        assert!(!covers_namespace_retirement(
            NAMESPACE_RETIREMENT_GRACE_MS - 1
        ));
    }

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
