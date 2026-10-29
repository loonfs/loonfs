//! One source of truth for publication, provider, and garbage-collection
//! timing bounds.
//!
//! The GC grace window's safety proof (format spec, Appendix C) is an
//! inequality over these constants: every publication measures itself
//! against a budget here and refuses to publish its manifest once the
//! budget is spent, provider operations consume one deadline across retries,
//! and the minimum grace window is derived — not tuned — from those bounds
//! plus a margin for clock error and scheduling delay. Callers may configure
//! a larger grace window, never a smaller one.

use loonfs_objectstore::{
    PROVIDER_ATTEMPT_TIMEOUT, PROVIDER_MAX_MULTIPART_PARTS, PROVIDER_MAX_MULTIPART_PART_BYTES,
    PROVIDER_MIN_MULTIPART_PART_BYTES, PROVIDER_OPERATION_DEADLINE,
    PROVIDER_PUBLICATION_REQUEST_BOUND,
};

/// Maximum run fan-in for either compaction execution path. Larger groups are
/// merged in several publications, keeping decoded input blocks independent of
/// history size.
pub(crate) const MAX_COMPACTION_INPUT_RUNS: usize = 8;

/// Caps each Bloom filter used by compaction and content collection.
pub const MAX_BLOOM_FILTER_BYTES: usize = 64 * 1024 * 1024;

/// Most levels of the walk that builds a base compaction's kept inode filter.
///
/// The walk's stack holds at most this many directories, counting the one
/// the walk starts at, and the walk follows at most this many ancestors up
/// from a recoverable deletion. Each level of the stack keeps a directory's
/// inode ID and the row key where the walk resumes that directory, which is
/// at most about 1.6 KiB for a name key of `MAX_NAME_KEY_BYTES`, so the stack
/// takes at most about 7 MiB. A move can nest a tree deeper than any request
/// path, and a compaction that meets a deeper tree keeps every row it would
/// have tested.
pub const MAX_KEPT_INODE_WALK_DEPTH: usize = 4_096;

/// Most encoded segment bytes that one fold or compaction keeps in memory
/// while the segments' puts run.
///
/// The `STORE_WRITES_IN_FLIGHT` count alone allows 128 MiB of 8 MiB segments,
/// and 32 MiB is about four such segments and still admits a fold's usual
/// seven small segments at once.
pub const MAX_PENDING_SEGMENT_BYTES: usize = 32 * 1024 * 1024;

/// Most stored bytes of metadata segment blocks that the SSD tier of the
/// block cache (in the `loonfs` crate) keeps in memory while they wait for
/// its writer.
///
/// An insert copies a block's stored bytes into the queue and returns, so a
/// read doesn't wait for a disk write, and an insert that finds the queue
/// full writes nothing to disk and records a rejection. One ranged GET of a
/// metadata read fetches at most 4 MiB of blocks, so 64 MiB keeps the blocks
/// of 16 such GETs while the writer catches up, and a local SSD writes
/// 64 MiB in well under a second. The server's sizing formula includes this
/// memory in `fixed`.
pub const SSD_WRITE_QUEUE_BYTES: usize = 64 * 1024 * 1024;

/// Stored bytes of metadata segment blocks that a read fetches ahead when it
/// misses a data block, counting the missed block.
///
/// A read that misses a data block fetches that block and the blocks after
/// it in the segment, as long as their stored bytes stay within this size,
/// in one ranged GET. The tail latency of a GET from an object store is
/// nearly flat from tens of kilobytes to about 1 MiB, and the store charges
/// the same for each GET, so a window of 1 MiB costs about as much as a GET
/// of the missed block alone, and each later block of the window that the
/// read uses needs no GET of its own. For example, with blocks stored in
/// about 4 KiB each, a window has about 256 blocks. A runtime can set
/// another size (`MetadataCacheBuilder::read_ahead_window_bytes` in the
/// `loonfs` crate), and a window never reads more than the 4 MiB that one
/// ranged GET of blocks reads.
pub const READAHEAD_WINDOW_BYTES: usize = 1024 * 1024;

/// Maximum semantic operations in one explicit commit, bounding how long one
/// request can occupy the serialized publisher during planning and
/// materialization.
pub const MAX_COMMIT_OPERATIONS: usize = 4096;

/// Most deletions that one of the writer's expiry commits expires, and so
/// the most `listed` rows that one scan of the writer's expiry returns.
///
/// The writer admits no other commit between such a scan and the commit that
/// expires what it found, and validating each expire event reads its
/// deletion's tombstone, so this bounds how long one step of the expiry
/// delays the namespace's other writes. A thousand expire deltas are well
/// under a megabyte of WAL record. A fork's first writer session expires
/// every inherited deletion before it admits a request, so that request
/// waits for one commit for each thousand inherited deletions.
pub const MAX_EXPIRED_DELETIONS_PER_COMMIT: usize = 1_000;

const _: () = assert!(MAX_EXPIRED_DELETIONS_PER_COMMIT <= MAX_COMMIT_OPERATIONS);

/// Most successful publications of its own that a writer session waits for
/// before it tries an expiry step again after consecutive failed steps. The
/// wait doubles from one publication with each failure.
///
/// A step that keeps failing, for example against a store that refuses the
/// put, then costs at most one failed WAL put for every 64 of the session's
/// publications, while a step that failed once is tried again after the
/// session's next publication.
pub const MAX_EXPIRY_RETRY_PUBLICATIONS: usize = 64;

/// Four MiB of event heap keeps ordinary change pages small while allowing several large commits per page.
pub const CHANGE_FEED_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Bounds the admission work for one commit.
pub const MAX_COMMIT_PRECONDITIONS: usize = 1024;

/// Maximum content-token or prepared-proof entries carried by one explicit
/// commit, bounding preparation work for a new primary.
///
/// An oversized candidate may occupy a publisher queue slot until candidate
/// preparation rejects it.
pub const MAX_COMMIT_CONTENT_TOKENS: usize = 4096;

/// Maximum distinct new external content refs in one explicit commit, bounding
/// its in-memory coverage work while it occupies the serialized publisher.
pub const MAX_COMMIT_EXTERNAL_CONTENT_REFS: usize = 4096;

/// Maximum byte length of a commit's optional `message` annotation, which is
/// stored in every durable WAL record, hashed into the mutation fingerprint,
/// and replayed by the change feed.
///
/// This is the only bound on the message: no transport-level body limit is
/// relied on.
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 4096;

/// Maximum attempts for a bounded compare-and-swap or allocation contention loop.
pub const MAX_CONTENTION_ATTEMPTS: usize = 8;

/// The most manifests that one check of a writer position moves it onto.
///
/// A walk that finds more drops the position, so the session's next publish
/// loads the namespace by discovery, which reads the hint instead of every
/// manifest in between. The walk sends at most one GET more than this.
pub const MAX_POSITION_MOVES: usize = 8;

/// Longest visible WAL tail, in WAL objects, a namespace may carry unfolded.
///
/// The commit engine refuses new commits at this length, so a published WAL
/// object never leaves more than this behind. A writer session keeps those
/// commits queued until its fold frees room, and returns
/// `maintenance_required` to a commit that's still waiting when the
/// write-stop wait passes.
pub const MAX_UNFOLDED_WAL_OBJECTS: u64 = 128;

/// Visible WAL-tail length, in WAL objects, that starts an automatic WAL fold.
pub const FOLD_AT_WAL_OBJECTS: u64 = 32;

const _: () = assert!(0 < FOLD_AT_WAL_OBJECTS && FOLD_AT_WAL_OBJECTS < MAX_UNFOLDED_WAL_OBJECTS);

/// Provider retry-admission deadline, in milliseconds. This alone does not
/// bound a final retry's backoff and request phase.
pub const PROVIDER_OPERATION_DEADLINE_MS: u64 = duration_ms(PROVIDER_OPERATION_DEADLINE);

/// One control-plane provider HTTP attempt's request timeout, in
/// milliseconds. Payload-sized conditional writes use a longer timeout.
pub const PROVIDER_ATTEMPT_TIMEOUT_MS: u64 = duration_ms(PROVIDER_ATTEMPT_TIMEOUT);

/// Allowance for publication request phases, including retry admission, final
/// backoff and a payload-sized final attempt.
///
/// Conditional publications always use a single request, even when their bodies
/// are large.
pub const PROVIDER_PUBLICATION_REQUEST_BOUND_MS: u64 =
    duration_ms(PROVIDER_PUBLICATION_REQUEST_BOUND);

/// Converts a duration to whole milliseconds at compile time. A value past
/// `u64::MAX` milliseconds fails the build instead of truncating.
const fn duration_ms(duration: std::time::Duration) -> u64 {
    let seconds_ms = duration
        .as_secs()
        .checked_mul(1_000)
        .expect("the duration should fit u64 milliseconds");
    seconds_ms + duration.subsec_millis() as u64
}

/// Maximum elapsed time from observing a tip to initiating its next numbered put.
///
/// A writer that commits again within this budget of its previous commit
/// doesn't send a request to confirm its tip first.
pub const WAL_PUBLISH_BUDGET_MS: u64 = 5 * 60 * 1000;

/// Self-enforced budget for one metadata publication: a fold, a compaction
/// step, a claim, a deletion, a fork installation, a retention advance, or a
/// grep step.
///
/// It runs from the start of the publication, before the publisher loads the
/// manifest it builds on, until the manifest put-if-absent is initiated. A
/// publication that exceeds it aborts without publishing; its immutable outputs
/// remain unreachable garbage-collection candidates.
pub const METADATA_PUBLICATION_BUDGET_MS: u64 = 15 * 60 * 1000;

/// Combined allowance for age overstatement from host-to-provider or
/// host-to-host clock error, timestamp precision, and scheduling delay around
/// publication checks.
///
/// Direct expiry comparisons do not add this margin to stored deadlines.
pub const GC_SAFETY_MARGIN_MS: u64 = 3 * 60 * 1000;

const _: () = assert!(GC_SAFETY_MARGIN_MS >= 1000);

const fn max_u64(left: u64, right: u64) -> u64 {
    if left > right {
        left
    } else {
        right
    }
}

/// Minimum age of an unreachable object before garbage collection or repair may
/// remove it.
///
/// The value covers the longer of two spans, then provider operation time and
/// the combined clock-error and scheduling allowance. The first span is the
/// longest publication budget. The second is the revalidation bound plus the
/// lifetime of a direct transfer capability: a view up to the bound old can
/// still issue a capability, and the capability outlives the view.
pub const GC_MIN_GRACE_WINDOW_MS: u64 = max_u64(
    max_u64(WAL_PUBLISH_BUDGET_MS, METADATA_PUBLICATION_BUDGET_MS),
    READ_REVALIDATION_BOUND_MS + DIRECT_TRANSFER_URL_TTL_MS,
) + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
    + GC_SAFETY_MARGIN_MS;

/// Longest gap a reader may leave between the manifest probe it relies on and
/// the successor check that confirms it.
///
/// A successor published after the probe cannot be collected within this bound:
/// collection waits the grace window, which reserves this bound plus the clock
/// allowance.
pub const READ_REVALIDATION_BOUND_MS: u64 =
    max_u64(WAL_PUBLISH_BUDGET_MS, METADATA_PUBLICATION_BUDGET_MS)
        + PROVIDER_PUBLICATION_REQUEST_BOUND_MS;

const _: () = assert!(READ_REVALIDATION_BOUND_MS + GC_SAFETY_MARGIN_MS <= GC_MIN_GRACE_WINDOW_MS);

/// A writer plans against its basis without checking it while the basis's
/// last confirmation is at most one WAL publication budget old. A batch
/// planned when the basis is a budget old can start its put up to a budget
/// later, because the put's budget runs from the batch's tip observation,
/// which the writer's previous commit refreshed. So the writer can read the
/// basis's segments, or commit a reference to content that only the basis
/// lists, until two budgets after the confirmation, and the provider applies
/// that put within its request bound. The basis's successor is created after
/// the confirmation, and garbage collection keeps the basis and its segments
/// for one grace after that.
const _: () = assert!(
    2 * WAL_PUBLISH_BUDGET_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS + GC_SAFETY_MARGIN_MS
        <= GC_MIN_GRACE_WINDOW_MS
);

/// A writer checks its basis once the last confirmation is more than a WAL
/// publication budget old, and it trusts the answer only within the
/// revalidation bound of that confirmation. A writer whose commits are less
/// than a budget apart sends that check when the basis is up to two budgets
/// old. If two budgets were longer than the bound, a writer whose commits
/// are spaced between half the bound and the budget would find its basis past
/// the bound at every other commit, and drop its position and replay its tail.
const _: () = assert!(2 * WAL_PUBLISH_BUDGET_MS <= READ_REVALIDATION_BOUND_MS);

const _: () = assert!(
    3 * PROVIDER_PUBLICATION_REQUEST_BOUND_MS + GC_SAFETY_MARGIN_MS <= GC_MIN_GRACE_WINDOW_MS,
    "3 * PROVIDER_PUBLICATION_REQUEST_BOUND_MS + GC_SAFETY_MARGIN_MS <= GC_MIN_GRACE_WINDOW_MS: \
     the final part, precondition, and completion requests a streamed write sends after its \
     last caller byte must land before an aborted upload's record can be removed"
);

/// Minimum provider age of a metadata segment no manifest lists before garbage
/// collection may delete it.
///
/// A compaction unit writes its output under `segments/` as it goes and
/// publishes at the end, so its earliest segment is unreferenced for the whole
/// run. One day is the bound on a run; the difference between this age and the
/// grace window below is how long a unit may keep merging before it abandons
/// its output.
pub const UNREFERENCED_SEGMENT_GRACE_WINDOW_MS: u64 = 24 * 60 * 60 * 1000;

/// Elapsed monotonic time after which a compaction unit abandons instead of
/// publishing.
///
/// A unit that starts its last publication attempt at this bound finishes within
/// one grace window, which the minimum age above reserves after the budget. The
/// subtraction fails to compile if the grace window ever exceeds the minimum
/// age.
pub const METADATA_COMPACTION_BUDGET_MS: u64 =
    UNREFERENCED_SEGMENT_GRACE_WINDOW_MS - GC_MIN_GRACE_WINDOW_MS;

/// Lifetime of a direct download or direct put capability.
///
/// A part capability writes no object, so the grace window does not need to
/// cover it.
pub const DIRECT_TRANSFER_URL_TTL_MS: u64 = 15 * 60 * 1000;

/// Minimum retirement grace from the deletion call's clock.
///
/// The tombstone put starts within the publication budget, and the provider
/// applies it within its request bound. Outstanding reads and direct
/// capabilities run from that landing, and the minimum grace window covers
/// them.
pub const NAMESPACE_RETIREMENT_GRACE_WINDOW_MS: u64 =
    METADATA_PUBLICATION_BUDGET_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS + GC_MIN_GRACE_WINDOW_MS;

/// Default age of an unreachable object before garbage collection may remove it.
///
/// A retention advance also treats this long before its clock as a cutoff,
/// so the floor doesn't pass a commit dated after that time, and a replaced
/// revision stays readable until the replacing commit is this old (format
/// spec, "Advancing the retention floor").
pub const GC_DEFAULT_GRACE_WINDOW_MS: u64 = 60 * 60 * 1000;

const _: () = assert!(GC_DEFAULT_GRACE_WINDOW_MS >= GC_MIN_GRACE_WINDOW_MS);

/// Most parts one direct multipart upload may cut into. This is the
/// S3-compatible ceiling, so with the session's part size it fixes the
/// largest object that session can carry: `part_size_bytes × 10_000`.
pub const MAX_MULTIPART_PARTS: u32 = PROVIDER_MAX_MULTIPART_PARTS;

/// Smallest part size a `direct_multipart` session may be opened with.
/// Every supported provider refuses a non-final part below 5 MiB.
pub const MIN_MULTIPART_PART_BYTES: u64 = PROVIDER_MIN_MULTIPART_PART_BYTES;

/// Largest part size a `direct_multipart` session may be opened with.
/// Every supported provider refuses a part above 5 GiB.
pub const MAX_MULTIPART_PART_BYTES: u64 = PROVIDER_MAX_MULTIPART_PART_BYTES;

/// Most part-upload capabilities one request may ask for. A client asks in
/// waves as it works through a file, so this bounds one response rather than
/// one upload.
pub const MAX_SIGNED_PARTS_PER_REQUEST: usize = 1_000;

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
/// collection.
///
/// The value covers the receipt window, receipt lifetime, and a final
/// publication with the clock-error and scheduling allowance. Every mint checks
/// the upload's receipt issuance window. Immediately before the WAL
/// put-if-absent, content proofs are checked against the request clock plus the
/// whole attempt's elapsed monotonic time.
pub const CONTENT_RECLAMATION_GRACE_WINDOW_MS: u64 =
    COMPLETED_UPLOAD_ADMISSION_WINDOW_MS + GC_MIN_GRACE_WINDOW_MS;

/// Bounds a chain's own extents while keeping shared base extents separate.
pub(crate) const MAX_LAYOUT_EXTENTS: usize = 32;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::GcOptions;

    #[test]
    fn derived_minimum_grace_window_sits_below_the_default() {
        // 19 min 15 s revalidation bound + 15 min capability lifetime
        // + 4 min 15 s request bound + 3 min margin = 41 min 30 s.
        assert_eq!(GC_MIN_GRACE_WINDOW_MS, 2_490_000);
        assert!(
            GC_MIN_GRACE_WINDOW_MS < GcOptions::default().grace_window_ms,
            "the conservative default grace window must satisfy its own floor"
        );
        assert!(
            GC_MIN_GRACE_WINDOW_MS
                > max_u64(WAL_PUBLISH_BUDGET_MS, METADATA_PUBLICATION_BUDGET_MS)
                    + PROVIDER_OPERATION_DEADLINE_MS,
            "the floor keeps a margin above budget plus provider deadline"
        );
    }
}
