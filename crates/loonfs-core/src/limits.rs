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

use loonfs_types::duration_ms;

use loonfs_objectstore::{
    PROVIDER_ATTEMPT_TIMEOUT, PROVIDER_MAX_MULTIPART_PARTS, PROVIDER_MAX_MULTIPART_PART_BYTES,
    PROVIDER_MIN_MULTIPART_PART_BYTES, PROVIDER_OPERATION_DEADLINE,
    PROVIDER_PUBLICATION_REQUEST_BOUND,
};
use std::num::NonZeroU64;

/// Caps each Bloom filter used by compaction and content collection.
pub const MAX_BLOOM_FILTER_BYTES: usize = 64 * 1024 * 1024;

/// Most levels of the walk that builds a base compaction's kept inode filter.
///
/// The walk's stack contains at most this many directories, counting the one
/// the walk starts at. Each level of the stack keeps a directory's
/// inode ID and the row key where the walk resumes that directory, which is
/// at most about 1.6 KiB for a name key of `MAX_NAME_KEY_BYTES`, so the stack
/// takes at most about 7 MiB. A move can nest a tree deeper than any request
/// path, and a compaction whose walk would need more levels keeps every row it
/// would have tested and reports that it skipped removing hidden rows.
pub const MAX_KEPT_INODE_WALK_DEPTH: usize = 4_096;

/// Most encoded segment bytes that one fold or compaction keeps in memory
/// while the segments' puts run.
///
/// The `STORE_WRITES_IN_FLIGHT` count alone allows 256 MiB of 8 MiB segments,
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

/// Stored bytes of metadata segment blocks that one refill of a scan asks
/// for.
///
/// A scan asks for the next blocks of its key range, up to this many stored
/// bytes, in one refill, and every block that the refill fetches goes into
/// the block cache. The tail latency of a GET from an object store is nearly
/// flat from tens of kilobytes to about 1 MiB, so a refill of 1 MiB costs
/// about as much as a GET of one block.
pub const MAX_SCAN_REFILL_BYTES: u64 = 1024 * 1024;

/// Maximum semantic operations in one explicit commit, bounding how long one
/// request can occupy the serialized publisher during planning and
/// content writes.
pub const MAX_COMMIT_OPERATIONS: usize = 4096;

/// Four MiB of event heap keeps ordinary change pages small while allowing several large commits per page.
pub const CHANGE_FEED_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Bounds the inodes that one diff page examines.
///
/// The count includes an inode once for each commit that touched it. The diff
/// keeps what a page reads about each of those inodes until the page ends, so
/// without this bound, a page of 1,000 commits of up to
/// [`MAX_COMMIT_OPERATIONS`] operations each could keep millions. A page ends
/// after the commit that reaches the bound, so the page returns that commit
/// whole. A request that sets `split_commits` reads a commit that alone
/// touches more inodes than the bound on pages of its own, each ending at the
/// event that reaches the bound.
pub const DIFF_PAGE_TOUCHES: usize = 16_384;

/// Bounds the admission work for one commit.
pub const MAX_COMMIT_PRECONDITIONS: usize = 1024;

/// Maximum content-token or prepared-proof entries included in one explicit
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

/// Manifest numbers that one window of a probe for newer manifests checks
/// at once.
///
/// Discovery, and a publication whose manifest put lost, send one HEAD
/// request for the number after the newest manifest they loaded. When that
/// manifest exists, they send one HEAD request for each of the next this
/// many numbers, all at once, and probe the next window only when the last
/// number of a window exists. A current hint costs the one HEAD request, and
/// a hint that lags by at most this many manifests costs one window more,
/// which takes about one round trip.
pub const MANIFEST_PROBE_WINDOW: usize = 8;

/// Unfolded WAL bytes that make a WAL fold due.
///
/// Unfolded WAL bytes are the stored sizes of the WAL objects above the
/// folded boundary, which a reader downloads to replay the tail and a writer
/// session keeps in memory. Inline content pieces travel in WAL objects, so
/// this trigger also bounds the unfolded inline bytes.
pub const FOLD_AT_WAL_BYTES: usize = 8 * 1024 * 1024;

/// Unfolded WAL objects that make a WAL fold due.
///
/// Each unfolded WAL object costs a reader one GET to replay the tail.
pub const FOLD_AT_WAL_OBJECTS: NonZeroU64 = NonZeroU64::new(256).unwrap();

/// Unfolded WAL bytes at which the commit engine refuses new commits.
///
/// This write stop bounds the bytes that a reader downloads to replay the
/// tail and that a writer session keeps in memory while folds fall behind.
/// One commit can still pass it, because the stop applies to the commits
/// after the tail reaches it. A writer session keeps those commits queued
/// until its fold frees room, and returns `maintenance_required` to a commit
/// that's still waiting when the write-stop wait passes.
pub const MAX_UNFOLDED_WAL_BYTES: usize = 64 * 1024 * 1024;

/// Unfolded WAL objects at which the commit engine refuses new commits.
///
/// This write stop bounds the GETs that a replay of the tail sends, because a
/// published WAL object never leaves more than this many objects behind. A
/// writer session keeps the refused commits queued until its fold frees
/// room, and returns `maintenance_required` to a commit that's still waiting
/// when the write-stop wait passes.
pub const MAX_UNFOLDED_WAL_OBJECTS: u64 = 1_024;

const _: () = assert!(FOLD_AT_WAL_BYTES < MAX_UNFOLDED_WAL_BYTES);
const _: () = assert!(FOLD_AT_WAL_OBJECTS.get() < MAX_UNFOLDED_WAL_OBJECTS);

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

/// Maximum elapsed time from observing a tip to initiating its next numbered put.
///
/// A writer that commits again within this budget of its previous commit
/// doesn't send a request to confirm its tip first.
pub const WAL_PUBLISH_BUDGET_MS: u64 = 5 * 60 * 1000;

/// Self-enforced budget for one metadata publication: a fold, a compaction
/// step, a claim, a deletion, a fork installation, or a grep step.
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
/// Every numbered put starts within a publication budget of the observation
/// it was planned against, and the provider applies it within its request
/// bound, so an object older than the longest budget plus the request bound
/// plus the clock allowance was written before any put still in flight was
/// planned. Collection roots only the current manifest and the pinned
/// manifests. A read whose manifest a newer one replaced can find a segment
/// collected; it returns `stale_head` and is read again. A read that must
/// outlive the current manifest takes a snapshot.
pub const GC_MIN_GRACE_WINDOW_MS: u64 =
    max_u64(WAL_PUBLISH_BUDGET_MS, METADATA_PUBLICATION_BUDGET_MS)
        + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
        + GC_SAFETY_MARGIN_MS;

pub use loonfs_types::limits::READ_REVALIDATION_BOUND_MS;

/// The revalidation bound is the longest publication budget plus the
/// provider's request bound. A WAL object or claim manifest that another
/// writer creates after this writer's successor check is younger than the
/// grace when a put that the check allowed runs, so the put collides with it
/// instead of finding a collected number.
const _: () = assert!(
    READ_REVALIDATION_BOUND_MS
        == max_u64(WAL_PUBLISH_BUDGET_MS, METADATA_PUBLICATION_BUDGET_MS)
            + PROVIDER_PUBLICATION_REQUEST_BOUND_MS
);

const _: () = assert!(READ_REVALIDATION_BOUND_MS + GC_SAFETY_MARGIN_MS <= GC_MIN_GRACE_WINDOW_MS);

/// A writer checks its basis once the last confirmation is more than a WAL
/// publication budget old, and the writer uses the result only within the
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
     last caller byte must complete before an aborted upload's record can be removed"
);

/// Minimum provider age of a metadata segment no manifest lists before garbage
/// collection may delete it.
///
/// A compaction unit writes its output under `segments/` as it goes and
/// publishes at the end, so its earliest segment is unreferenced for the whole
/// run. One day is the bound on a run; the difference between this age and the
/// grace window below is how long a unit may keep compacting before it abandons
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

/// Lifetime of a direct download or direct put presigned URL.
///
/// A download URL reads the content of a revision that a view at most the
/// revalidation bound old refers to. The content of the view's current
/// revision stays referenced until the URL ends, because a retention advance
/// passes the commit that replaces the revision only once that commit is a
/// default grace window old, and the assertion below the default grace keeps
/// that longer than the bound and this lifetime together. A URL for a
/// revision that the view already lists as replaced can stop working before
/// it expires, once a compaction removes the revision and a collection
/// deletes its content. A part presigned URL writes no object.
pub const DIRECT_TRANSFER_URL_TTL_MS: u64 = 15 * 60 * 1000;

/// Minimum retirement grace from the deletion call's clock.
///
/// The tombstone put starts within the publication budget, and the provider
/// applies it within its request bound. Reads and direct transfer URLs that
/// a view issued before the tombstone end within the minimum grace window
/// after that write.
pub const NAMESPACE_RETIREMENT_GRACE_WINDOW_MS: u64 =
    METADATA_PUBLICATION_BUDGET_MS + PROVIDER_PUBLICATION_REQUEST_BOUND_MS + GC_MIN_GRACE_WINDOW_MS;

/// Elapsed monotonic time, from the start of a pool's collection, within
/// which the collection reads the roots of every member of the pool, or skips
/// the pool's content sweep.
///
/// A member that the collection hasn't read yet can't be retired until the
/// retirement grace, less the clock allowance between the deleting host and
/// the collector, has passed since the collection started, so its fork pin keeps
/// it in the walk until then (format spec, "Content roots").
pub const POOL_ROOTS_READ_BUDGET_MS: u64 =
    NAMESPACE_RETIREMENT_GRACE_WINDOW_MS - GC_SAFETY_MARGIN_MS;

pub use loonfs_types::limits::{GC_DEFAULT_GRACE_WINDOW_MS, RETENTION_ADVANCE_MIN_COMMIT_AGE_MS};

const _: () = assert!(GC_DEFAULT_GRACE_WINDOW_MS >= GC_MIN_GRACE_WINDOW_MS);

/// A member that replaces or deletes a revision after a pool's
/// collection started keeps the revision's rows until a retention advance
/// passes that commit, at least the advance's minimum commit age later, so
/// the collection reads the member's current manifest with the reference
/// still in it (format section 11.9).
const _: () = assert!(POOL_ROOTS_READ_BUDGET_MS < RETENTION_ADVANCE_MIN_COMMIT_AGE_MS);

/// A retention advance moves the floor only past commits older than the
/// advance's minimum commit age, so a revision stays referenced for at least
/// that long after a commit replaces it. A view at most the revalidation
/// bound old, and a direct transfer URL it issued, read content that was
/// referenced when the view was checked, and the margin keeps it referenced
/// until the URL ends.
const _: () = assert!(
    READ_REVALIDATION_BOUND_MS + DIRECT_TRANSFER_URL_TTL_MS + GC_SAFETY_MARGIN_MS
        <= RETENTION_ADVANCE_MIN_COMMIT_AGE_MS
);

/// Most parts one direct multipart upload may cut into. This is the
/// S3-compatible ceiling, so with the session's part size it fixes the
/// largest object that session can upload: `part_size_bytes × 10_000`.
pub const MAX_MULTIPART_PARTS: u32 = PROVIDER_MAX_MULTIPART_PARTS;

/// Smallest part size a `direct_multipart` session may be opened with.
/// Every supported provider refuses a non-final part below 5 MiB.
pub const MIN_MULTIPART_PART_BYTES: u64 = PROVIDER_MIN_MULTIPART_PART_BYTES;

/// Largest part size a `direct_multipart` session may be opened with.
/// Every supported provider refuses a part above 5 GiB.
pub const MAX_MULTIPART_PART_BYTES: u64 = PROVIDER_MAX_MULTIPART_PART_BYTES;

/// Most part-upload presigned URLs one request may ask for. A client asks in
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

/// The fewest binding rows that one directory scan reads per manifest round
/// trip.
///
/// A directory page, a tree listing, and the kept inode walk of a base
/// compaction each keep at most this many manifest rows of a scan at once, or
/// the page's limit when that is larger.
pub(crate) const DIRECTORY_PAGE_RAW_SCAN_LIMIT: usize = 64;

/// Most inode IDs that one batch resolution of current files accepts.
///
/// One item costs about what one row of a listing page costs, so the batch
/// has the bound of a page.
pub const MAX_CURRENT_FILES_PER_STAT: usize = 1_000;

const _: () = assert!(MAX_CURRENT_FILES_PER_STAT as u64 == loonfs_types::MAX_PAGE_LIMIT as u64);

/// Bytes fetched by each ranged content read.
///
/// A content stream keeps at most one chunk of a file's bytes at a time.
pub const CONTENT_READ_CHUNK_BYTES: NonZeroU64 = NonZeroU64::new(8 * 1024 * 1024).unwrap();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::GcOptions;

    #[test]
    fn derived_minimum_grace_window_sits_below_the_default() {
        // 15 min publication budget + 4 min 15 s request bound + 3 min margin
        // = 22 min 15 s.
        assert_eq!(GC_MIN_GRACE_WINDOW_MS, 1_335_000);
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
