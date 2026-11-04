//! [`GrepService`]: query planning and execution over grep index state.

mod candidates;
mod page_walk;
mod value_files;

use crate::cache::GrepBlockCache;
use crate::entry::{ChangeFeedResume, GrepEntry, GrepIndexStatus};
use crate::query::{compile_pattern, CompiledPattern, GramPlanOutcome};
use crate::reads::{NamespaceReadView, NamespaceReads};
use crate::segments::GrepSegments;
use crate::{GrepError, Result};
use candidates::{add_tail_files, scan_candidate_files, tail_revisions, GrepCandidates, TailScan};
use loonfs::CoreError;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ExtensionSegmentRef;
use loonfs_types::{
    decode_cursor, ChangeSeq, GrepPageCursor, GrepRequest, GrepResponse, GrepScope, PageRequest,
    PathEntry, MAX_PAGE_LIMIT,
};
use page_walk::{walk_page, GrepResume};
use std::sync::Arc;

/// Unindexed-tail revisions one query will scan exhaustively before
/// failing with `index_lagging` (or skipping the tail under `allow_stale`).
/// Advertised as the `query.grep.tail_budget_files` capability limit.
pub const MAX_GREP_TAIL_FILES: usize = 512;
/// Files a plan-less `allow_scan` query will scan before refusing.
/// Advertised as the `query.grep.scan_budget_files` capability limit.
pub const MAX_GREP_SCAN_FILES: usize = 4096;
/// Longest match line returned, in bytes; longer lines are truncated.
pub(crate) const GREP_LINE_CAP_BYTES: usize = 512;
/// Candidate content values whose bytes one page will read and verify before
/// returning with a resume cursor, so a page's cost is bounded by its own
/// budget rather than by how many false-positive candidates the plan admits.
/// The files of each value count against
/// [`MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE`].
pub(crate) const MAX_GREP_VERIFIED_VALUES_PER_PAGE: usize = 256;
/// Maximum candidate files examined while building one page. This bounds work
/// when scope filters reject most candidates. The cursor advances past
/// rejected candidates so the next page does not examine them again.
pub(crate) const MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE: usize = 4096;
/// Maximum concurrent `(gram, segment)` posting probes, and the data-block
/// reads that one probe keeps in flight.
///
/// Each probe may issue several small ranged reads, so this limit is lower
/// than the whole-content read limit. A query's index lookup keeps at most
/// 16 probes times 16 data-block reads, or 256 reads, in flight.
pub(crate) const GREP_READS_IN_FLIGHT: usize = 16;
/// Commits one unindexed-tail page asks the change feed for. The tail is
/// measured in files, not commits, so this is a transfer size rather than a
/// budget: paging continues until the tail is exhausted or exceeds
/// [`MAX_GREP_TAIL_FILES`].
pub(crate) const TAIL_FEED_PAGE_COMMITS: usize = 256;
/// Directory entries one plan-less scan page reads. The scan is bounded by
/// [`MAX_GREP_SCAN_FILES`]; this only sizes the reads that reach it.
pub(crate) const SCAN_DIRECTORY_PAGE_ENTRIES: usize = 1000;
const _: () = assert!(
    0 < TAIL_FEED_PAGE_COMMITS
        && TAIL_FEED_PAGE_COMMITS <= MAX_PAGE_LIMIT as usize
        && 0 < SCAN_DIRECTORY_PAGE_ENTRIES
        && SCAN_DIRECTORY_PAGE_ENTRIES <= MAX_PAGE_LIMIT as usize
);
/// Maximum concurrent content reads while verifying grep candidates.
///
/// Indexed files are size-limited, so content reads may use more concurrency
/// than posting probes. The per-page verification budget still caps the total
/// number of content reads.
pub(crate) const GREP_CONTENT_READS_IN_FLIGHT: usize = 32;

/// Namespace-independent grep execution over a decoded-block cache.
#[derive(Debug)]
pub struct GrepService {
    block_cache: Arc<GrepBlockCache>,
}

/// The index that a query reads: grep's entry in the manifest of the
/// query's read view, which is active.
#[derive(Debug, Clone)]
struct ActiveIndex {
    /// The sequence that the index's backfill captured, which identifies
    /// the build that a cursor records.
    captured_seq: ChangeSeq,
    resume: ChangeFeedResume,
    entry: GrepEntry,
}

impl GrepService {
    /// Creates a service over a host-composed process-wide grep block cache.
    pub fn new(block_cache: Arc<GrepBlockCache>) -> Self {
        Self { block_cache }
    }

    /// The counters and retained bytes of the service's block cache.
    pub fn block_cache_stats(&self) -> loonfs::engine::DecodedBlockCacheStats {
        self.block_cache.stats()
    }

    async fn plan_query<'a, S: ObjectStore>(
        &'a self,
        request: &GrepRequest,
        cursor: Option<&str>,
        namespace_reads: &'a NamespaceReads,
        store: &'a S,
    ) -> Result<QueryPlan<'a, S>> {
        validate_grep_request(request)?;
        // The index is the entry in the view's manifest, which was published
        // after every commit that the entry records, so the view's head is
        // at or above every sequence that the entry records, such as its
        // build's capture and a reorganization's pruning floor.
        let head = namespace_reads.read_view().await?;
        let snapshot = active_index(head.grep_entry().await?)?;
        let fingerprint = request.fingerprint();
        let page = page_reads(
            cursor,
            head,
            namespace_reads,
            fingerprint,
            snapshot.captured_seq,
        )
        .await?;
        let read_seq = page.reads.head_seq();
        let CompiledPattern {
            regex: pattern,
            plan: gram_plan,
        } = compile_pattern(request)?;
        // A later page decides its cut at its read sequence as the service,
        // and that includes the scope.
        let member_reads = page.member_reads();
        let scope = match &request.scope {
            GrepScope::PathPrefix(prefix) => Some(member_reads.resolve_path(prefix).await?),
            GrepScope::Inode(inode_id) => Some(member_reads.resolve_inode(*inode_id).await?),
            GrepScope::Namespace => None,
        };
        let index = GrepSegments {
            store,
            namespace_id: namespace_reads.namespace_id(),
            block_cache: Some(&self.block_cache),
        };
        let (candidates, cut_seq, tail_scanned) =
            plan_candidates(request, gram_plan, &snapshot, &index, &page, scope.as_ref()).await?;
        let PageReads {
            reads,
            resume,
            later,
            ..
        } = page;
        Ok(QueryPlan {
            reads,
            later,
            read_seq,
            built_through_seq: snapshot.resume.built_through_seq(),
            index_captured_seq: snapshot.captured_seq,
            fingerprint,
            resume,
            pattern,
            scope,
            candidates,
            tail_scanned,
            cut_seq,
            index,
            index_runs: snapshot.entry.segments().cloned().collect(),
        })
    }

    /// Content search over one namespace read view, for the page at
    /// `page.cursor`.
    ///
    /// A cursor resumes only the request whose page returned it. A first page
    /// reads at the namespace head, and a later page reads at the sequence
    /// that the first page read at, which the cursor records.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "debug"),
        skip_all,
        fields(phase = "grep")
    )]
    pub async fn query<S: ObjectStore>(
        &self,
        request: &GrepRequest,
        page: PageRequest<String>,
        reads: &NamespaceReads,
        store: &S,
    ) -> Result<GrepResponse> {
        let plan = self
            .plan_query(request, page.cursor.as_deref(), reads, store)
            .await?;
        let page = walk_page(&plan, page.limit.as_usize()).await?;
        Ok(GrepResponse {
            namespace_id: plan.reads.namespace_id().clone(),
            head_seq: plan.read_seq,
            built_through_seq: plan.built_through_seq,
            tail_scanned: plan.tail_scanned,
            matches: page.matches,
            next_cursor: page.next_cursor,
        })
    }
}

/// The views and resume position of one page: the head and no resume for a
/// first page, and for a later page the views at the cursor's sequence and
/// the cursor's position and cut.
struct PageReads<'a> {
    reads: NamespaceReadView<'a>,
    resume: Option<GrepResume>,
    cursor_cut_seq: Option<ChangeSeq>,
    later: Option<LaterPage<'a>>,
}

impl PageReads<'_> {
    /// The view that the page decides its cut and scope through: the view at
    /// the read sequence as the service.
    fn member_reads(&self) -> &NamespaceReadView<'_> {
        self.later
            .as_ref()
            .map_or(&self.reads, |later| &later.members)
    }
}

/// Decodes and checks `cursor` against the request's `fingerprint`, the
/// serving `head`, and the index's `captured_seq`, and returns the views
/// that the page reads with.
async fn page_reads<'a>(
    cursor: Option<&str>,
    head: NamespaceReadView<'a>,
    namespace_reads: &'a NamespaceReads,
    fingerprint: u64,
    captured_seq: ChangeSeq,
) -> Result<PageReads<'a>> {
    let Some(cursor) = cursor else {
        return Ok(PageReads {
            reads: head,
            resume: None,
            cursor_cut_seq: None,
            later: None,
        });
    };
    let head_seq = head.head_seq();
    let cursor: GrepPageCursor =
        decode_cursor(cursor).map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    if cursor.fingerprint != fingerprint {
        return Err(CoreError::InvalidCursor(
            "the cursor was issued by a different request; replaying it under new criteria would silently skip results"
                .to_owned(),
        )
        .into());
    }
    if cursor.head_seq > head_seq {
        return Err(CoreError::InvalidCursor(format!(
            "the cursor was minted at seq `{}`, ahead of the serving head `{head_seq}`",
            cursor.head_seq
        ))
        .into());
    }
    if cursor.index_captured_seq != captured_seq {
        return Err(CoreError::InvalidCursor(
            "the cursor was issued by an earlier build of the grep index; start the search again"
                .to_owned(),
        )
        .into());
    }
    // Every page after the first reads at the first page's sequence, so a
    // file whose content changed between pages keeps its place in content
    // order.
    let at_cursor = head.at_seq(cursor.head_seq).await?;
    let later = LaterPage {
        members: namespace_reads.membership_view(cursor.head_seq).await?,
        head,
    };
    Ok(PageReads {
        reads: at_cursor,
        resume: Some(GrepResume::from_cursor(&cursor)),
        cursor_cut_seq: Some(cursor.cut_seq),
        later: Some(later),
    })
}

/// Returns a page's candidates, its cut sequence, and whether the page
/// scanned the unindexed tail: index lookups plus the tail for an indexable
/// pattern, and a scan of the scope's files for an `allow_scan` pattern
/// without one.
async fn plan_candidates<S: ObjectStore>(
    request: &GrepRequest,
    gram_plan: GramPlanOutcome,
    snapshot: &ActiveIndex,
    index: &GrepSegments<'_, S>,
    page: &PageReads<'_>,
    scope: Option<&PathEntry>,
) -> Result<(GrepCandidates, ChangeSeq, bool)> {
    let mut candidates = GrepCandidates::default();
    let read_seq = page.reads.head_seq();
    let mut cut_seq = read_seq;
    let mut tail_scanned = true;
    match gram_plan {
        GramPlanOutcome::Indexable(plan) => {
            candidates.indexed = index
                .indexed_candidates(snapshot.entry.runs(), &plan)
                .await?;
            match page
                .cursor_cut_seq
                .filter(|cursor_cut_seq| *cursor_cut_seq < read_seq)
            {
                // An earlier page of this stale search left out the
                // commits after its cut, so this page leaves them out
                // too, however far the index has moved since. The cut
                // stays at or below what this page's index covers in
                // full.
                Some(cursor_cut_seq) => {
                    tail_scanned = false;
                    cut_seq = cursor_cut_seq.min(stale_cut_seq(snapshot.resume));
                }
                None => match tail_revisions(&page.reads, snapshot.resume).await? {
                    TailScan::Within(tail) => {
                        add_tail_files(index, snapshot.entry.runs(), &mut candidates, tail).await?;
                    }
                    TailScan::OverBudget if request.allow_stale => {
                        tail_scanned = false;
                        cut_seq = cut_seq.min(stale_cut_seq(snapshot.resume));
                    }
                    TailScan::OverBudget => {
                        return Err(GrepError::IndexLagging {
                            behind_commits: read_seq
                                .0
                                .saturating_sub(snapshot.resume.built_through_seq().0),
                        });
                    }
                },
            }
        }
        GramPlanOutcome::Unindexable => {
            if !request.allow_scan {
                return Err(GrepError::QueryUnindexable(
                    "the pattern has no run of at least 3 literal bytes for the trigram index; set `allow_scan` to search without it"
                        .to_owned(),
                ));
            }
            candidates.files = scan_candidate_files(page.member_reads(), scope).await?;
        }
    }
    Ok((candidates, cut_seq, tail_scanned))
}

/// Checks the shape of a grep request: the pattern length.
fn validate_grep_request(request: &GrepRequest) -> Result<()> {
    const MAX_GREP_PATTERN_BYTES: usize = 1024;
    if request.pattern.len() > MAX_GREP_PATTERN_BYTES {
        return Err(loonfs::Error::InvalidRequest {
            message: format!(
                "grep pattern is {} bytes; the maximum is {MAX_GREP_PATTERN_BYTES} bytes",
                request.pattern.len()
            ),
            param: "pattern",
        }
        .into());
    }
    Ok(())
}

/// Returns the active index of `entry`. A namespace that grep was never
/// enabled on, and a disabled or backfilling index, return their errors.
fn active_index(entry: Option<GrepEntry>) -> Result<ActiveIndex> {
    let entry = entry.ok_or(GrepError::NotEnabled)?;
    let (captured_seq, resume) = match entry.status() {
        GrepIndexStatus::Disabled {} => return Err(GrepError::NotEnabled),
        GrepIndexStatus::Backfilling { .. } => return Err(GrepError::Backfilling),
        status @ GrepIndexStatus::Active { captured_seq, .. } => (
            *captured_seq,
            status
                .active_watermark()
                .expect("an active grep status should have a watermark"),
        ),
    };
    Ok(ActiveIndex {
        captured_seq,
        resume,
        entry,
    })
}

/// Everything one page of a query reads with: the read view, the candidates,
/// and the index that the candidates' files are read from.
struct QueryPlan<'a, S> {
    /// The subject's view at the read sequence. A later page reads the files
    /// that changed since its read sequence through it, which needs the
    /// subject's `history` right.
    reads: NamespaceReadView<'a>,
    /// The other views of a page after the first.
    later: Option<LaterPage<'a>>,
    /// The sequence that the page reads at: the head for a first page, and
    /// the cursor's sequence for a later page.
    read_seq: ChangeSeq,
    built_through_seq: ChangeSeq,
    /// The sequence that the index's backfill captured, which the page's
    /// cursor records.
    index_captured_seq: ChangeSeq,
    fingerprint: u64,
    resume: Option<GrepResume>,
    pattern: regex::bytes::Regex,
    scope: Option<PathEntry>,
    candidates: GrepCandidates,
    tail_scanned: bool,
    /// The newest commit whose `content_revisions` rows the page uses: the
    /// read sequence, or with `allow_stale` and a tail past its budget, the
    /// last commit that the index covers in full, if that's lower.
    cut_seq: ChangeSeq,
    index: GrepSegments<'a, S>,
    /// The segments of the index that the page reads.
    index_runs: Vec<ExtensionSegmentRef>,
}

/// The views of a page after the first, which reads at its cursor's
/// sequence.
///
/// The page decides its cut at that sequence through `members`, as the
/// service. A file in the cut whose state at the head is its state at the
/// read sequence (the same revision and path) is current content, which the
/// page serves through `head` with the subject's rights there, as a first
/// page would. The page serves any other file in the cut through the
/// subject's view at the read sequence, so only to a subject with the
/// `history` right.
struct LaterPage<'a> {
    members: NamespaceReadView<'a>,
    head: NamespaceReadView<'a>,
}

/// Returns the last commit that an index at `resume` covers in full, which a
/// stale query treats as the end of what it scanned. The commit at the
/// watermark is left out while the index covers only part of it.
fn stale_cut_seq(resume: ChangeFeedResume) -> ChangeSeq {
    match resume.next_event_index() {
        0 => resume.built_through_seq(),
        _ => ChangeSeq(resume.built_through_seq().0.saturating_sub(1)),
    }
}
