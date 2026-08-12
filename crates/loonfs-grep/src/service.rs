//! [`GrepService`]: query planning and execution over grep index state.

use crate::cache::{
    DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockKind,
    DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use crate::codec::{lookup, Gram, IndexRow, INDEX_GRAMS_MAX_FILE_BYTES};
use crate::index_read::{
    index_segment_corrupt, load_data_block, load_filter_block, load_index_block,
};
use crate::keyspace::{manifest_key, segment_key};
use crate::query::{plan_pattern, GramPlanOutcome, GramQueryPlan};
use crate::reads::{published_revision, resolve_batch_size, NamespaceReads};
use crate::root::{
    load_grep_manifest, load_grep_root_pointer, ChangeFeedResume, GrepLifecycle, GrepManifestState,
};
use crate::{GrepError, Result};
use futures::future::{join_all, try_join_all};
use loonfs::{CoreError, CurrentFileState, MetadataViewError};
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::wire::sst_blocks::{
    decode_filter_block, index_blocks_for_key_range, string_prefix_upper_bound, BlockHandle,
};
use loonfs_api::{
    decode_cursor, encode_cursor, AbsolutePath, AuthoritativePathEntry, ChangeSeq, ErrorCode,
    GrepMatch, GrepPageCursor, GrepRequest, GrepResponse, InodeId, InodeKind, NamespaceId,
    RevisionNo,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

/// Matches returned per page when the request names no limit.
pub const DEFAULT_GREP_PAGE_LIMIT: usize = 100;
/// Largest per-page match limit a request may name.
pub const MAX_GREP_PAGE_LIMIT: usize = 1000;
/// Unindexed-tail revisions one query will scan exhaustively before
/// failing with `index_lagging` (or skipping the tail under `allow_stale`).
/// Advertised as the `query.grep.tail_budget_files` capability limit.
pub const MAX_GREP_TAIL_FILES: usize = 512;
/// Files a plan-less `allow_scan` query will scan before refusing.
/// Advertised as the `query.grep.scan_budget_files` capability limit.
pub const MAX_GREP_SCAN_FILES: usize = 4096;
/// Longest match line returned, in bytes; longer lines are truncated.
pub(crate) const GREP_LINE_CAP_BYTES: usize = 512;
/// Candidate files one page will read and verify before returning with a
/// resume cursor, so a page's cost is bounded by its own budget rather
/// than by how many false-positive candidates the plan admits.
pub(crate) const MAX_GREP_VERIFIED_FILES_PER_PAGE: usize = 256;
/// Candidates one page will examine — visibility, latest revision, path
/// derivation, scope — before returning with a resume cursor: the metadata
/// twin of [`MAX_GREP_VERIFIED_FILES_PER_PAGE`], for pages where a scope
/// filter rejects nearly every candidate. Rejected candidates move the
/// cursor with them (see `reorganize_rejected_frontier`), so the next page
/// continues past them instead of re-examining the same run.
pub(crate) const MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE: usize = 4096;
/// Maximum concurrent `(gram, segment)` posting probes.
///
/// Each probe may issue several small ranged reads, so this limit is lower
/// than the whole-content read limit.
pub(crate) const MAX_GREP_READ_IO: usize = 16;
/// Commits one unindexed-tail page asks the change feed for. The tail is
/// measured in files, not commits, so this is a transfer size rather than a
/// budget: paging continues until the tail is exhausted or exceeds
/// [`MAX_GREP_TAIL_FILES`].
pub(crate) const TAIL_FEED_PAGE_COMMITS: usize = 256;
/// Directory entries one plan-less scan page reads. The scan is bounded by
/// [`MAX_GREP_SCAN_FILES`]; this only sizes the reads that reach it.
pub(crate) const SCAN_DIRECTORY_PAGE_ENTRIES: usize = 1000;
/// Maximum concurrent content reads while verifying grep candidates.
///
/// Indexed files are size-limited, so content reads may use more concurrency
/// than posting probes. The per-page verification budget still caps the total
/// number of content reads.
pub(crate) const MAX_GREP_CONTENT_IO: usize = 32;

#[derive(Debug, Clone)]
struct MaterializedGrepIndexSnapshot {
    built_through_seq: ChangeSeq,
    next_event_index: u32,
    segments: Vec<GrepQuerySegment>,
}

#[derive(Debug, Clone)]
struct GrepQuerySegment {
    object_key: String,
    min_key: String,
    max_key: String,
    index_block: BlockHandle,
    filter_block: BlockHandle,
    filter_inline: Option<String>,
    payload_checksum: String,
}

impl GrepService {
    /// Freshly loads the grep pointer, then loads or reuses its immutable
    /// manifest.
    async fn load_index_snapshot<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        namespace_id: &NamespaceId,
    ) -> Result<MaterializedGrepIndexSnapshot> {
        let pointer = load_grep_root_pointer(store, namespace_id)
            .await
            .map_err(GrepError::from)?
            .ok_or(GrepError::NotEnabled)?;
        let manifest_id = pointer.pointer().manifest_id();
        let cache_key = GrepBlockCacheKey {
            payload_checksum: pointer.pointer().manifest_payload_checksum().to_owned(),
            block_kind: GrepBlockKind::Manifest,
            block_offset: 0,
        };
        let state = match self
            .block_cache
            .get_or_load(&cache_key, || async {
                let manifest = load_grep_manifest(store, namespace_id, pointer.pointer())
                    .await
                    .map_err(GrepError::from)?
                    .ok_or_else(|| GrepError::CorruptIndex {
                        message: format!(
                            "grep root `{}` names missing manifest `{}`",
                            pointer.object_key(),
                            manifest_key(namespace_id, manifest_id)
                        ),
                    })?;
                let state = Arc::new(manifest.manifest_state().clone());
                // As with the metadata manifest cache, JSON-backed decoded
                // state is weighted at twice its canonical payload bytes to
                // cover both owned strings and decoded structure overhead.
                let decoded_byte_len = serde_json::to_vec(manifest.manifest_state())
                    .map_err(|error| {
                        CoreError::Internal(format!(
                            "failed to size decoded grep manifest `{}`: {error}",
                            manifest_key(namespace_id, manifest_id)
                        ))
                    })?
                    .len()
                    .saturating_mul(2);
                Ok(DecodedGrepBlock::Manifest {
                    state,
                    decoded_byte_len,
                })
            })
            .await
        {
            Ok(DecodedGrepBlock::Manifest { state, .. }) => state,
            Ok(
                DecodedGrepBlock::Filter { .. }
                | DecodedGrepBlock::Index { .. }
                | DecodedGrepBlock::Data { .. },
            ) => {
                return Err(GrepError::CorruptIndex {
                    message: format!(
                        "grep manifest `{}` resolved to a non-manifest cache entry",
                        manifest_key(namespace_id, manifest_id)
                    ),
                });
            }
            Err(error) => return Err(error),
        };
        if state.namespace_id() != namespace_id {
            return Err(GrepError::CorruptIndex {
                message: format!(
                    "grep manifest `{}` names namespace `{}` instead of requested namespace `{namespace_id}`",
                    manifest_key(namespace_id, manifest_id),
                    state.namespace_id()
                ),
            });
        }
        materialized_snapshot_from_state(&state)
    }
}

fn materialized_snapshot_from_state(
    root: &GrepManifestState,
) -> Result<MaterializedGrepIndexSnapshot> {
    // Only a steady root has a watermark, and only a watermark makes an
    // index answerable: the other two phases refuse the query outright
    // rather than borrow a sequence they do not own.
    let (built_through_seq, next_event_index) = match root.lifecycle() {
        GrepLifecycle::Disabled => return Err(GrepError::NotEnabled),
        GrepLifecycle::Backfilling { .. } => return Err(GrepError::Backfilling),
        GrepLifecycle::Steady {
            built_through_seq,
            next_event_index,
        } => (*built_through_seq, *next_event_index),
    };
    let segments = root
        .segments()
        .iter()
        .map(|segment| GrepQuerySegment {
            object_key: segment_key(root.namespace_id(), &segment.segment_id),
            min_key: segment.min_row_key.clone(),
            max_key: segment.max_row_key.clone(),
            index_block: segment.index_block,
            filter_block: segment.filter_block,
            filter_inline: segment.filter_inline.clone(),
            payload_checksum: segment.payload_checksum.clone(),
        })
        .collect();
    Ok(MaterializedGrepIndexSnapshot {
        built_through_seq,
        next_event_index,
        segments,
    })
}

/// Namespace-independent grep execution over a decoded-block cache.
#[derive(Debug)]
pub struct GrepService {
    block_cache: Arc<GrepBlockCache>,
}

impl GrepService {
    /// Creates a service with a fresh default-sized block cache.
    pub fn new() -> Self {
        Self::with_block_cache(Arc::new(GrepBlockCache::new(
            DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
        )))
    }

    /// Creates a service over a host-composed process-wide grep block cache.
    pub fn with_block_cache(block_cache: Arc<GrepBlockCache>) -> Self {
        Self { block_cache }
    }
}

impl Default for GrepService {
    fn default() -> Self {
        Self::new()
    }
}

/// The revisions a query must examine, keyed by durable inode identity.
#[derive(Debug, Default)]
struct GrepCandidates {
    /// Index-supplied candidates: the revisions whose content contained
    /// every required gram. A candidate survives only if the inode's
    /// newest visible revision is in its set.
    indexed: BTreeMap<InodeId, BTreeSet<RevisionNo>>,
    /// Tail- or scan-supplied candidates: examined whatever their newest
    /// visible revision is, because no gram filter applies to them.
    unfiltered: BTreeSet<InodeId>,
}

impl GrepCandidates {
    fn inodes(&self) -> impl Iterator<Item = InodeId> + '_ {
        let mut merged: BTreeSet<InodeId> = self.indexed.keys().copied().collect();
        merged.extend(self.unfiltered.iter().copied());
        merged.into_iter()
    }

    /// Whether the inode's newest visible revision should be verified.
    fn admits(&self, inode_id: InodeId, revision_no: RevisionNo) -> bool {
        if self.unfiltered.contains(&inode_id) {
            return true;
        }
        self.indexed
            .get(&inode_id)
            .is_some_and(|revisions| revisions.contains(&revision_no))
    }
}

/// Evaluates the gram plan against all index segments.
///
/// For each required set, postings for alternative grams are unioned; the
/// results of required sets are intersected. Independent probes run in
/// bounded concurrent batches. Evaluation stops when the intersection is
/// empty or small enough to verify within the page budget. Stopping early
/// may widen the candidate set, but final byte-level pattern verification
/// preserves exact results.
async fn indexed_candidates<S: ObjectStore + ?Sized>(
    store: &S,
    block_cache: &GrepBlockCache,
    segments: &[GrepQuerySegment],
    plan: &GramQueryPlan,
) -> Result<BTreeMap<InodeId, BTreeSet<RevisionNo>>> {
    let mut intersection: Option<BTreeSet<(InodeId, RevisionNo)>> = None;
    for or_set in &plan.required {
        // Lookup keys derive once per gram; the key-range prune is free
        // (already in the descriptor), so only surviving probes fan out.
        let lookups: Vec<GramLookup> = or_set.iter().map(|gram| GramLookup::new(*gram)).collect();
        let mut probes: Vec<(&GramLookup, &GrepQuerySegment)> = Vec::new();
        for gram_lookup in &lookups {
            for descriptor in segments {
                if descriptor.max_key.as_str() < gram_lookup.probe.as_str()
                    || gram_lookup
                        .upper
                        .as_deref()
                        .is_some_and(|upper| descriptor.min_key.as_str() >= upper)
                {
                    continue;
                }
                probes.push((gram_lookup, descriptor));
            }
        }
        // This union is the query's largest temporary allocation. In the worst
        // case it holds one `(inode, revision)` pair per indexed revision. A
        // streaming intersection would reduce memory but add probe-ordering
        // complexity, so it is deferred until profiles show this allocation matters.
        let mut set_postings = BTreeSet::new();
        for chunk in probes.chunks(MAX_GREP_READ_IO) {
            let batches = try_join_all(chunk.iter().map(|(gram_lookup, descriptor)| {
                segment_postings_for_gram(store, block_cache, descriptor, gram_lookup)
            }))
            .await?;
            for batch in batches {
                set_postings.extend(batch);
            }
        }
        intersection = Some(match intersection {
            None => set_postings,
            Some(current) => current.intersection(&set_postings).copied().collect(),
        });
        if intersection.as_ref().is_some_and(BTreeSet::is_empty) {
            break;
        }
        // The budget stop: the intersection holds (inode, revision) pairs,
        // an upper bound on the files a page could ever verify from it, so
        // once it fits the per-page verification budget the remaining AND
        // sets are constraints the page cannot use.
        if intersection
            .as_ref()
            .is_some_and(|candidates| candidates.len() <= MAX_GREP_VERIFIED_FILES_PER_PAGE)
        {
            break;
        }
    }
    let mut candidates: BTreeMap<InodeId, BTreeSet<RevisionNo>> = BTreeMap::new();
    for (inode_id, revision_no) in intersection.unwrap_or_default() {
        candidates.entry(inode_id).or_default().insert(revision_no);
    }
    Ok(candidates)
}

/// One gram's derived lookup keys: the exact filter probe, the row-key
/// prefix its postings share, and that prefix's exclusive upper bound.
struct GramLookup {
    gram: Gram,
    probe: String,
    prefix: String,
    upper: Option<String>,
}

impl GramLookup {
    fn new(gram: Gram) -> Self {
        let probe = lookup::gram_probe(gram);
        let prefix = lookup::gram_prefix(gram);
        let upper = string_prefix_upper_bound(&prefix);
        Self {
            gram,
            probe,
            prefix,
            upper,
        }
    }
}

/// One gram's postings from one segment: bloom filter first (the inline
/// copy when the descriptor carries one — no fetch at all — otherwise the
/// cached filter block), then only the data blocks the segment index
/// names for the gram's key range, loaded concurrently in chunks of
/// [`MAX_GREP_READ_IO`] (the cap applies per probe). Every fetched
/// section resolves through the grep-private decoded-block cache.
async fn segment_postings_for_gram<S: ObjectStore + ?Sized>(
    store: &S,
    block_cache: &GrepBlockCache,
    descriptor: &GrepQuerySegment,
    gram_lookup: &GramLookup,
) -> Result<BTreeSet<(InodeId, RevisionNo)>> {
    let mut postings = BTreeSet::new();
    let admitted = match &descriptor.filter_inline {
        Some(inline) => {
            let filter_bytes =
                hex_decode_bytes(inline).map_err(|error| GrepError::CorruptIndex {
                    message: format!(
                        "index segment `{}` carries undecodable inline filter hex: {error}",
                        descriptor.object_key
                    ),
                })?;
            let filter =
                decode_filter_block(&filter_bytes, &descriptor.filter_block).map_err(|error| {
                    index_segment_corrupt(&descriptor.object_key, "filter block", &error)
                })?;
            filter.may_contain(&gram_lookup.probe)
        }
        None => {
            let filter = load_filter_block(
                store,
                block_cache,
                &descriptor.object_key,
                &descriptor.payload_checksum,
                &descriptor.filter_block,
            )
            .await?;
            filter.may_contain(&gram_lookup.probe)
        }
    };
    if !admitted {
        return Ok(postings);
    }
    let entries = load_index_block(
        store,
        block_cache,
        &descriptor.object_key,
        &descriptor.payload_checksum,
        &descriptor.index_block,
    )
    .await?;
    let range =
        index_blocks_for_key_range(&entries, &gram_lookup.prefix, gram_lookup.upper.as_deref());
    // The range's blocks are independent ranged GETs, so they fan out
    // instead of paying one round trip each; `try_join_all` returns them
    // in entry order and rows fold in that order, so assembly stays
    // deterministic even though the posting union is order-independent.
    for chunk in entries[range].chunks(MAX_GREP_READ_IO) {
        let blocks = try_join_all(chunk.iter().map(|entry| {
            load_data_block(
                store,
                block_cache,
                &descriptor.object_key,
                &descriptor.payload_checksum,
                &entry.block,
            )
        }))
        .await?;
        for block in blocks {
            for row in &block.rows {
                let IndexRow::GramPostings { gram: row_gram, .. } = row;
                if *row_gram != gram_lookup.gram {
                    continue;
                }
                let batch = row.postings().map_err(|error| {
                    index_segment_corrupt(&descriptor.object_key, "posting batch", &error)
                })?;
                postings.extend(
                    batch
                        .into_iter()
                        .map(|posting| (posting.inode_id, posting.revision_no)),
                );
            }
        }
    }
    Ok(postings)
}

/// The files whose content changed after the index cursor: the unindexed
/// tail a query scans exhaustively. A cursor inside its watermark commit
/// includes only that commit's remaining event suffix.
///
/// Paging stops as soon as the tail exceeds the query's tail budget: past
/// that the query either fails as lagging or serves the index's cut, and
/// neither needs the rest of the feed enumerated.
async fn tail_revisions(reads: &NamespaceReads<'_>, resume: ChangeFeedResume) -> Result<TailScan> {
    let mut inodes = BTreeSet::new();
    let mut after_seq = resume.after_seq();
    loop {
        let feed = reads
            .list_changes_after(after_seq, TAIL_FEED_PAGE_COMMITS)
            .await?;
        for change in &feed.changes {
            let start_event_index = resume.start_event_index(change.seq).map_err(|_| {
                CoreError::Internal("grep event cursor does not fit in memory".to_owned())
            })?;
            if start_event_index > change.events.len() {
                let next_event_index = resume.next_event_index();
                return Err(GrepError::CorruptIndex {
                    message: format!(
                        "grep event cursor `{next_event_index}` exceeds commit `{}` length `{}`",
                        change.seq,
                        change.events.len()
                    ),
                });
            }
            for event in change.events.iter().skip(start_event_index) {
                if let Some(revision) = published_revision(event) {
                    inodes.insert(revision.inode_id);
                }
            }
        }
        if inodes.len() > MAX_GREP_TAIL_FILES {
            return Ok(TailScan::OverBudget);
        }
        match feed.next_after_seq {
            Some(next_after_seq) => after_seq = next_after_seq,
            None => return Ok(TailScan::Within(inodes)),
        }
    }
}

/// The unindexed tail, or the fact that it is larger than one query scans.
enum TailScan {
    /// Every file whose content changed after the index watermark.
    Within(BTreeSet<InodeId>),
    /// More files changed after the watermark than the tail budget allows;
    /// enumeration stopped there, so no set is carried.
    OverBudget,
}

/// One verified line match inside a file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LineMatch {
    line_number: u64,
    byte_offset: u64,
    line: String,
    line_truncated: bool,
}

/// Runs the pattern over content, one match per line, in offset order.
/// One forward pass: matches arrive in offset order, so the current line's
/// bounds and number advance monotonically — a file full of matches costs
/// one scan of its bytes, never a repeated scan per match.
fn line_matches(content: &[u8], pattern: &regex::bytes::Regex) -> Vec<LineMatch> {
    let mut matches = Vec::new();
    let mut line_start = 0usize;
    let mut line_number = 1u64;
    let mut emitted_line_start = usize::MAX;
    for found in pattern.find_iter(content) {
        // Advance the line window forward to the one holding this match.
        while let Some(newline) = content[line_start..found.start()]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            line_start += newline + 1;
            line_number += 1;
        }
        if line_start == emitted_line_start {
            continue;
        }
        emitted_line_start = line_start;
        let line_end = content[found.start()..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(content.len(), |newline| found.start() + newline);
        let line_bytes = &content[line_start..line_end];
        let line_truncated = line_bytes.len() > GREP_LINE_CAP_BYTES;
        let line_bytes = &line_bytes[..line_bytes.len().min(GREP_LINE_CAP_BYTES)];
        matches.push(LineMatch {
            line_number,
            byte_offset: found.start() as u64,
            line: String::from_utf8_lossy(line_bytes).into_owned(),
            line_truncated,
        });
    }
    matches
}

/// Whether a file's current path lies inside the requested scope.
///
/// Both spellings are rendered from the same stored display names — the
/// candidate's by the current-state resolution, the scope's by resolving
/// the requested prefix — so this compares durable names rather than caller
/// input, and name-policy folding applies exactly as it does to every other
/// read.
fn path_within_scope(path: &AbsolutePath, scope: &AbsolutePath) -> bool {
    if scope.is_root() {
        return true;
    }
    let (path, scope) = (path.as_str(), scope.as_str());
    path == scope
        || (path.len() > scope.len()
            && path.starts_with(scope)
            && path.as_bytes()[scope.len()] == b'/')
}

/// Folds the page's highest examined-and-rejected candidate into the resume
/// cursor on a budget exit. Sound only when every examined candidate is
/// resolved — rejected, or scanned by a fully-walked batch — which the
/// budget exits guarantee. Never sound on a mid-file page fill: there,
/// later batch members were examined but their fetched contents discarded,
/// and the cursor must stay at the last emitted match.
fn reorganize_rejected_frontier(
    resume_cursor: &mut Option<(InodeId, u64)>,
    rejected_frontier: Option<InodeId>,
) {
    let Some(frontier) = rejected_frontier else {
        return;
    };
    let folded = (frontier, u64::MAX);
    if resume_cursor.is_none_or(|cursor| cursor < folded) {
        *resume_cursor = Some(folded);
    }
}

/// One grep candidate that survived the current-state checks and awaits its
/// content read, carrying everything match emission needs so the fetched
/// batch is processed without further metadata lookups.
struct GrepContentCandidate {
    inode_id: InodeId,
    revision_no: RevisionNo,
    path: AbsolutePath,
}

/// What one candidate's content fetch produced.
enum CandidateContent {
    /// The candidate's current path no longer names it at the pinned head:
    /// the derived path and the forward resolution disagree. Treated as a
    /// rejection, never as a match.
    Superseded,
    /// The declared content size exceeds the index eligibility cap, so no
    /// read was issued: the file could never pass the post-read text check,
    /// and the walk counts it as fully scanned.
    Oversized,
    /// The content read's result, surfaced at this candidate's position in
    /// the ordered walk rather than where the fan-out produced it.
    Fetched(Result<Vec<u8>>),
}

/// Resolves the candidate's current path forward — proving it still names
/// this inode at this revision — and reads the bytes that path publishes.
///
/// The forward resolution is what the derived path owes verification: a
/// path walked up from an inode is only a match's path if walking back down
/// reaches the same inode. It also supplies the content reference, so the
/// oversized skip stays a decision on declared size, before any fetch.
async fn candidate_content(
    reads: &NamespaceReads<'_>,
    candidate: &GrepContentCandidate,
) -> CandidateContent {
    let entry = match reads.resolve_path(&candidate.path).await {
        Ok(entry) => entry,
        Err(error) if error.code() == ErrorCode::PathNotFound => {
            return CandidateContent::Superseded
        }
        Err(error) => return CandidateContent::Fetched(Err(error)),
    };
    if entry.inode_id != candidate.inode_id || entry.revision_no() != Some(candidate.revision_no) {
        return CandidateContent::Superseded;
    }
    let Some(content_ref) = entry.content_ref() else {
        return CandidateContent::Superseded;
    };
    if content_ref.size_bytes > INDEX_GRAMS_MAX_FILE_BYTES {
        return CandidateContent::Oversized;
    }
    CandidateContent::Fetched(
        reads
            .read_content_ref(content_ref, INDEX_GRAMS_MAX_FILE_BYTES)
            .await,
    )
}

impl GrepService {
    /// Content search over one pinned snapshot: index-accelerated
    /// candidates through the `query.grep` watermark, an exhaustive scan of
    /// the unindexed tail, and real-pattern verification of every candidate
    /// against current state. Matches order by `(inode_id, byte_offset)`.
    /// Two budgets bound a page — the match limit and a verified-candidate
    /// budget — and the cursor resumes strictly after the last candidate the
    /// page finished scanning, bound to the request that issued it. Each
    /// page is evaluated against the snapshot it runs on and reports that
    /// head in `head_seq`.
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
        reads: &NamespaceReads<'_>,
        store: &S,
    ) -> Result<GrepResponse> {
        let head_seq = reads.head_seq().await?;
        let limit = match request.limit {
            None => DEFAULT_GREP_PAGE_LIMIT,
            Some(0) => {
                return Err(
                    CoreError::InvalidQuery("limit must be greater than zero".to_owned()).into(),
                );
            }
            Some(limit) if limit as usize > MAX_GREP_PAGE_LIMIT => {
                return Err(CoreError::InvalidQuery(format!(
                    "limit {limit} exceeds the maximum of {MAX_GREP_PAGE_LIMIT}"
                ))
                .into());
            }
            Some(limit) => limit as usize,
        };
        let fingerprint = request.fingerprint();
        let resume = match &request.cursor {
            Some(cursor) => {
                let cursor: GrepPageCursor = decode_cursor(cursor)
                    .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
                if cursor.fingerprint != fingerprint {
                    return Err(CoreError::InvalidCursor(
                        "the cursor was issued by a different request; replaying it \
                         under new criteria would silently skip results"
                            .to_owned(),
                    )
                    .into());
                }
                if cursor.head_seq > head_seq {
                    return Err(CoreError::from(MetadataViewError::SnapshotUnavailable {
                        requested_seq: cursor.head_seq,
                        head_seq,
                    })
                    .into());
                }
                Some((cursor.last_inode_id, cursor.last_byte_offset))
            }
            None => None,
        };

        let snapshot = self
            .load_index_snapshot(store, reads.namespace_id())
            .await?;

        // Line-anchored semantics: `^` and `$` match line boundaries, the
        // grep-family contract. The planner parses with the same flags so
        // its gram analysis sees the pattern the verifier runs.
        let pattern = regex::bytes::RegexBuilder::new(&request.pattern)
            .case_insensitive(request.case_insensitive)
            .multi_line(true)
            .build()
            .map_err(|error| CoreError::InvalidQuery(error.to_string()))?;

        // The scope filter resolves the requested prefix once, to the path
        // the namespace spells it as; every candidate is then tested against
        // that spelling, so name-policy folding and path normalization apply
        // exactly as they do to every other read. A prefix that resolves to
        // nothing has no matches.
        let scope = match &request.path_prefix {
            Some(prefix) => match reads.resolve_path(prefix).await {
                Ok(entry) => Some(entry),
                Err(error) if error.code() == ErrorCode::PathNotFound => {
                    return Ok(GrepResponse {
                        namespace_id: reads.namespace_id().clone(),
                        head_seq,
                        built_through_seq: snapshot.built_through_seq,
                        tail_scanned: true,
                        matches: Vec::new(),
                        next_cursor: None,
                    });
                }
                Err(error) => return Err(error),
            },
            None => None,
        };

        let mut candidates = GrepCandidates::default();
        let tail_resume = match plan_pattern(&request.pattern, request.case_insensitive)
            .map_err(CoreError::InvalidQuery)?
        {
            GramPlanOutcome::Indexable(plan) => {
                candidates.indexed =
                    indexed_candidates(store, &self.block_cache, &snapshot.segments, &plan).await?;
                Some(ChangeFeedResume::new(
                    snapshot.built_through_seq,
                    snapshot.next_event_index,
                ))
            }
            GramPlanOutcome::Unindexable => {
                if !request.allow_scan {
                    return Err(CoreError::QueryUnindexable(
                        "the pattern has no run of at least 3 literal bytes for the \
                         trigram index; set allow_scan to search without it"
                            .to_owned(),
                    )
                    .into());
                }
                // The walk enumerates current state, so a plan-less scan has
                // no unindexed tail of its own: everything committed through
                // this head is already a candidate.
                candidates.unfiltered = scan_candidate_inodes(reads, &scope).await?;
                None
            }
        };

        let mut tail_scanned = true;
        if let Some(tail_resume) = tail_resume {
            match tail_revisions(reads, tail_resume).await? {
                TailScan::Within(inodes) => candidates.unfiltered.extend(inodes),
                TailScan::OverBudget if request.allow_stale => {
                    // Serve the index's cut and nothing newer. A candidate
                    // whose current revision is past the watermark carries no
                    // posting for that revision, so `admits` already refuses
                    // it below — the skipped tail cannot leak a file verified
                    // at an unindexed revision.
                    tail_scanned = false;
                }
                TailScan::OverBudget => {
                    return Err(CoreError::IndexLagging {
                        behind_commits: head_seq.0.saturating_sub(snapshot.built_through_seq.0),
                    }
                    .into());
                }
            }
        }

        let mut matches: Vec<GrepMatch> = Vec::new();
        let mut verified_files = 0usize;
        let mut examined_candidates = 0usize;
        let mut has_more = false;
        // Where the next page resumes when this one stops early: the last
        // candidate this page finished scanning (offset MAX), or the last
        // emitted match when the page filled mid-file.
        let mut resume_cursor: Option<(InodeId, u64)> = None;
        // Highest candidate this page examined and rejected (invisible,
        // superseded, or out of scope). A rejection is final for the page,
        // so budget exits fold this into the cursor — without it, a run of
        // rejections longer than a page budget would resume at the same
        // cursor and re-reject the same candidates forever.
        let mut rejected_frontier: Option<InodeId> = None;
        // Candidates the cursor already covered never reach the walk, so
        // they cost neither examination budget nor a resolution.
        let ordered_candidates = candidates
            .inodes()
            .filter(|inode_id| match resume {
                Some((last_inode, last_offset)) => {
                    *inode_id > last_inode || (*inode_id == last_inode && last_offset != u64::MAX)
                }
                None => true,
            })
            .collect::<Vec<_>>();
        let mut next_candidate = 0usize;
        // Current state for candidates this page has resolved but not yet
        // walked: one resolution call serves many content batches.
        let mut resolved: VecDeque<CurrentFileState> = VecDeque::new();
        'page: loop {
            // Select the next fan-out batch: walk candidates in inode order
            // through the current-state checks until enough survivors need
            // content, the verified-file budget fills, or the candidates run
            // out. The content read is the only per-candidate store fetch,
            // so it is the only stage that fans out — the design doc's
            // "small fixed fan-out" for candidate reads.
            let mut batch: Vec<GrepContentCandidate> = Vec::new();
            let mut budget_exhausted = false;
            while batch.len() < MAX_GREP_CONTENT_IO {
                if resolved.is_empty() {
                    if next_candidate == ordered_candidates.len() {
                        break;
                    }
                    // The examination budget bounds a page's metadata work
                    // the way the verified budget bounds its content work: a
                    // scope filter that rejects nearly every candidate would
                    // otherwise resolve the entire candidate set in one
                    // page. Resolutions are requested in batches, never past
                    // what the budget still allows.
                    let examinable =
                        MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE.saturating_sub(examined_candidates);
                    if examinable == 0 {
                        budget_exhausted = true;
                        break;
                    }
                    let wanted = resolve_batch_size(
                        examinable.min(ordered_candidates.len() - next_candidate),
                    );
                    let chunk = &ordered_candidates[next_candidate..next_candidate + wanted];
                    resolved.extend(reads.resolve_current_files(chunk).await?);
                    next_candidate += wanted;
                }
                let Some(state) = resolved.pop_front() else {
                    continue;
                };
                let inode_id = state.inode_id;
                examined_candidates += 1;
                if !state.visible {
                    rejected_frontier = Some(inode_id);
                    continue;
                }
                let (Some(revision_no), Some(path)) =
                    (state.current_revision_no, state.current_path)
                else {
                    // A visible inode with no current revision is a
                    // directory, or a file whose revisions are gone; neither
                    // has content to match.
                    rejected_frontier = Some(inode_id);
                    continue;
                };
                if !candidates.admits(inode_id, revision_no) {
                    rejected_frontier = Some(inode_id);
                    continue;
                }
                if let Some(scope) = &scope {
                    if !path_within_scope(&path, &scope.absolute_path) {
                        rejected_frontier = Some(inode_id);
                        continue;
                    }
                }
                if verified_files == MAX_GREP_VERIFIED_FILES_PER_PAGE {
                    budget_exhausted = true;
                    break;
                }
                verified_files += 1;
                batch.push(GrepContentCandidate {
                    inode_id,
                    revision_no,
                    path,
                });
            }
            if batch.is_empty() {
                if budget_exhausted {
                    has_more = true;
                    reorganize_rejected_frontier(&mut resume_cursor, rejected_frontier);
                }
                break 'page;
            }
            // The reads fan out, but their errors do not short-circuit:
            // each result rides with its candidate into the ordered walk
            // below, which surfaces a failure only when it reaches that
            // candidate — the position the serial loop surfaced it. A
            // failure the walk never reaches (the page filled first) is
            // discarded with the rest of the speculative batch; the next
            // page re-issues that read and reports it then.
            let contents = join_all(
                batch
                    .iter()
                    .map(|candidate| candidate_content(reads, candidate)),
            )
            .await;
            // Emission stays strictly in candidate (inode) order: the batch
            // was selected in order and its results are consumed in order,
            // so matches, limits, errors, and the resume cursor advance
            // exactly as the serial walk advanced them.
            for (candidate, content) in batch.iter().zip(contents) {
                let inode_id = candidate.inode_id;
                let content = match content {
                    // Scanned and refused without a fetch, so the cursor
                    // moves past it like any other ineligible file.
                    CandidateContent::Oversized | CandidateContent::Superseded => {
                        resume_cursor = Some((inode_id, u64::MAX));
                        continue;
                    }
                    CandidateContent::Fetched(content) => content?,
                };
                if !is_indexable_text_content(&content) {
                    resume_cursor = Some((inode_id, u64::MAX));
                    continue;
                }
                for found in line_matches(&content, &pattern) {
                    if let Some((last_inode, last_offset)) = resume {
                        if inode_id == last_inode && found.byte_offset <= last_offset {
                            continue;
                        }
                    }
                    if matches.len() == limit {
                        // The page filled mid-batch: contents already
                        // fetched for the batch's later candidates are
                        // discarded, and the cursor resumes from the last
                        // processed candidate, never a discarded one.
                        has_more = true;
                        break 'page;
                    }
                    resume_cursor = Some((inode_id, found.byte_offset));
                    matches.push(GrepMatch {
                        absolute_path: candidate.path.clone(),
                        inode_id,
                        revision_no: candidate.revision_no,
                        line_number: found.line_number,
                        byte_offset: found.byte_offset,
                        line: found.line,
                        line_truncated: found.line_truncated,
                    });
                }
                // The file was fully scanned; a later stop resumes past it.
                resume_cursor = Some((inode_id, u64::MAX));
            }
            if budget_exhausted {
                has_more = true;
                // The whole final batch was scanned, so every examined
                // candidate is resolved; rejections past the last scanned
                // file move the cursor with them.
                reorganize_rejected_frontier(&mut resume_cursor, rejected_frontier);
                break 'page;
            }
        }

        let next_cursor = if has_more {
            let (last_inode_id, last_byte_offset) = resume_cursor.or(resume).ok_or_else(|| {
                CoreError::Internal(
                    "a truncated page must have scanned at least one candidate".to_owned(),
                )
            })?;
            Some(
                encode_cursor(&GrepPageCursor {
                    head_seq,
                    last_inode_id,
                    last_byte_offset,
                    fingerprint,
                })
                .map_err(|error| CoreError::Internal(error.to_string()))?,
            )
        } else {
            None
        };
        Ok(GrepResponse {
            namespace_id: reads.namespace_id().clone(),
            head_seq,
            built_through_seq: snapshot.built_through_seq,
            tail_scanned,
            matches,
            next_cursor,
        })
    }
}

/// Every visible file inside the query's scope, for plan-less scans.
///
/// A plan-less pattern has no gram filter, so the candidates are the files
/// themselves: a bounded directory walk from the scope root — the namespace
/// root when the request names no prefix — at the pinned head. The walk
/// reads current state, so it covers revisions the index has not reached
/// yet and needs no separate tail. Refuses past the scan budget, which
/// bounds directories as well as files: a namespace deep enough to exhaust
/// it is past what a plan-less query serves either way, and the count is
/// also what stops a walk if bindings ever formed a cycle.
async fn scan_candidate_inodes(
    reads: &NamespaceReads<'_>,
    scope: &Option<AuthoritativePathEntry>,
) -> Result<BTreeSet<InodeId>> {
    let mut inodes = BTreeSet::new();
    let root = match scope {
        Some(entry) if entry.inode_kind() == InodeKind::File => {
            // A file-shaped scope names exactly one candidate.
            inodes.insert(entry.inode_id);
            return Ok(inodes);
        }
        Some(entry) => entry.absolute_path.clone(),
        None => AbsolutePath::root(),
    };
    let mut directories = vec![root];
    let mut walked_directories = 0usize;
    while let Some(directory) = directories.pop() {
        walked_directories += 1;
        let mut cursor = None;
        loop {
            let page = reads
                .list_path_page(&directory, cursor, SCAN_DIRECTORY_PAGE_ENTRIES)
                .await?;
            for entry in page.items {
                match entry.inode_kind() {
                    InodeKind::Directory => directories.push(entry.absolute_path),
                    InodeKind::File => {
                        inodes.insert(entry.inode_id);
                    }
                }
            }
            if inodes.len() > MAX_GREP_SCAN_FILES || walked_directories > MAX_GREP_SCAN_FILES {
                return Err(CoreError::QueryUnindexable(format!(
                    "the namespace exceeds the {MAX_GREP_SCAN_FILES}-file scan budget; \
                     give the pattern a run of at least 3 literal bytes so the \
                     trigram index can narrow candidates"
                ))
                .into());
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(inodes)
}

/// True when content participates in the gram index: within the size cap
/// and text by the grep-family sniff (no NUL byte and valid UTF-8 in the
/// leading sample; a sample that ends inside a multi-byte character still
/// counts as valid).
pub(crate) fn is_indexable_text_content(content: &[u8]) -> bool {
    const ELIGIBILITY_SAMPLE_BYTES: usize = 8 * 1024;

    if content.len() as u64 > INDEX_GRAMS_MAX_FILE_BYTES {
        return false;
    }
    let sample = &content[..content.len().min(ELIGIBILITY_SAMPLE_BYTES)];
    if sample.contains(&0) {
        return false;
    }
    match std::str::from_utf8(sample) {
        Ok(_) => true,
        Err(error) => error.error_len().is_none() && error.valid_up_to() > 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(text: &str) -> regex::bytes::Regex {
        regex::bytes::Regex::new(text).expect("pattern")
    }

    #[test]
    fn line_matches_report_positions_once_per_line() {
        let content = b"alpha\nneedle one needle\nomega needle\n";
        let matches = line_matches(content, &pattern("needle"));
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].line_number, 2);
        assert_eq!(matches[0].byte_offset, 6);
        assert_eq!(matches[0].line, "needle one needle");
        assert_eq!(matches[1].line_number, 3);
        assert_eq!(matches[1].line, "omega needle");
    }

    #[test]
    fn rejected_frontier_folds_forward_only() {
        let mut cursor = None;
        reorganize_rejected_frontier(&mut cursor, Some(InodeId(9)));
        assert_eq!(cursor, Some((InodeId(9), u64::MAX)));
        cursor = Some((InodeId(12), 40));
        reorganize_rejected_frontier(&mut cursor, Some(InodeId(9)));
        assert_eq!(cursor, Some((InodeId(12), 40)));
    }

    #[test]
    fn scope_containment_stops_at_component_boundaries() {
        let path = |value: &str| AbsolutePath::parse(value).expect("valid path");
        let scope = path("/docs");
        assert!(path_within_scope(&scope, &scope), "the scope itself");
        assert!(path_within_scope(&path("/docs/a.txt"), &scope));
        assert!(path_within_scope(&path("/docs/deep/a.txt"), &scope));
        assert!(
            !path_within_scope(&path("/docsy/a.txt"), &scope),
            "a longer sibling name is not inside the scope"
        );
        assert!(!path_within_scope(&path("/other/a.txt"), &scope));
        assert!(
            path_within_scope(&path("/anything"), &AbsolutePath::root()),
            "the root scope holds everything"
        );
    }
}
