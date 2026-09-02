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
use crate::reads::{published_revision, resolve_batch_size, NamespaceReads, PinnedNamespaceReads};
use crate::root::{
    load_grep_manifest, load_grep_root_pointer, ChangeFeedResume, GrepIndexStatus,
    GrepManifestState, GrepSegmentRef,
};
use crate::{GrepError, Result};
use futures::future::{join_all, try_join_all};
use loonfs::{CoreError, CurrentFileState, MetadataViewError};
use loonfs_api::v0::FilesystemChange;
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::wire::sst_blocks::{
    decode_filter_block, index_blocks_for_key_range, key_range_may_intersect,
    string_prefix_upper_bound,
};
use loonfs_api::{
    decode_cursor, encode_cursor, AbsolutePath, ChangeSeq, EffectiveLimit, ErrorCode, GrepMatch,
    GrepPageCursor, GrepRequest, GrepResponse, InodeId, InodeKind, NamespaceId, PathEntry,
    RevisionNo,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
/// Candidate files one page will read and verify before returning with a
/// resume cursor, so a page's cost is bounded by its own budget rather
/// than by how many false-positive candidates the plan admits.
pub(crate) const MAX_GREP_VERIFIED_FILES_PER_PAGE: usize = 256;
/// Maximum candidates examined while building one page. This bounds work
/// when scope filters reject most candidates. The cursor advances past
/// rejected candidates so the next page does not examine them again.
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

/// Namespace-independent grep execution over a decoded-block cache.
#[derive(Debug)]
pub struct GrepService {
    block_cache: Arc<GrepBlockCache>,
}

#[derive(Debug, Clone)]
struct MaterializedGrepIndexSnapshot {
    resume: ChangeFeedResume,
    state: Arc<GrepManifestState>,
}

impl GrepService {
    /// Creates a service over a host-composed process-wide grep block cache.
    pub fn new(block_cache: Arc<GrepBlockCache>) -> Self {
        Self { block_cache }
    }

    /// Freshly loads the grep pointer, then loads or reuses its immutable
    /// manifest.
    async fn load_index_snapshot<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        namespace_id: &NamespaceId,
    ) -> Result<MaterializedGrepIndexSnapshot> {
        let pointer = load_grep_root_pointer(store, namespace_id)
            .await?
            .ok_or(GrepError::NotEnabled)?;
        let manifest_object_id = pointer.pointer().manifest_object_id();
        let cache_key = GrepBlockCacheKey {
            identity: pointer.pointer().manifest_payload_checksum().to_owned(),
            block_kind: GrepBlockKind::Manifest,
            block_offset: 0,
        };
        let state = match self
            .block_cache
            .get_or_load(&cache_key, || async {
                let manifest = load_grep_manifest(store, namespace_id, pointer.pointer())
                    .await?
                    .ok_or_else(|| GrepError::CorruptIndex {
                        message: format!(
                            "grep root `{}` names missing manifest `{}`",
                            pointer.object_key(),
                            manifest_key(namespace_id, manifest_object_id)
                        ),
                    })?;
                let state = Arc::new(manifest.manifest_state().clone());
                // As with the metadata manifest cache, JSON-backed decoded
                // state is weighted at twice its canonical payload bytes to
                // cover both owned strings and decoded structure overhead.
                let decoded_bytes = serde_json::to_vec(manifest.manifest_state())
                    .map_err(|error| {
                        CoreError::Internal(format!(
                            "failed to size decoded grep manifest `{}`: {error}",
                            manifest_key(namespace_id, manifest_object_id)
                        ))
                    })?
                    .len()
                    .saturating_mul(2);
                Ok(DecodedGrepBlock::Manifest {
                    manifest: state,
                    decoded_bytes,
                })
            })
            .await
        {
            Ok(DecodedGrepBlock::Manifest { manifest, .. }) => manifest,
            Ok(
                DecodedGrepBlock::Filter { .. }
                | DecodedGrepBlock::Index { .. }
                | DecodedGrepBlock::Data { .. },
            ) => {
                return Err(GrepError::CorruptIndex {
                    message: format!(
                        "grep manifest `{}` resolved to a non-manifest cache entry",
                        manifest_key(namespace_id, manifest_object_id)
                    ),
                });
            }
            Err(error) => return Err(error),
        };
        if state.namespace_id() != namespace_id {
            return Err(GrepError::CorruptIndex {
                message: format!(
                    "grep manifest `{}` names namespace `{}` instead of requested namespace `{namespace_id}`",
                    manifest_key(namespace_id, manifest_object_id),
                    state.namespace_id()
                ),
            });
        }
        materialized_snapshot_from_state(state)
    }

    async fn plan_query<'a, S: ObjectStore>(
        &self,
        request: &GrepRequest,
        reads: &NamespaceReads<'a>,
        store: &S,
    ) -> Result<QueryPlan<'a>> {
        let reads = reads.pin().await?;
        let head_seq = reads.head_seq();
        let fingerprint = request.fingerprint();
        let resume = match &request.cursor {
            Some(cursor) => {
                let cursor: GrepPageCursor = decode_cursor(cursor)
                    .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
                if cursor.fingerprint != fingerprint {
                    return Err(CoreError::InvalidCursor(
                        "the cursor was issued by a different request; replaying it under new criteria would silently skip results"
                            .to_owned(),
                    )
                    .into());
                }
                if cursor.head_seq > head_seq {
                    return Err(CoreError::from(MetadataViewError::CursorAheadOfHead {
                        cursor_seq: cursor.head_seq,
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
        let pattern = regex::bytes::RegexBuilder::new(&request.pattern)
            .case_insensitive(request.case_insensitive)
            .multi_line(true)
            .build()
            .map_err(|error| CoreError::InvalidQuery(error.to_string()))?;
        let scope = match &request.path_prefix {
            Some(prefix) => Some(reads.resolve_path(prefix).await?),
            None => None,
        };
        let mut candidates = GrepCandidates::default();
        let tail_resume = match plan_pattern(&request.pattern, request.case_insensitive)
            .map_err(CoreError::InvalidQuery)?
        {
            GramPlanOutcome::Indexable(plan) => {
                candidates.indexed = indexed_candidates(
                    store,
                    &self.block_cache,
                    reads.namespace_id(),
                    snapshot.state.segments(),
                    &plan,
                )
                .await?;
                Some(snapshot.resume)
            }
            GramPlanOutcome::Unindexable => {
                if !request.allow_scan {
                    return Err(CoreError::QueryUnindexable(
                        "the pattern has no run of at least 3 literal bytes for the trigram index; set allow_scan to search without it"
                            .to_owned(),
                    )
                    .into());
                }
                candidates.unfiltered = scan_candidate_inodes(&reads, scope.as_ref()).await?;
                None
            }
        };
        let mut tail_scanned = true;
        if let Some(tail_resume) = tail_resume {
            match tail_revisions(&reads, tail_resume).await? {
                TailScan::Within(inodes) => candidates.unfiltered.extend(inodes),
                TailScan::OverBudget | TailScan::RebuildRequired if request.allow_stale => {
                    tail_scanned = false;
                }
                TailScan::OverBudget | TailScan::RebuildRequired => {
                    return Err(CoreError::IndexLagging {
                        behind_commits: head_seq
                            .0
                            .saturating_sub(snapshot.resume.built_through_seq().0),
                    }
                    .into());
                }
            }
        }
        Ok(QueryPlan {
            reads,
            head_seq,
            built_through_seq: snapshot.resume.built_through_seq(),
            fingerprint,
            resume,
            pattern,
            scope,
            candidates,
            tail_scanned,
        })
    }

    /// Content search over one pinned namespace snapshot.
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
        limit: EffectiveLimit,
        reads: &NamespaceReads<'_>,
        store: &S,
    ) -> Result<GrepResponse> {
        let plan = self.plan_query(request, reads, store).await?;
        let mut walk = PageWalk::new(&plan, limit.as_usize());
        'page: while let Some(batch) = walk.next_batch().await? {
            let contents = join_all(
                batch
                    .candidates
                    .iter()
                    .map(|candidate| candidate_content(&plan.reads, candidate)),
            )
            .await;
            for (candidate, content) in batch.candidates.iter().zip(contents) {
                if walk.record(candidate, content)? {
                    break 'page;
                }
            }
            if walk.finish_batch(batch.exhausts_page) {
                break;
            }
        }
        let next_cursor = walk.next_cursor()?;
        Ok(GrepResponse {
            namespace_id: plan.reads.namespace_id().clone(),
            head_seq: plan.head_seq,
            built_through_seq: plan.built_through_seq,
            tail_scanned: plan.tail_scanned,
            matches: walk.matches,
            next_cursor,
        })
    }
}

fn materialized_snapshot_from_state(
    state: Arc<GrepManifestState>,
) -> Result<MaterializedGrepIndexSnapshot> {
    // Queries require an active index and its watermark. Disabled and
    // backfilling indexes return their corresponding errors.
    let resume = match state.status() {
        GrepIndexStatus::Disabled {} => return Err(GrepError::NotEnabled),
        GrepIndexStatus::Backfilling { .. } => return Err(GrepError::Backfilling),
        GrepIndexStatus::Active { .. } => state
            .status()
            .active_watermark()
            .expect("an active grep status should have a watermark"),
    };
    Ok(MaterializedGrepIndexSnapshot { resume, state })
}

impl Default for GrepService {
    fn default() -> Self {
        Self::new(Arc::new(GrepBlockCache::new(
            loonfs::DecodedBlockCacheConfig::with_max_decoded_bytes(
                DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
            ),
        )))
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
    namespace_id: &NamespaceId,
    segments: &[GrepSegmentRef],
    plan: &GramQueryPlan,
) -> Result<BTreeMap<InodeId, BTreeSet<RevisionNo>>> {
    let mut intersection: Option<BTreeSet<(InodeId, RevisionNo)>> = None;
    for or_set in &plan.required {
        // Lookup keys derive once per gram; the key-range prune is free
        // (already in the descriptor), so only surviving probes fan out.
        let lookups: Vec<GramLookup> = or_set.iter().map(|gram| GramLookup::new(*gram)).collect();
        let mut probes: Vec<(&GramLookup, &GrepSegmentRef)> = Vec::new();
        for gram_lookup in &lookups {
            for descriptor in segments {
                if !key_range_may_intersect(
                    &descriptor.min_row_key,
                    &descriptor.max_row_key,
                    descriptor.row_count,
                    &gram_lookup.probe,
                    gram_lookup.upper.as_deref(),
                ) {
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
                segment_postings_for_gram(store, block_cache, namespace_id, descriptor, gram_lookup)
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
    namespace_id: &NamespaceId,
    descriptor: &GrepSegmentRef,
    gram_lookup: &GramLookup,
) -> Result<BTreeSet<(InodeId, RevisionNo)>> {
    let object_key = segment_key(namespace_id, &descriptor.segment_id);
    let mut postings = BTreeSet::new();
    let admitted = match &descriptor.filter_inline {
        Some(inline) => {
            let filter_bytes =
                hex_decode_bytes(inline).map_err(|error| GrepError::CorruptIndex {
                    message: format!(
                        "index segment `{}` carries undecodable inline filter hex: {error}",
                        object_key
                    ),
                })?;
            let filter = decode_filter_block(&filter_bytes, &descriptor.filter_block)
                .map_err(|error| index_segment_corrupt(&object_key, "filter block", &error))?;
            filter.may_contain(&gram_lookup.probe)
        }
        None => {
            let filter = load_filter_block(store, block_cache, &object_key, descriptor).await?;
            filter.may_contain(&gram_lookup.probe)
        }
    };
    if !admitted {
        return Ok(postings);
    }
    let entries = load_index_block(store, block_cache, &object_key, descriptor).await?;
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
                &object_key,
                &descriptor.object_checksum,
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
                let batch = row
                    .postings()
                    .map_err(|error| index_segment_corrupt(&object_key, "posting batch", &error))?;
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
async fn tail_revisions(
    reads: &PinnedNamespaceReads<'_>,
    resume: ChangeFeedResume,
) -> Result<TailScan> {
    let mut inodes = BTreeSet::new();
    let mut after_seq = resume.after_seq();
    loop {
        let feed = reads
            .list_changes_after(after_seq, TAIL_FEED_PAGE_COMMITS)
            .await?;
        for change in &feed.changes {
            let start_event_index =
                resume
                    .start_event_index(change.committed_seq)
                    .map_err(|_| {
                        CoreError::Internal("grep event cursor does not fit in memory".to_owned())
                    })?;
            if start_event_index > change.events.len() {
                let next_event_index = resume.next_event_index();
                return Err(GrepError::CorruptIndex {
                    message: format!(
                        "grep event cursor `{next_event_index}` exceeds commit `{}` length `{}`",
                        change.committed_seq,
                        change.events.len()
                    ),
                });
            }
            for event in change.events.iter().skip(start_event_index) {
                if matches!(event, FilesystemChange::Undeleted { .. }) {
                    return Ok(TailScan::RebuildRequired);
                }
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
    /// An undelete may expose files absent from the checkpoint index. The
    /// event names only the restored root, so an exact query must wait for
    /// the worker to rebuild the projection from the now-visible tree.
    RebuildRequired,
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
fn fold_rejected_frontier(
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
    reads: &PinnedNamespaceReads<'_>,
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

struct QueryPlan<'a> {
    reads: PinnedNamespaceReads<'a>,
    head_seq: ChangeSeq,
    built_through_seq: ChangeSeq,
    fingerprint: u64,
    resume: Option<(InodeId, u64)>,
    pattern: regex::bytes::Regex,
    scope: Option<PathEntry>,
    candidates: GrepCandidates,
    tail_scanned: bool,
}

struct PageBatch {
    candidates: Vec<GrepContentCandidate>,
    exhausts_page: bool,
}

struct PageWalk<'plan, 'reads> {
    plan: &'plan QueryPlan<'reads>,
    limit: usize,
    matches: Vec<GrepMatch>,
    verified_files: usize,
    examined_candidates: usize,
    has_more: bool,
    resume_cursor: Option<(InodeId, u64)>,
    rejected_frontier: Option<InodeId>,
    ordered_candidates: Vec<InodeId>,
    next_candidate: usize,
    resolved: VecDeque<CurrentFileState>,
}

impl<'plan, 'reads> PageWalk<'plan, 'reads> {
    fn new(plan: &'plan QueryPlan<'reads>, limit: usize) -> Self {
        let ordered_candidates = plan
            .candidates
            .inodes()
            .filter(|inode_id| match plan.resume {
                Some((last_inode, last_offset)) => {
                    *inode_id > last_inode || (*inode_id == last_inode && last_offset != u64::MAX)
                }
                None => true,
            })
            .collect();
        Self {
            plan,
            limit,
            matches: Vec::new(),
            verified_files: 0,
            examined_candidates: 0,
            has_more: false,
            resume_cursor: None,
            rejected_frontier: None,
            ordered_candidates,
            next_candidate: 0,
            resolved: VecDeque::new(),
        }
    }

    async fn next_batch(&mut self) -> Result<Option<PageBatch>> {
        let mut candidates = Vec::new();
        let mut budget_exhausted = false;
        while candidates.len() < MAX_GREP_CONTENT_IO {
            if self.resolved.is_empty() {
                if self.next_candidate == self.ordered_candidates.len() {
                    break;
                }
                let examinable =
                    MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE.saturating_sub(self.examined_candidates);
                if examinable == 0 {
                    budget_exhausted = true;
                    break;
                }
                let wanted = resolve_batch_size(
                    examinable.min(self.ordered_candidates.len() - self.next_candidate),
                );
                let chunk =
                    &self.ordered_candidates[self.next_candidate..self.next_candidate + wanted];
                self.resolved
                    .extend(self.plan.reads.resolve_current_files(chunk).await?);
                self.next_candidate += wanted;
            }
            let Some(state) = self.resolved.pop_front() else {
                continue;
            };
            let inode_id = state.inode_id;
            self.examined_candidates += 1;
            if !state.visible {
                self.rejected_frontier = Some(inode_id);
                continue;
            }
            let (Some(revision_no), Some(path)) = (state.current_revision_no, state.current_path)
            else {
                self.rejected_frontier = Some(inode_id);
                continue;
            };
            if !self.plan.candidates.admits(inode_id, revision_no) {
                self.rejected_frontier = Some(inode_id);
                continue;
            }
            if self
                .plan
                .scope
                .as_ref()
                .is_some_and(|scope| !path_within_scope(&path, &scope.path))
            {
                self.rejected_frontier = Some(inode_id);
                continue;
            }
            if self.verified_files == MAX_GREP_VERIFIED_FILES_PER_PAGE {
                budget_exhausted = true;
                break;
            }
            self.verified_files += 1;
            candidates.push(GrepContentCandidate {
                inode_id,
                revision_no,
                path,
            });
        }
        if candidates.is_empty() {
            if budget_exhausted {
                self.has_more = true;
                fold_rejected_frontier(&mut self.resume_cursor, self.rejected_frontier);
            }
            return Ok(None);
        }
        Ok(Some(PageBatch {
            candidates,
            exhausts_page: budget_exhausted,
        }))
    }

    fn record(
        &mut self,
        candidate: &GrepContentCandidate,
        content: CandidateContent,
    ) -> Result<bool> {
        let inode_id = candidate.inode_id;
        let content = match content {
            CandidateContent::Oversized | CandidateContent::Superseded => {
                self.resume_cursor = Some((inode_id, u64::MAX));
                return Ok(false);
            }
            CandidateContent::Fetched(content) => content?,
        };
        if !is_indexable_text_content(&content) {
            self.resume_cursor = Some((inode_id, u64::MAX));
            return Ok(false);
        }
        for found in line_matches(&content, &self.plan.pattern) {
            if self.plan.resume.is_some_and(|(last_inode, last_offset)| {
                inode_id == last_inode && found.byte_offset <= last_offset
            }) {
                continue;
            }
            if self.matches.len() == self.limit {
                self.has_more = true;
                return Ok(true);
            }
            self.resume_cursor = Some((inode_id, found.byte_offset));
            self.matches.push(GrepMatch {
                path: candidate.path.clone(),
                inode_id,
                revision_no: candidate.revision_no,
                line_number: found.line_number,
                byte_offset: found.byte_offset,
                line: found.line,
                line_truncated: found.line_truncated,
            });
        }
        self.resume_cursor = Some((inode_id, u64::MAX));
        Ok(false)
    }

    fn finish_batch(&mut self, exhausts_page: bool) -> bool {
        if !exhausts_page {
            return false;
        }
        self.has_more = true;
        fold_rejected_frontier(&mut self.resume_cursor, self.rejected_frontier);
        true
    }

    fn next_cursor(&self) -> Result<Option<String>> {
        if !self.has_more {
            return Ok(None);
        }
        let (last_inode_id, last_byte_offset) =
            self.resume_cursor.or(self.plan.resume).ok_or_else(|| {
                CoreError::Internal(
                    "a truncated page must have scanned at least one candidate".to_owned(),
                )
            })?;
        Ok(Some(
            encode_cursor(&GrepPageCursor {
                head_seq: self.plan.head_seq,
                last_inode_id,
                last_byte_offset,
                fingerprint: self.plan.fingerprint,
            })
            .map_err(|error| CoreError::Internal(error.to_string()))?,
        ))
    }
}

/// Collects visible files in the query scope for a full scan.
///
/// The bounded walk starts at the requested path, or at the namespace root
/// when no path is given. The directory limit bounds the work and prevents a
/// binding cycle from running forever.
async fn scan_candidate_inodes(
    reads: &PinnedNamespaceReads<'_>,
    scope: Option<&PathEntry>,
) -> Result<BTreeSet<InodeId>> {
    let mut inodes = BTreeSet::new();
    let root = match scope {
        Some(entry) if entry.inode_kind() == InodeKind::File => {
            // A file scope contains only that file.
            inodes.insert(entry.inode_id);
            return Ok(inodes);
        }
        Some(entry) => entry.path.clone(),
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
                    InodeKind::Directory => directories.push(entry.path),
                    InodeKind::File => {
                        inodes.insert(entry.inode_id);
                    }
                }
            }
            if inodes.len() + directories.len() > MAX_GREP_SCAN_FILES
                || walked_directories > MAX_GREP_SCAN_FILES
            {
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
        fold_rejected_frontier(&mut cursor, Some(InodeId(9)));
        assert_eq!(cursor, Some((InodeId(9), u64::MAX)));
        cursor = Some((InodeId(12), 40));
        fold_rejected_frontier(&mut cursor, Some(InodeId(9)));
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
