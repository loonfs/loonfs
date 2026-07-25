//! [`GrepService`]: query planning and execution over a core read view.

use crate::cache::{
    DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockKind, MAX_CACHED_GREP_BLOCKS,
};
use crate::codec::{lookup, Gram, IndexRow, INDEX_GRAMS_MAX_FILE_BYTES};
use crate::index_read::{
    index_segment_corrupt, load_data_block, load_filter_block, load_index_block,
};
use crate::keyspace::{manifest_key, segment_key};
use crate::query::{plan_pattern, GramPlanOutcome, GramQueryPlan};
use crate::root::{
    load_grep_manifest, load_grep_root_pointer, ChangeFeedResume, GrepLifecycle, GrepRootState,
};
use crate::{GrepError, Result};
use futures::future::{join_all, try_join_all};
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::wire::sst_blocks::{decode_filter_block, index_blocks_for_key_range, BlockHandle};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    decode_cursor, encode_cursor, AbsolutePath, ChangeSeq, ErrorCode, GrepMatch, GrepPageCursor,
    GrepRequest, GrepResponse, InodeId, NamespaceId, RevisionNo,
};
use loonfs_core::content::read_durable_content_bytes;
use loonfs_core::grep::{
    string_prefix_upper_bound, LeafRevisionPrefetch, LoadedMetadataView, MetadataViewSession,
    REVISION_ROW_PREFIX,
};
use loonfs_core::metadata::RevisionRecord;
use loonfs_core::{Error as CoreError, MetadataViewError};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
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
/// Concurrent gram posting probes one grep query issues at a time: the
/// (gram, segment) probes of an OR-set fan out in chunks of this size,
/// each probe a handful of small ranged GETs (filter, index, and posting
/// blocks). Deliberately below [`MAX_GREP_CONTENT_IO`]: probes multiply
/// into many small requests per chunk, where a content read is one whole
/// object. The read-side sibling of the maintenance path's
/// `MAX_MAINTENANCE_TABLE_IO` (`checkpoint/runs.rs`), which stays at its
/// own value.
pub(crate) const MAX_GREP_READ_IO: usize = 16;
/// Concurrent candidate content reads one grep query issues at a time.
/// Content verification is a whole-object GET of a small file (index
/// eligibility caps candidates at `INDEX_GRAMS_MAX_FILE_BYTES`), so it
/// tolerates a wider fan-out than the posting probes: 32 matches the
/// content-object concurrency the ecosystem already runs against these
/// stores (bulk content uploads fan out 32 wide). Each batch is
/// additionally bounded by the remaining
/// [`MAX_GREP_VERIFIED_FILES_PER_PAGE`] budget, so a page issues at most
/// that many content reads however wide the batches are.
pub(crate) const MAX_GREP_CONTENT_IO: usize = 32;

/// The immutable gram-index state one query reads.
#[derive(Debug, Clone)]
pub struct GrepIndexSnapshot {
    state: Result<MaterializedGrepIndexSnapshot>,
}

#[derive(Debug, Clone)]
struct MaterializedGrepIndexSnapshot {
    built_through_seq: ChangeSeq,
    next_delta_index: u32,
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

impl GrepIndexSnapshot {
    /// Captures unreadable grep extension state while preserving error precedence.
    pub fn from_error(error: GrepError) -> Self {
        Self { state: Err(error) }
    }

    /// Freshly loads the grep pointer, then loads or reuses its immutable manifest.
    ///
    /// Missing or disabled roots and incomplete backfills remain
    /// not-materialized. Unreadable derived state retains its actionable
    /// grep-specific failure without changing core read behavior.
    pub async fn from_grep_root<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        service: &GrepService,
    ) -> Self {
        let pointer = match load_grep_root_pointer(store, namespace_id).await {
            Ok(Some(pointer)) => pointer,
            Ok(None) => return Self::from_error(GrepError::NotEnabled),
            Err(error) => return Self::from_error(error.into()),
        };
        let manifest_id = pointer.pointer().manifest_id();
        let cache_key = GrepBlockCacheKey {
            payload_checksum: manifest_id.payload_checksum(),
            block_kind: GrepBlockKind::Manifest,
            block_offset: 0,
        };
        let state = match service.block_cache.get(&cache_key) {
            Some(DecodedGrepBlock::Manifest(state)) => state,
            Some(
                DecodedGrepBlock::Filter(_)
                | DecodedGrepBlock::Index(_)
                | DecodedGrepBlock::Data(_),
            ) => {
                return Self::from_error(GrepError::CorruptIndex {
                    message: format!(
                        "grep manifest `{}` resolved to a non-manifest cache entry",
                        manifest_key(namespace_id, manifest_id)
                    ),
                });
            }
            None => {
                let manifest = match load_grep_manifest(store, namespace_id, manifest_id).await {
                    Ok(Some(manifest)) => manifest,
                    Ok(None) => {
                        return Self::from_error(GrepError::CorruptIndex {
                            message: format!(
                                "grep root `{}` names missing manifest `{}`",
                                pointer.object_key(),
                                manifest_key(namespace_id, manifest_id)
                            ),
                        });
                    }
                    Err(error) => return Self::from_error(error.into()),
                };
                let state = Arc::new(manifest.state().clone());
                service
                    .block_cache
                    .insert(cache_key, DecodedGrepBlock::Manifest(state.clone()));
                state
            }
        };
        if state.namespace_id() != namespace_id {
            return Self::from_error(GrepError::CorruptIndex {
                message: format!(
                    "grep manifest `{}` names namespace `{}` instead of requested namespace `{namespace_id}`",
                    manifest_key(namespace_id, manifest_id),
                    state.namespace_id()
                ),
            });
        }
        Self::from_state(&state)
    }

    fn from_state(root: &GrepRootState) -> Self {
        match root.lifecycle() {
            GrepLifecycle::Disabled => return Self::from_error(GrepError::NotEnabled),
            GrepLifecycle::Backfilling { .. } => {
                return Self::from_error(GrepError::Backfilling);
            }
            GrepLifecycle::Steady => {}
        }
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
        Self {
            state: Ok(MaterializedGrepIndexSnapshot {
                built_through_seq: root.index().built_through_seq,
                next_delta_index: root.index().next_delta_index,
                segments,
            }),
        }
    }

    fn materialized(&self) -> Result<&MaterializedGrepIndexSnapshot> {
        self.state.as_ref().map_err(Clone::clone)
    }
}

/// Namespace-independent grep execution with its own decoded-block cache.
#[derive(Debug)]
pub struct GrepService {
    block_cache: GrepBlockCache,
}

impl GrepService {
    /// Creates a service with the fixed grep-private block-cache bound.
    pub fn new() -> Self {
        Self {
            block_cache: GrepBlockCache::new(MAX_CACHED_GREP_BLOCKS),
        }
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

/// Evaluates the plan against every gram index segment: for each AND set,
/// the union of its grams' postings; the candidates are the intersection.
/// The probes of one AND set — one per surviving (gram, segment) pair —
/// are independent, so they run concurrently in chunks of
/// [`MAX_GREP_READ_IO`]; the sets union order-independently, so the
/// result is identical to a serial evaluation, and an AND set whose
/// running intersection empties still short-circuits the sets after it.
///
/// Probing also stops once the running intersection fits the page's
/// verification budget ([`MAX_GREP_VERIFIED_FILES_PER_PAGE`]): further AND
/// sets could only shrink a candidate set the page can already afford to
/// verify whole. The invariant that makes this sound: dropping AND
/// constraints only WIDENS the candidate set, and verification runs the
/// real pattern over every candidate, so grep results are byte-identical
/// — the only effect is fewer cold posting reads (a rare literal's 16
/// single-gram sets typically stop after the first few). The stop rule is
/// deliberately this simple; a refinement that also stops when an AND set
/// fails to shrink the intersection materially (common terms whose grams
/// match nearly everything) is left out until evidence demands a
/// heuristic.
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
        // This union is the query's peak memory: worst case one
        // (inode, revision) pair per indexed revision when a gram is
        // common to every file — 16 bytes a pair, on the order of 16 MiB
        // per million indexed revisions. A streamed merge-intersection
        // would trade that ceiling for probe-ordering complexity; not
        // taken until a profile shows these unions dominating.
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

/// The file revisions after the index cursor, newest revision per inode, from
/// WAL replay — the exhaustive-scan tail. A cursor within its watermark
/// commit includes only that commit's remaining delta-vector suffix.
async fn tail_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    view: &LoadedMetadataView<'_, S>,
    resume: ChangeFeedResume,
) -> Result<BTreeSet<InodeId>> {
    let mut tail = BTreeMap::new();
    for record in view
        .grep_wal_records_after(store, resume.after_seq())
        .await?
    {
        let start_delta_index = resume.start_delta_index(record.seq).map_err(|_| {
            CoreError::Internal("grep delta cursor does not fit in memory".to_owned())
        })?;
        if start_delta_index > record.deltas.len() {
            let next_delta_index = resume.next_delta_index();
            return Err(GrepError::CorruptIndex {
                message: format!(
                    "grep delta cursor `{next_delta_index}` exceeds commit `{}` length `{}`",
                    record.seq,
                    record.deltas.len()
                ),
            });
        }
        for delta in record.deltas.iter().skip(start_delta_index) {
            if let WalDelta::AppendFileRevision { inode_id, .. } = &delta.delta {
                tail.insert(*inode_id, ());
            }
        }
    }
    Ok(tail.into_keys().collect())
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

/// Derives the inode's visible absolute path by walking active parent
/// bindings to the root, then verifies the derived path forward under the
/// full visibility rules — tombstones, unbinds, kinds — by resolving it
/// back to the same inode. `None` means the inode is not visible at the
/// view's sequence. Runs over the page's session so candidates under the
/// same directories share the ancestor lookups.
async fn derive_visible_path<S: ObjectStore + ?Sized>(
    session: &mut MetadataViewSession<'_, '_, S>,
    inode_id: InodeId,
) -> Result<Option<VisiblePathChain>> {
    const MAX_PATH_DEPTH: usize = 4096;
    let mut segments = Vec::new();
    // The chain includes the inode itself and every ancestor up to the
    // root, so scope filters test durable identity instead of comparing
    // path strings (which would bypass name-policy folding and slash
    // normalization).
    let mut ancestors = vec![inode_id];
    let mut current = inode_id;
    while current != InodeId(1) {
        if segments.len() >= MAX_PATH_DEPTH {
            return Ok(None);
        }
        let Some(binding) = session.current_parent_binding_for_child(current).await? else {
            return Ok(None);
        };
        segments.push(binding.display_name.clone());
        current = binding.parent_inode_id;
        ancestors.push(current);
    }
    segments.reverse();
    let path = format!("/{}", segments.join("/"));
    let parsed = match AbsolutePath::parse(&path) {
        Ok(parsed) => parsed,
        // A display name the path grammar rejects cannot be served as a
        // path result; the file is unreachable by path and skipped.
        Err(_) => return Ok(None),
    };
    match session
        .resolve_visible_path(&parsed, LeafRevisionPrefetch::Skip)
        .await
    {
        Ok(resolved) if resolved.inode_id == inode_id => Ok(Some(VisiblePathChain {
            path: parsed,
            ancestors,
        })),
        Ok(_) => Ok(None),
        Err(error) if error.code() == ErrorCode::PathNotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
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

/// A derived visible path plus the inode chain that produced it, root
/// included.
struct VisiblePathChain {
    path: AbsolutePath,
    ancestors: Vec<InodeId>,
}

/// One grep candidate that survived the cheap visibility checks and awaits
/// its content read, carrying everything match emission needs so the
/// fetched batch is processed without further metadata lookups.
struct GrepContentCandidate {
    inode_id: InodeId,
    revision: RevisionRecord,
    path: AbsolutePath,
    /// The declared content size exceeds the index eligibility cap, so no
    /// read is scheduled: the file could never pass the post-read text
    /// check, and the walk skips it as fully scanned.
    oversized: bool,
}

impl GrepService {
    /// Content search over the view: index-accelerated candidates through
    /// the `grep.index` watermark, an exhaustive scan of the unindexed
    /// tail, and real-pattern verification of every candidate. Matches
    /// order by `(inode_id, byte_offset)`. Two budgets bound a page — the
    /// match limit and a verified-candidate budget — and the cursor
    /// resumes strictly after the last candidate the page finished
    /// scanning, bound to the request that issued it. Each page is
    /// evaluated against the view it runs on and reports that head in
    /// `head_seq`.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.phase",
        err,
        skip_all,
        fields(phase = "grep")
    )]
    pub async fn query<S: ObjectStore + ?Sized>(
        &self,
        request: &GrepRequest,
        snapshot: &GrepIndexSnapshot,
        view: &LoadedMetadataView<'_, S>,
        store: &S,
    ) -> Result<GrepResponse> {
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
                if cursor.head_seq > view.head().seq {
                    return Err(CoreError::from(MetadataViewError::SnapshotUnavailable {
                        requested_seq: cursor.head_seq,
                        head_seq: view.head().seq,
                    })
                    .into());
                }
                Some((cursor.last_inode_id, cursor.last_byte_offset))
            }
            None => None,
        };

        let snapshot = snapshot.materialized()?;

        // Line-anchored semantics: `^` and `$` match line boundaries, the
        // grep-family contract. The planner parses with the same flags so
        // its gram analysis sees the pattern the verifier runs.
        let pattern = regex::bytes::RegexBuilder::new(&request.pattern)
            .case_insensitive(request.case_insensitive)
            .multi_line(true)
            .build()
            .map_err(|error| CoreError::InvalidQuery(error.to_string()))?;

        // One view session serves the whole page: candidates in the same
        // directory tree share ancestor bindings, tombstone checks, and
        // path verifications, so the per-candidate metadata walks below hit
        // the session caches instead of re-fetching per candidate.
        let mut session = view.grep_session();
        // The scope filter tests durable identity: resolve the prefix to
        // its inode once, then require it among each candidate's ancestors,
        // so name-policy folding and path normalization apply exactly as
        // they do to every other read. A prefix that resolves to nothing
        // has no matches.
        let scope_root = match &request.path_prefix {
            Some(prefix) => {
                match session
                    .resolve_visible_path(prefix, LeafRevisionPrefetch::Skip)
                    .await
                {
                    Ok(resolved) => Some(resolved.inode_id),
                    Err(error) if error.code() == ErrorCode::PathNotFound => {
                        return Ok(GrepResponse {
                            namespace_id: view.namespace_id().clone(),
                            head_seq: view.head().seq,
                            built_through_seq: snapshot.built_through_seq,
                            tail_scanned: true,
                            matches: Vec::new(),
                            next_cursor: None,
                        });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            None => None,
        };

        let mut candidates = GrepCandidates::default();
        let tail_resume = match plan_pattern(&request.pattern, request.case_insensitive)
            .map_err(CoreError::InvalidQuery)?
        {
            GramPlanOutcome::Indexable(plan) => {
                candidates.indexed =
                    indexed_candidates(store, &self.block_cache, &snapshot.segments, &plan).await?;
                ChangeFeedResume::new(snapshot.built_through_seq, snapshot.next_delta_index)
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
                candidates.unfiltered = scan_candidate_inodes(view).await?;
                ChangeFeedResume::new(view.grep_materialized_through_seq(), 0)
            }
        };

        let tail = tail_revisions(store, view, tail_resume).await?;
        let mut tail_scanned = true;
        if tail.len() > MAX_GREP_TAIL_FILES {
            if request.allow_stale {
                tail_scanned = false;
            } else {
                return Err(CoreError::IndexLagging {
                    behind_commits: view
                        .head()
                        .seq
                        .0
                        .saturating_sub(snapshot.built_through_seq.0),
                }
                .into());
            }
        } else {
            candidates.unfiltered.extend(tail.iter().copied());
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
        let ordered_candidates = candidates.inodes().collect::<Vec<_>>();
        let mut next_candidate = 0usize;
        'page: loop {
            // Select the next fan-out batch: walk candidates in inode
            // order through the cheap checks (metadata lookups served by
            // the loaded view) until enough survivors need content, the
            // verified-file budget fills, or the candidates run out. The
            // content read is the only per-candidate store fetch, so it is
            // the only stage that fans out — the design doc's "small fixed
            // fan-out" for candidate reads.
            let mut batch: Vec<GrepContentCandidate> = Vec::new();
            let mut budget_exhausted = false;
            while next_candidate < ordered_candidates.len() {
                let inode_id = ordered_candidates[next_candidate];
                if let Some((last_inode, last_offset)) = resume {
                    if inode_id < last_inode || (inode_id == last_inode && last_offset == u64::MAX)
                    {
                        next_candidate += 1;
                        continue;
                    }
                }
                // The examination budget bounds a page's metadata work the
                // way the verified budget bounds its content work: a scope
                // filter that rejects nearly every candidate would
                // otherwise walk metadata for the entire candidate set in
                // one page. The candidate at the boundary is left for the
                // next page.
                if examined_candidates == MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE {
                    budget_exhausted = true;
                    break;
                }
                next_candidate += 1;
                examined_candidates += 1;
                if session.visible_inode(inode_id).await?.is_none() {
                    rejected_frontier = Some(inode_id);
                    continue;
                }
                let Some(revision) = session.latest_revision_head_of_visible(inode_id).await?
                else {
                    rejected_frontier = Some(inode_id);
                    continue;
                };
                if !candidates.admits(inode_id, revision.revision_no) {
                    rejected_frontier = Some(inode_id);
                    continue;
                }
                // With the tail skipped (`allow_stale`), serve the index's
                // cut and nothing newer: a candidate whose newest revision
                // is past the watermark would otherwise be verified at an
                // unindexed revision while tail-only files stay invisible
                // — a mix of two snapshots rather than a stale-but-
                // consistent one.
                if !tail_scanned && tail.contains(&inode_id) {
                    rejected_frontier = Some(inode_id);
                    continue;
                }
                let Some(chain) = derive_visible_path(&mut session, inode_id).await? else {
                    rejected_frontier = Some(inode_id);
                    continue;
                };
                if let Some(scope_root) = scope_root {
                    if !chain.ancestors.contains(&scope_root) {
                        rejected_frontier = Some(inode_id);
                        continue;
                    }
                }
                if verified_files == MAX_GREP_VERIFIED_FILES_PER_PAGE {
                    budget_exhausted = true;
                    break;
                }
                verified_files += 1;
                // Content past the index eligibility cap can never be
                // indexable text, so tail and scan candidates skip their
                // doomed reads on the declared size alone — the same
                // pre-fetch check the index builder applies
                // (the `worker.rs` collection paths); index-supplied candidates
                // are under the cap by construction. The candidate still
                // rides the batch in inode order, so the budget and the
                // resume cursor advance exactly as if its bytes had been
                // fetched and refused.
                let oversized = revision.content_ref.size_bytes > INDEX_GRAMS_MAX_FILE_BYTES;
                batch.push(GrepContentCandidate {
                    inode_id,
                    revision,
                    path: chain.path,
                    oversized,
                });
                if batch.len() == MAX_GREP_CONTENT_IO {
                    break;
                }
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
            // page re-issues that read and reports it then. An oversized
            // candidate carries no read at all (`None`).
            let contents = join_all(batch.iter().map(|candidate| async move {
                if candidate.oversized {
                    return None;
                }
                Some(
                    read_durable_content_bytes(
                        store,
                        view.content_store_id(),
                        &candidate.revision.content_ref,
                    )
                    .await,
                )
            }))
            .await;
            // Emission stays strictly in candidate (inode) order: the batch
            // was selected in order and its results are consumed in order,
            // so matches, limits, errors, and the resume cursor advance
            // exactly as the serial walk advanced them.
            for (candidate, content) in batch.iter().zip(contents) {
                let inode_id = candidate.inode_id;
                let Some(content) = content else {
                    // Skipped as oversized: scanned-and-refused without the
                    // fetch, so the cursor moves past it like any other
                    // ineligible file.
                    resume_cursor = Some((inode_id, u64::MAX));
                    continue;
                };
                let content = content.map_err(CoreError::from)?;
                if !is_indexable_text_content(&content.bytes) {
                    resume_cursor = Some((inode_id, u64::MAX));
                    continue;
                }
                for found in line_matches(&content.bytes, &pattern) {
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
                        revision_no: candidate.revision.revision_no,
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
                    head_seq: view.head().seq,
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
            namespace_id: view.namespace_id().clone(),
            head_seq: view.head().seq,
            built_through_seq: snapshot.built_through_seq,
            tail_scanned,
            matches,
            next_cursor,
        })
    }
}

/// Every inode holding any revision in the manifest tables, for plan-less
/// scans; the WAL tail is collected separately. Refuses past the scan budget.
async fn scan_candidate_inodes<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
) -> Result<BTreeSet<InodeId>> {
    let mut inodes = BTreeSet::new();
    let mut lower = REVISION_ROW_PREFIX.to_owned();
    let upper = string_prefix_upper_bound(REVISION_ROW_PREFIX);
    loop {
        let page = view
            .grep_revision_inode_page(
                &lower,
                upper.as_deref(),
                MAX_GREP_SCAN_FILES.saturating_add(1),
            )
            .await?;
        let Some((last_key, _)) = page.last() else {
            break;
        };
        let last_key = last_key.clone();
        let exhausted = page.len() <= MAX_GREP_SCAN_FILES;
        inodes.extend(page.into_iter().map(|(_, inode_id)| inode_id));
        if inodes.len() > MAX_GREP_SCAN_FILES {
            return Err(CoreError::QueryUnindexable(format!(
                "the namespace exceeds the {MAX_GREP_SCAN_FILES}-file scan budget; \
                 give the pattern a run of at least 3 literal bytes so the \
                 trigram index can narrow candidates"
            ))
            .into());
        }
        if exhausted {
            break;
        }
        lower = format!("{last_key}\0");
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
}
