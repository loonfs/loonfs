//! Content-search machinery: gram posting reads over index segments, the
//! exhaustive-scan tail, and line-oriented match extraction. The
//! orchestrating `grep` read lives on `LoadedMetadataView`
//! (`materialized_view.rs`); this module holds the pieces that need no
//! access to the view's internals.

use crate::checkpoint::{
    index_segment_corrupt, load_index_segment_data_block, load_index_segment_filter_block,
    load_index_segment_index_block, string_prefix_upper_bound, CacheAdmission, MetadataTableCache,
};
use crate::error::{CoreError, Result};
use crate::query::grep_plan::GramQueryPlan;
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use futures::future::try_join_all;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::index_grams::{lookup, Gram, IndexRow, INDEX_FAMILY_GRAMS};
use loonfs_api::wire::manifest::{hex_decode_bytes, IndexFileRef};
use loonfs_api::wire::sst_blocks::{decode_filter_block, index_blocks_for_key_range};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{ChangeSeq, ContentRef, InodeId, NamespaceId, RevisionNo};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

/// Matches returned per page when the request names no limit.
pub const DEFAULT_GREP_PAGE_LIMIT: usize = 100;
/// Largest per-page match limit a request may name.
pub const MAX_GREP_PAGE_LIMIT: usize = 1000;
/// Unindexed-tail revisions one query will scan exhaustively before
/// failing with `index_lagging` (or skipping the tail under `allow_stale`).
pub(super) const MAX_GREP_TAIL_FILES: usize = 512;
/// Files a plan-less `allow_scan` query will scan before refusing.
pub(super) const MAX_GREP_SCAN_FILES: usize = 4096;
/// Longest match line returned, in bytes; longer lines are truncated.
pub(super) const GREP_LINE_CAP_BYTES: usize = 512;
/// Candidate files one page will read and verify before returning with a
/// resume cursor, so a page's cost is bounded by its own budget rather
/// than by how many false-positive candidates the plan admits.
pub(super) const MAX_GREP_VERIFIED_FILES_PER_PAGE: usize = 256;
/// Concurrent gram posting probes one grep query issues at a time: the
/// (gram, segment) probes of an OR-set fan out in chunks of this size,
/// each probe a handful of small ranged GETs (filter, index, and posting
/// blocks). Deliberately below [`MAX_GREP_CONTENT_IO`]: probes multiply
/// into many small requests per chunk, where a content read is one whole
/// object. The read-side sibling of the maintenance path's
/// `MAX_MAINTENANCE_TABLE_IO` (`checkpoint/runs.rs`), which stays at its
/// own value.
pub(super) const MAX_GREP_READ_IO: usize = 16;
/// Concurrent candidate content reads one grep query issues at a time.
/// Content verification is a whole-object GET of a small file (index
/// eligibility caps candidates at `INDEX_GRAMS_MAX_FILE_BYTES`), so it
/// tolerates a wider fan-out than the posting probes: 32 matches the
/// content-object concurrency the ecosystem already runs against these
/// stores (bulk content uploads fan out 32 wide). Each batch is
/// additionally bounded by the remaining
/// [`MAX_GREP_VERIFIED_FILES_PER_PAGE`] budget, so a page issues at most
/// that many content reads however wide the batches are.
pub(super) const MAX_GREP_CONTENT_IO: usize = 32;

/// The revisions a query must examine, keyed by durable inode identity.
#[derive(Debug, Default)]
pub(super) struct GrepCandidates {
    /// Index-supplied candidates: the revisions whose content contained
    /// every required gram. A candidate survives only if the inode's
    /// newest visible revision is in its set.
    pub(super) indexed: BTreeMap<InodeId, BTreeSet<RevisionNo>>,
    /// Tail- or scan-supplied candidates: examined whatever their newest
    /// visible revision is, because no gram filter applies to them.
    pub(super) unfiltered: BTreeSet<InodeId>,
}

impl GrepCandidates {
    pub(super) fn inodes(&self) -> impl Iterator<Item = InodeId> + '_ {
        let mut merged: BTreeSet<InodeId> = self.indexed.keys().copied().collect();
        merged.extend(self.unfiltered.iter().copied());
        merged.into_iter()
    }

    /// Whether the inode's newest visible revision should be verified.
    pub(super) fn admits(&self, inode_id: InodeId, revision_no: RevisionNo) -> bool {
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
pub(super) async fn indexed_candidates<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    segments: &[IndexFileRef],
    plan: &GramQueryPlan,
) -> Result<BTreeMap<InodeId, BTreeSet<RevisionNo>>> {
    let mut intersection: Option<BTreeSet<(InodeId, RevisionNo)>> = None;
    for or_set in &plan.required {
        // Lookup keys derive once per gram; the key-range prune is free
        // (already in the descriptor), so only surviving probes fan out.
        let lookups: Vec<GramLookup> = or_set.iter().map(|gram| GramLookup::new(*gram)).collect();
        let mut probes: Vec<(&GramLookup, &IndexFileRef)> = Vec::new();
        for gram_lookup in &lookups {
            for descriptor in segments {
                if descriptor.family != INDEX_FAMILY_GRAMS {
                    continue;
                }
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
        let mut set_postings = BTreeSet::new();
        for chunk in probes.chunks(MAX_GREP_READ_IO) {
            let batches = try_join_all(chunk.iter().map(|(gram_lookup, descriptor)| {
                segment_postings_for_gram(store, table_cache, descriptor, gram_lookup)
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
/// names for the gram's key range. Every fetched section resolves through
/// the shared decoded-block cache when one is attached.
async fn segment_postings_for_gram<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    descriptor: &IndexFileRef,
    gram_lookup: &GramLookup,
) -> Result<BTreeSet<(InodeId, RevisionNo)>> {
    let mut postings = BTreeSet::new();
    let admitted = match &descriptor.filter_inline {
        Some(inline) => {
            let filter_bytes = hex_decode_bytes(inline).map_err(|error| {
                CoreError::NamespaceCorrupt(format!(
                    "index segment `{}` carries undecodable inline filter hex: {error}",
                    descriptor.object_key
                ))
            })?;
            let filter =
                decode_filter_block(&filter_bytes, &descriptor.filter_block).map_err(|error| {
                    index_segment_corrupt(&descriptor.object_key, "filter block", &error)
                })?;
            filter.may_contain(&gram_lookup.probe)
        }
        None => {
            let filter = load_index_segment_filter_block(
                store,
                table_cache,
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
    let entries = load_index_segment_index_block(
        store,
        table_cache,
        &descriptor.object_key,
        &descriptor.payload_checksum,
        &descriptor.index_block,
    )
    .await?;
    let range =
        index_blocks_for_key_range(&entries, &gram_lookup.prefix, gram_lookup.upper.as_deref());
    for entry in &entries[range] {
        let block = load_index_segment_data_block(
            store,
            table_cache,
            CacheAdmission::Admit,
            &descriptor.object_key,
            &descriptor.payload_checksum,
            &entry.block,
        )
        .await?;
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
    Ok(postings)
}

/// The file revisions committed after the index watermark, newest revision
/// per inode, from WAL replay — the exhaustive-scan tail.
pub(super) async fn tail_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    head: &HeadState,
    floor_seq: ChangeSeq,
    built_through_seq: ChangeSeq,
) -> Result<BTreeMap<InodeId, (RevisionNo, ContentRef)>> {
    let mut tail = BTreeMap::new();
    if built_through_seq >= head.seq {
        return Ok(tail);
    }
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: floor_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: Some(built_through_seq),
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        crate::error::CoreError::MetadataProjection(
            crate::error::MetadataProjectionLoadError::WalChainLoad(error),
        )
    })?;
    for segment in wal_chain.segments() {
        for record in segment.records() {
            if record.seq <= built_through_seq {
                continue;
            }
            for delta in &record.deltas {
                if let WalDelta::AppendFileRevision {
                    inode_id,
                    revision_no,
                    content_ref,
                    ..
                } = &delta.delta
                {
                    tail.insert(*inode_id, (*revision_no, content_ref.clone()));
                }
            }
        }
    }
    Ok(tail)
}

/// One verified line match inside a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LineMatch {
    pub(super) line_number: u64,
    pub(super) byte_offset: u64,
    pub(super) line: String,
    pub(super) line_truncated: bool,
}

/// Runs the pattern over content, one match per line, in offset order.
/// One forward pass: matches arrive in offset order, so the current line's
/// bounds and number advance monotonically — a file full of matches costs
/// one scan of its bytes, never a rescan per match.
pub(super) fn line_matches(content: &[u8], pattern: &regex::bytes::Regex) -> Vec<LineMatch> {
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
/// view's sequence.
pub(super) async fn derive_visible_path<S: ObjectStore + ?Sized>(
    view: &crate::metadata::MetadataView<'_, '_, S>,
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
        let Some(binding) = view.current_parent_binding_for_child(current).await? else {
            return Ok(None);
        };
        segments.push(binding.display_name.clone());
        current = binding.parent_inode_id;
        ancestors.push(current);
    }
    segments.reverse();
    let path = format!("/{}", segments.join("/"));
    let parsed = match crate::path::helpers::parse_absolute_path_for_core(&path) {
        Ok(parsed) => parsed,
        // A display name the path grammar rejects cannot be served as a
        // path result; the file is unreachable by path and skipped.
        Err(_) => return Ok(None),
    };
    match view.resolve_visible_path(&parsed).await {
        Ok(resolved) if resolved.inode_id == inode_id => {
            Ok(Some(VisiblePathChain { path, ancestors }))
        }
        Ok(_) => Ok(None),
        Err(error) if error.code() == loonfs_api::ErrorCode::PathNotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// A derived visible path plus the inode chain that produced it, root
/// included.
pub(super) struct VisiblePathChain {
    pub(super) path: String,
    pub(super) ancestors: Vec<InodeId>,
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
    fn line_matches_truncate_long_lines() {
        let mut content = vec![b'x'; GREP_LINE_CAP_BYTES + 64];
        content.extend_from_slice(b"needle");
        let matches = line_matches(&content, &pattern("needle"));
        assert_eq!(matches.len(), 1);
        assert!(matches[0].line_truncated);
        assert_eq!(matches[0].line.len(), GREP_LINE_CAP_BYTES);
    }

    #[test]
    fn matches_on_the_last_unterminated_line_are_reported() {
        let matches = line_matches(b"one\ntwo needle", &pattern("needle"));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].line_number, 2);
        assert_eq!(matches[0].line, "two needle");
    }
}
