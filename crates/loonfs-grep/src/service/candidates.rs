//! The candidates a grep query examines: index postings, the unindexed
//! tail, and the plan-less scan.

use super::{
    GREP_READS_IN_FLIGHT, MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES,
    MAX_GREP_VERIFIED_FILES_PER_PAGE, SCAN_DIRECTORY_PAGE_ENTRIES, TAIL_FEED_PAGE_COMMITS,
};
use crate::codec::{lookup, Gram, IndexRow};
use crate::index_read::{
    index_segment_corrupt, load_data_block, load_filter_block, load_index_block,
};
use crate::manifest::{ChangeFeedResume, GrepSegmentRef};
use crate::query::GramQueryPlan;
use crate::reads::{published_revision, NamespaceReadView};
use crate::segments::GrepSegments;
use crate::{GrepError, Result};
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::hex::hex_decode_bytes;
use loonfs_types::format::segment_blocks::{
    decode_filter_block, index_blocks_for_key_range, key_range_may_intersect,
    string_prefix_upper_bound,
};
use loonfs_types::{EntryInodeKind, ErrorCode, InodeId, PathEntry, RevisionNo, ROOT_INODE_ID};
use std::collections::{BTreeMap, BTreeSet};

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

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Evaluates the gram plan against all index segments.
    ///
    /// For each required set, postings for alternative grams are unioned; the
    /// results of required sets are intersected. Independent probes run with
    /// at most [`GREP_READS_IN_FLIGHT`] in flight. Evaluation stops when the
    /// intersection is empty or small enough to verify within the page budget.
    /// Stopping early may widen the candidate set, but final byte-level
    /// pattern verification preserves exact results.
    pub(super) async fn indexed_candidates(
        &self,
        segments: &[GrepSegmentRef],
        plan: &GramQueryPlan,
    ) -> Result<BTreeMap<InodeId, BTreeSet<RevisionNo>>> {
        let mut intersection: Option<BTreeSet<(InodeId, RevisionNo)>> = None;
        for or_set in &plan.required {
            // Lookup keys derive once per gram; the key-range prune is free
            // (already in the descriptor), so only surviving probes fan out.
            let lookups: Vec<GramLookup> =
                or_set.iter().map(|gram| GramLookup::new(*gram)).collect();
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
            // case it contains one `(inode, revision)` pair per indexed revision. A
            // streaming intersection would reduce memory but add probe-ordering
            // complexity.
            let mut set_postings = BTreeSet::new();
            let mut batches = stream::iter(0..probes.len())
                .map(|index| {
                    let (gram_lookup, descriptor) = probes[index];
                    self.postings_for_gram(descriptor, gram_lookup)
                })
                .buffered(GREP_READS_IN_FLIGHT);
            while let Some(batch) = batches.try_next().await? {
                set_postings.extend(batch);
            }
            intersection = Some(match intersection {
                None => set_postings,
                Some(current) => current.intersection(&set_postings).copied().collect(),
            });
            if intersection.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
            // The budget stop: the intersection contains (inode, revision) pairs,
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

    /// One gram's postings from one segment: bloom filter first (the inline
    /// copy when the descriptor has one — no fetch at all — otherwise the
    /// cached filter block), then only the data blocks the segment index
    /// names for the gram's key range, loaded with at most
    /// [`GREP_READS_IN_FLIGHT`] in flight. Every fetched section resolves
    /// through the grep-private decoded-block cache.
    async fn postings_for_gram(
        &self,
        descriptor: &GrepSegmentRef,
        gram_lookup: &GramLookup,
    ) -> Result<BTreeSet<(InodeId, RevisionNo)>> {
        let object_key = self.object_key(&descriptor.segment_id);
        let mut postings = BTreeSet::new();
        let admitted = match &descriptor.filter_inline {
            Some(inline) => {
                let filter_bytes =
                    hex_decode_bytes(inline).map_err(|error| GrepError::IndexCorrupt {
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
                let filter =
                    load_filter_block(self.store, self.block_cache, &object_key, descriptor)
                        .await?;
                filter.may_contain(&gram_lookup.probe)
            }
        };
        if !admitted {
            return Ok(postings);
        }
        let entries =
            load_index_block(self.store, self.block_cache, &object_key, descriptor).await?;
        let range =
            index_blocks_for_key_range(&entries, &gram_lookup.prefix, gram_lookup.upper.as_deref());
        // The range's blocks are independent ranged GETs, so they fan out
        // instead of paying one round trip each; `buffered` returns them
        // in entry order and rows fold in that order, so assembly stays
        // deterministic even though the posting union is order-independent.
        let mut blocks = stream::iter(range)
            .map(|index| {
                load_data_block(
                    self.store,
                    self.block_cache,
                    &object_key,
                    &descriptor.segment_id,
                    &entries[index].block,
                )
            })
            .buffered(GREP_READS_IN_FLIGHT);
        while let Some(block) = blocks.try_next().await? {
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
        Ok(postings)
    }
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

/// The files whose content changed after the index cursor: the unindexed
/// tail a query scans exhaustively. A cursor inside its watermark commit
/// includes only that commit's remaining event suffix.
///
/// Paging stops as soon as the tail exceeds the query's tail budget: past
/// that the query either fails as lagging or serves the index's cut, and
/// neither needs the rest of the feed enumerated.
pub(super) async fn tail_revisions(
    reads: &NamespaceReadView<'_>,
    resume: ChangeFeedResume,
) -> Result<TailScan> {
    let mut inodes = BTreeSet::new();
    let mut after_seq = resume.after_seq();
    loop {
        let feed = reads
            .list_changes_after(after_seq, TAIL_FEED_PAGE_COMMITS)
            .await?;
        for change in &feed.changes {
            let start_event_index = resume.start_event_index(change.committed_seq);
            let events = &change.events;
            if start_event_index > events.len() {
                let next_event_index = resume.next_event_index();
                return Err(GrepError::IndexCorrupt {
                    message: format!(
                        "grep event cursor `{next_event_index}` exceeds commit `{}` length `{}`",
                        change.committed_seq,
                        events.len()
                    ),
                });
            }
            for event in events.iter().skip(start_event_index) {
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
pub(super) enum TailScan {
    /// Every file whose content changed after the index watermark.
    Within(BTreeSet<InodeId>),
    /// More files changed after the watermark than the tail budget allows;
    /// enumeration stopped there, so no set is returned.
    OverBudget,
}

/// Collects visible files in the query scope for a full scan.
///
/// The bounded walk starts at the scope's inode, or at the root inode when
/// the request names no scope. It lists directories by inode because a
/// derived path may exceed the request path limits. The directory limit
/// bounds the work and prevents a binding cycle from running forever.
pub(super) async fn scan_candidate_inodes(
    reads: &NamespaceReadView<'_>,
    scope: Option<&PathEntry>,
) -> Result<BTreeSet<InodeId>> {
    let mut inodes = BTreeSet::new();
    let root = match scope {
        Some(entry) if entry.inode_kind() == EntryInodeKind::File => {
            // A file scope contains only that file.
            inodes.insert(entry.inode_id);
            return Ok(inodes);
        }
        Some(entry) => entry.inode_id,
        None => ROOT_INODE_ID,
    };
    let mut directories = vec![root];
    let mut walked_directories = 0usize;
    while let Some(directory) = directories.pop() {
        walked_directories += 1;
        let mut cursor = None;
        loop {
            let page = match reads
                .list_inode_children_page(directory, cursor, SCAN_DIRECTORY_PAGE_ENTRIES)
                .await
            {
                Ok(page) => page,
                Err(error)
                    if matches!(
                        error.code(),
                        ErrorCode::InodeNotFound | ErrorCode::Forbidden
                    ) =>
                {
                    break;
                }
                Err(error) => return Err(error),
            };
            for entry in page.items {
                match entry.inode_kind() {
                    EntryInodeKind::Directory => directories.push(entry.inode_id),
                    EntryInodeKind::File => {
                        inodes.insert(entry.inode_id);
                    }
                    EntryInodeKind::Unknown => {}
                }
            }
            if inodes.len() + directories.len() > MAX_GREP_SCAN_FILES
                || walked_directories > MAX_GREP_SCAN_FILES
            {
                return Err(GrepError::QueryUnindexable(format!(
                    "the namespace exceeds the {MAX_GREP_SCAN_FILES}-file scan budget; \
                     give the pattern a run of at least 3 literal bytes so the \
                     trigram index can narrow candidates"
                )));
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(inodes)
}
