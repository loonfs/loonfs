//! The candidates a grep query examines: the content values the index
//! matched, the files of the unindexed tail, and the files of the plan-less
//! scan.

use super::{
    GREP_READS_IN_FLIGHT, MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES,
    MAX_GREP_VERIFIED_VALUES_PER_PAGE, SCAN_DIRECTORY_PAGE_ENTRIES, TAIL_FEED_PAGE_COMMITS,
};
use crate::codec::{lookup, ContentValue, Gram, IndexRow};
use crate::entry::ChangeFeedResume;
use crate::index_read::index_segment_corrupt;
use crate::query::GramQueryPlan;
use crate::reads::{published_revision, NamespaceReadView};
use crate::segments::GrepSegments;
use crate::{GrepError, Result};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{ExtensionRunRef, ExtensionSegmentRef};
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{
    EntryInodeKind, ErrorCode, InodeId, PathEntry, PathEntryKind, RevisionNo, RunNo, ROOT_INODE_ID,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// The files that the index doesn't decide for a query, grouped by the
/// content value of each file's revision at the read sequence.
pub(super) type CandidateFiles = BTreeMap<ContentValue, BTreeMap<InodeId, RevisionNo>>;

/// The candidates a query examines, in content order.
#[derive(Debug, Default)]
pub(super) struct GrepCandidates {
    /// The ordinals, in each run, of the content values whose bytes contain
    /// every required gram, a superset of the values that match. A page maps
    /// the ordinals that it walks to values, and finds the values' files in
    /// the index's `content_revisions` rows.
    pub(super) indexed: Vec<RunCandidates>,
    /// Files from the unindexed tail or the plan-less scan, each with its
    /// revision at the read sequence. A file of an indexed value joins that
    /// value's files from the index.
    pub(super) files: CandidateFiles,
    /// Tail files whose values the index has examined and found eligible. A
    /// file here joins the walk only if the index produces its value.
    pub(super) index_decided: CandidateFiles,
}

/// The candidate ordinals of one run, in ascending order, which is content
/// order.
#[derive(Debug)]
pub(super) struct RunCandidates {
    segments: Vec<ExtensionSegmentRef>,
    ordinals: Vec<u64>,
}

/// Ordinals of one run that one read of its value table maps. A page
/// verifies at most this many values, so it usually maps one batch of each
/// run.
const VALUES_PER_TABLE_READ: usize = MAX_GREP_VERIFIED_VALUES_PER_PAGE;

/// One value that a page walks, and whether the index produced it.
#[derive(Debug, Clone, Copy)]
pub(super) struct CandidateValue {
    pub(super) value: ContentValue,
    pub(super) indexed: bool,
}

/// The position of a walk in the candidate values of a query, which are in
/// content order.
///
/// Each run's ordinals map to values a batch at a time, as the walk reaches
/// them, so a page reads the table rows of the values it walks rather than
/// those of every candidate. The runs' values combine with the values of the
/// tail and scan files in content order, and a value that two runs produce
/// is walked once. The position refers to its candidates by index, so a
/// stream can own it.
pub(super) struct CandidateValues {
    /// One position for each run of `GrepCandidates::indexed`, in order.
    runs: Vec<RunValues>,
    /// The values of the candidates' `files` and `index_decided` at or after
    /// the resume point, in content order.
    files: VecDeque<ContentValue>,
}

struct RunValues {
    /// The position in the run's ordinals of the next ordinal to map.
    next: usize,
    /// Mapped values that the walk hasn't reached, in content order.
    mapped: VecDeque<ContentValue>,
}

impl CandidateValues {
    /// Returns the position of the values of `candidates` at or after
    /// `from`, or at the start when `from` is `None`.
    pub(super) async fn start<S: ObjectStore + ?Sized>(
        index: &GrepSegments<'_, S>,
        candidates: &GrepCandidates,
        from: Option<&ContentValue>,
    ) -> Result<Self> {
        let starts: Vec<usize> = match from {
            None => vec![0; candidates.indexed.len()],
            Some(from) => {
                stream::iter(0..candidates.indexed.len())
                    .map(|position| index.first_at_or_after(&candidates.indexed[position], from))
                    .buffered(GREP_READS_IN_FLIGHT)
                    .try_collect()
                    .await?
            }
        };
        let files: BTreeSet<&ContentValue> = candidates
            .files
            .keys()
            .chain(candidates.index_decided.keys())
            .filter(|value| from.is_none_or(|from| *value >= from))
            .collect();
        Ok(Self {
            runs: starts
                .into_iter()
                .map(|next| RunValues {
                    next,
                    mapped: VecDeque::new(),
                })
                .collect(),
            files: files.into_iter().copied().collect(),
        })
    }

    /// Returns the next value of `candidates`, the candidates that the walk
    /// started from, in content order, or `None` after the last.
    pub(super) async fn next<S: ObjectStore + ?Sized>(
        &mut self,
        index: &GrepSegments<'_, S>,
        candidates: &GrepCandidates,
    ) -> Result<Option<CandidateValue>> {
        loop {
            self.map_next_batches(index, candidates).await?;
            let Some(value) = self
                .runs
                .iter()
                .filter_map(|run| run.mapped.front())
                .chain(self.files.front())
                .min()
                .copied()
            else {
                return Ok(None);
            };
            let mut indexed = false;
            for run in &mut self.runs {
                if run.mapped.front() == Some(&value) {
                    run.mapped.pop_front();
                    indexed = true;
                }
            }
            if self.files.front() == Some(&value) {
                self.files.pop_front();
            }
            if indexed || candidates.files.contains_key(&value) {
                return Ok(Some(CandidateValue { value, indexed }));
            }
        }
    }

    /// Maps the next batch of ordinals of each run whose mapped values the
    /// walk has passed, with the runs' table reads in flight together.
    async fn map_next_batches<S: ObjectStore + ?Sized>(
        &mut self,
        index: &GrepSegments<'_, S>,
        candidates: &GrepCandidates,
    ) -> Result<()> {
        let batches: Vec<(usize, &RunCandidates, BTreeSet<u64>)> = self
            .runs
            .iter()
            .zip(&candidates.indexed)
            .enumerate()
            .filter(|(_, (values, run))| {
                values.mapped.is_empty() && values.next < run.ordinals.len()
            })
            .map(|(position, (values, run))| {
                let end = (values.next + VALUES_PER_TABLE_READ).min(run.ordinals.len());
                (
                    position,
                    run,
                    run.ordinals[values.next..end].iter().copied().collect(),
                )
            })
            .collect();
        let mapped: Vec<BTreeMap<u64, ContentValue>> = stream::iter(0..batches.len())
            .map(|batch| index.table_values(&batches[batch].1.segments, &batches[batch].2))
            .buffered(GREP_READS_IN_FLIGHT)
            .try_collect()
            .await?;
        for ((position, _, ordinals), values) in batches.into_iter().zip(mapped) {
            let run = &mut self.runs[position];
            run.next += ordinals.len();
            run.mapped.extend(values.into_values());
        }
        Ok(())
    }
}

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Evaluates the gram plan against all index segments, and returns the
    /// content values that contain every required gram.
    ///
    /// Each run evaluates the plan on its own value ordinals: for each
    /// required set, postings for alternative grams are unioned, and the
    /// results of required sets are intersected. Independent probes run with
    /// at most [`GREP_READS_IN_FLIGHT`] in flight. Evaluation stops when the
    /// intersections are empty or small enough together to verify within the
    /// page budget. Stopping early may widen the candidate set, but final
    /// byte-level pattern verification preserves exact results.
    pub(super) async fn indexed_candidates(
        &self,
        runs: &[ExtensionRunRef],
        plan: &GramQueryPlan,
    ) -> Result<Vec<RunCandidates>> {
        let mut intersections: Option<BTreeMap<RunNo, BTreeSet<u64>>> = None;
        for or_set in &plan.required {
            let lookups: Vec<GramLookup> =
                or_set.iter().map(|gram| GramLookup::new(*gram)).collect();
            let probes: Vec<(&GramLookup, RunNo, &ExtensionSegmentRef)> = lookups
                .iter()
                .flat_map(|gram_lookup| {
                    runs.iter()
                        .filter(|run| {
                            intersections
                                .as_ref()
                                .is_none_or(|found| found.contains_key(&run.run_no))
                        })
                        .flat_map(move |run| {
                            run.segments
                                .iter()
                                .map(move |descriptor| (gram_lookup, run.run_no, descriptor))
                        })
                })
                .collect();
            // This union is the query's largest temporary allocation. In the worst
            // case it contains one ordinal per indexed value of each run. A
            // streaming intersection would reduce memory but add probe-ordering
            // complexity.
            let mut set_postings: BTreeMap<RunNo, BTreeSet<u64>> = BTreeMap::new();
            let mut batches = stream::iter(0..probes.len())
                .map(|index| {
                    let (gram_lookup, run_no, descriptor) = probes[index];
                    self.postings_for_gram(descriptor, gram_lookup)
                        .map_ok(move |batch| (run_no, batch))
                })
                .buffered(GREP_READS_IN_FLIGHT);
            while let Some((run_no, batch)) = batches.try_next().await? {
                set_postings.entry(run_no).or_default().extend(batch);
            }
            let mut next = match intersections {
                None => set_postings,
                Some(current) => current
                    .into_iter()
                    .filter_map(|(run_no, ordinals)| {
                        let found = set_postings.get(&run_no)?;
                        Some((run_no, ordinals.intersection(found).copied().collect()))
                    })
                    .collect(),
            };
            next.retain(|_, ordinals: &mut BTreeSet<u64>| !ordinals.is_empty());
            let candidates: usize = next.values().map(BTreeSet::len).sum();
            intersections = Some(next);
            // The budget stop: the intersections contain one ordinal per content
            // value of a run, an upper bound on the values a page could ever
            // verify from them, so once they fit the per-page verification
            // budget the remaining AND sets are constraints the page cannot
            // use.
            if candidates <= MAX_GREP_VERIFIED_VALUES_PER_PAGE {
                break;
            }
        }
        Ok(intersections
            .unwrap_or_default()
            .into_iter()
            .map(|(run_no, ordinals)| RunCandidates {
                segments: runs
                    .iter()
                    .find(|run| run.run_no == run_no)
                    .expect("a run with candidates should be one of the index's runs")
                    .segments
                    .clone(),
                ordinals: ordinals.into_iter().collect(),
            })
            .collect())
    }

    /// Returns the position in `run`'s ordinals of the first one whose value
    /// is at or after `from`, by a binary search over single reads of the
    /// run's value table, because ordinal order is content order.
    async fn first_at_or_after(&self, run: &RunCandidates, from: &ContentValue) -> Result<usize> {
        let (mut low, mut high) = (0, run.ordinals.len());
        while low < high {
            let middle = low + (high - low) / 2;
            let ordinal = run.ordinals[middle];
            let values = self
                .table_values(&run.segments, &BTreeSet::from([ordinal]))
                .await?;
            if values[&ordinal] < *from {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    /// One gram's posting ordinals from one segment, read from the data
    /// blocks that the segment's index lists for the gram's key range once
    /// its filter admits the gram.
    async fn postings_for_gram(
        &self,
        descriptor: &ExtensionSegmentRef,
        gram_lookup: &GramLookup,
    ) -> Result<BTreeSet<u64>> {
        let range = self
            .rows_in_range(
                descriptor,
                Some(&gram_lookup.probe),
                &gram_lookup.prefix,
                gram_lookup.upper.as_deref(),
                usize::MAX,
            )
            .await?;
        let mut postings = BTreeSet::new();
        for (_, row) in range.rows {
            let IndexRow::GramPostings(row) = row else {
                continue;
            };
            if row.gram != gram_lookup.gram {
                continue;
            }
            let batch = row.ordinals().map_err(|error| {
                index_segment_corrupt(&self.object_key(descriptor), "posting batch", &error)
            })?;
            postings.extend(batch);
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

/// The revision of one file in the unindexed tail, at the read sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TailRevision {
    pub(super) revision_no: RevisionNo,
    pub(super) value: ContentValue,
}

/// The files whose content changed after the index cursor: the unindexed
/// tail a query scans exhaustively, with each file's newest revision. A
/// cursor inside its watermark commit includes only that commit's remaining
/// event suffix.
///
/// Paging stops as soon as the tail exceeds the query's tail budget: past
/// that the query either fails as lagging or serves the index's cut, and
/// neither needs the rest of the feed enumerated.
pub(super) async fn tail_revisions(
    reads: &NamespaceReadView<'_>,
    resume: ChangeFeedResume,
) -> Result<TailScan> {
    let mut files = BTreeMap::new();
    let mut after_seq = resume.after_seq();
    if after_seq >= reads.head_seq() {
        return Ok(TailScan::Within(files));
    }
    loop {
        let feed = reads
            .list_changes_after(after_seq, TAIL_FEED_PAGE_COMMITS)
            .await?;
        for change in &feed.changes {
            for (_, event) in resume.events_after(change)? {
                if let Some(revision) = published_revision(event) {
                    files.insert(
                        revision.inode_id,
                        TailRevision {
                            revision_no: revision.revision_no,
                            value: ContentValue::of(revision.content_ref),
                        },
                    );
                }
            }
        }
        if files.len() > MAX_GREP_TAIL_FILES {
            return Ok(TailScan::OverBudget);
        }
        match feed.next_after_seq {
            Some(next_after_seq) => after_seq = next_after_seq,
            None => return Ok(TailScan::Within(files)),
        }
    }
}

/// The unindexed tail, or the fact that it is larger than one query scans.
pub(super) enum TailScan {
    /// Every file whose content changed after the index watermark, with its
    /// newest revision.
    Within(BTreeMap<InodeId, TailRevision>),
    /// More files changed after the watermark than the tail budget allows;
    /// enumeration stopped there, so no set is returned.
    OverBudget,
}

/// Adds the files of the unindexed tail to `candidates`, except the files
/// whose values the index has already examined and the query can skip.
///
/// A tail value with a `texts` row is either ineligible, so its files can't
/// match, or indexed, so the posting intersection decides whether the value
/// is a candidate, and its files wait in `index_decided` for the walk to
/// find the value among the index's candidates. Only the values without a
/// row are read and verified whatever their grams are.
pub(super) async fn add_tail_files<S: ObjectStore + ?Sized>(
    segments: &GrepSegments<'_, S>,
    runs: &[ExtensionRunRef],
    candidates: &mut GrepCandidates,
    tail: BTreeMap<InodeId, TailRevision>,
) -> Result<()> {
    let values: BTreeSet<ContentValue> = tail.values().map(|revision| revision.value).collect();
    let descriptors: Vec<ExtensionSegmentRef> = runs
        .iter()
        .flat_map(|run| run.segments.iter().cloned())
        .collect();
    let texts = segments
        .text_rows(&descriptors, &values, GREP_READS_IN_FLIGHT)
        .await?;
    for (inode_id, revision) in tail {
        let files = match texts.get(&revision.value) {
            Some(true) => &mut candidates.index_decided,
            Some(false) => continue,
            None => &mut candidates.files,
        };
        files
            .entry(revision.value)
            .or_default()
            .insert(inode_id, revision.revision_no);
    }
    Ok(())
}

/// Collects the visible files in the query scope for a full scan, grouped
/// by the content value of each file's current revision.
///
/// The bounded walk starts at the scope's inode, or at the root inode when
/// the request names no scope. It lists directories by inode because a
/// derived path may exceed the request path limits. The directory limit
/// bounds the work and prevents a binding cycle from running forever. Then
/// the scan reads each file's entry, with at most [`GREP_READS_IN_FLIGHT`] in
/// flight, for its content value.
pub(super) async fn scan_candidate_files(
    reads: &NamespaceReadView<'_>,
    scope: Option<&PathEntry>,
) -> Result<CandidateFiles> {
    let inodes = scan_candidate_inodes(reads, scope).await?;
    let mut files = CandidateFiles::new();
    let mut entries = stream::iter(inodes)
        .map(|inode_id| async move {
            match reads.resolve_inode(inode_id).await {
                Ok(entry) => Ok(Some(entry)),
                Err(error)
                    if matches!(
                        error.code(),
                        ErrorCode::InodeNotFound | ErrorCode::Forbidden
                    ) =>
                {
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        })
        .buffered(GREP_READS_IN_FLIGHT);
    while let Some(entry) = entries.try_next().await? {
        let Some(entry) = entry else {
            continue;
        };
        let PathEntryKind::File {
            revision_no,
            content_ref: Some(content_ref),
            ..
        } = &entry.kind
        else {
            continue;
        };
        files
            .entry(ContentValue::of(content_ref))
            .or_default()
            .insert(entry.inode_id, *revision_no);
    }
    Ok(files)
}

/// Walks the scope's directories for the inodes of its visible files.
async fn scan_candidate_inodes(
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
