//! Verified row scans over a loaded manifest's segments, with per-segment
//! caching and prefix-window pruning.

use super::cache::MetadataSegmentCache;
use super::compaction_merge::{refill_iterators, SegmentRowIterator};
use super::error::ManifestLoadError;
use super::load::{load_segment_filter, SessionBlockMemo};
use super::runs::{MetadataFamilySegments, MetadataRunManifest, MANIFEST_ROW_FAMILIES};
use super::scan_load::{ScanDescriptor, ScanLoader};
use crate::in_flight::STORE_READS_IN_FLIGHT;
#[cfg(test)]
use crate::metadata::MetadataState;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    MetadataRow, MetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope,
};
use loonfs_types::format::segment_blocks::{key_range_may_intersect, string_prefix_upper_bound};
use loonfs_types::ChangeSeq;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Pending segments a bounded scan opens together when it has none open. It
/// opens every segment that starts no later than the sixteenth, so their
/// first blocks load in one refill.
const SCAN_ITERATORS_OPEN: usize = 16;

/// Whether a scan reads ahead when it misses a data block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Readahead {
    /// The scan fetches a read-ahead window from each data block that it
    /// misses, and the read's memo keeps the window's other blocks as stored
    /// bytes until the scan reaches them.
    Enabled,
    /// The scan fetches the blocks that it asks for and no others.
    Disabled,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestMaterializationForInspection {
    pub(crate) manifest: NamespaceManifestEnvelope,
    pub(crate) metadata_state: MetadataState,
}

pub(crate) struct VerifiedMetadataSegments<'a, S: ObjectStore + ?Sized> {
    pub(super) store: &'a S,
    pub(super) segment_cache: Option<&'a MetadataSegmentCache>,
    pub(super) manifest_object_key: String,
    pub(super) manifest: Option<Arc<NamespaceManifestEnvelope>>,
    pub(super) manifest_bytes: u64,
    /// The manifest's runs, newest first, grouped once during load
    /// validation and shared through the manifest cache entry. A merge
    /// doesn't depend on this order, because row keys are unique across
    /// runs, but [`Self::scan_prefix_in_newest_run_for_lookup`] does.
    pub(super) scan_runs: Arc<Vec<MetadataRunManifest>>,
    /// Retains fetched blocks within the operation's data budget.
    pub(super) block_memo: SessionBlockMemo,
    #[cfg(test)]
    pub(super) peak_page_rows: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl<'a, S: ObjectStore + ?Sized> VerifiedMetadataSegments<'a, S> {
    pub(crate) fn peak_page_rows(&self) -> usize {
        self.peak_page_rows
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(super) fn from_runs(
        store: &'a S,
        segment_cache: &'a MetadataSegmentCache,
        scan_runs: Vec<MetadataRunManifest>,
    ) -> Self {
        Self {
            store,
            segment_cache: Some(segment_cache),
            manifest_object_key: String::new(),
            manifest: None,
            manifest_bytes: 0,
            scan_runs: Arc::new(scan_runs),
            block_memo: SessionBlockMemo::new(super::cache::read_working_memory(Some(
                segment_cache,
            ))),
            peak_page_rows: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl<S: ObjectStore + ?Sized> VerifiedMetadataSegments<'_, S> {
    /// The same segments with an empty block memo of their own, so the
    /// blocks that one pass memoizes are released when the pass ends.
    pub(super) fn with_empty_block_memo(&self) -> Self {
        Self {
            store: self.store,
            segment_cache: self.segment_cache,
            manifest_object_key: self.manifest_object_key.clone(),
            manifest: self.manifest.clone(),
            manifest_bytes: self.manifest_bytes,
            scan_runs: Arc::clone(&self.scan_runs),
            block_memo: SessionBlockMemo::new(super::cache::read_working_memory(
                self.segment_cache,
            )),
            #[cfg(test)]
            peak_page_rows: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn manifest(&self) -> &NamespaceManifestEnvelope {
        self.manifest
            .as_deref()
            .expect("loaded metadata segments should carry their manifest")
    }

    pub(crate) async fn get_for_lookup(
        &self,
        family: MetadataRowFamily,
        key: &str,
        filter_probe: &str,
    ) -> Result<Option<MetadataRow>, ManifestLoadError> {
        // Matching on the stored row key: the scan already selected the rows
        // by that key, and recomputing keys from rows allocates per row.
        Ok(self
            .scan_prefix_rows(family, key, Some(filter_probe), Readahead::Enabled)
            .await?
            .into_iter()
            .find(|(row_key, _)| row_key == key)
            .map(|(_, row)| row))
    }

    /// Whole-prefix scan without a bloom probe or a row limit: every segment
    /// in range is read. No production read is allowed to cost the whole
    /// family, so this exists only for the test-only inspection
    /// materialization, which is defined as reading everything.
    #[cfg(test)]
    pub(crate) async fn scan_prefix(
        &self,
        family: MetadataRowFamily,
        prefix: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        Ok(strip_row_keys(
            self.scan_prefix_rows(family, prefix, None, Readahead::Enabled)
                .await?,
        ))
    }

    /// [`Self::scan_prefix`] for a point lookup: `filter_probe` is the
    /// family's exact filter key for the value being looked up, so each
    /// candidate segment's bloom filter is consulted before its index or
    /// data — a negative skips the segment without fetching either. Scans
    /// coarser than the filter key must use [`Self::scan_prefix`] instead.
    pub(crate) async fn scan_prefix_for_lookup(
        &self,
        family: MetadataRowFamily,
        prefix: &str,
        filter_probe: &str,
        readahead: Readahead,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        Ok(strip_row_keys(
            self.scan_prefix_rows(family, prefix, Some(filter_probe), readahead)
                .await?,
        ))
    }

    /// Returns up to `limit` rows of `prefix` from the newest run that has a
    /// row for it.
    ///
    /// A lookup that wants the newest fact for one key prefix (the latest
    /// revision of an inode, the newest version of a directory slot, or the
    /// newest attribute or access revision) gets the same answer here as from
    /// the merge of every run, for two reasons:
    ///
    /// - Each run covers its own range of commit sequences, and `scan_runs`
    ///   lists the runs newest first, so every row of a run is newer than
    ///   every row of the runs after it. A fold writes the commits after the
    ///   manifest's folded WAL object into one new run. A compaction merges
    ///   a contiguous window of a family group's runs into one run at the
    ///   window's position, and the group's base run comes before every
    ///   delta run, whatever its sequence stamp. (Run numbers don't give this
    ///   order, because a compaction's output gets a new number.) See
    ///   [Manifests, runs, and segments](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#71-manifests-runs-and-segments),
    ///   [Compaction windows](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#102-compaction-windows),
    ///   and "Runs cover separate sequence ranges" in [Row retention during
    ///   a base compaction](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#103-row-retention-during-a-base-compaction).
    /// - A later commit gives an inode a higher revision, attribute revision,
    ///   or access revision number, and a later version of a directory slot
    ///   has a higher position. See [Metadata rows and row
    ///   keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
    ///
    /// So the newest run with a row for the prefix has the newest fact, and
    /// the older runs can only add older facts.
    ///
    /// The lookup checks the candidate segments' filters in every run at
    /// once, as the merge does, and then reads the admitted runs one at a
    /// time, newest first. The newest admitted run has the rows unless its
    /// filter admitted the prefix by a false positive, and then the lookup
    /// reads the next admitted run. A lookup that needs every fact for a
    /// prefix (a revision listing or a tombstone check) uses
    /// [`Self::scan_prefix_for_lookup`] instead, and so does a read below a
    /// run's sequence ([`Self::every_row_is_visible_at`]), because the newest
    /// run's rows could all be above the read.
    pub(crate) async fn scan_prefix_in_newest_run_for_lookup(
        &self,
        family: MetadataRowFamily,
        prefix: &str,
        filter_probe: &str,
        limit: usize,
        readahead: Readahead,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        let upper_bound = string_prefix_upper_bound(prefix);
        let upper_bound = upper_bound.as_deref();
        let mut candidates = Vec::new();
        let mut run_ends = Vec::with_capacity(self.scan_runs.len());
        for run in self.scan_runs.iter() {
            self.push_run_candidates(&mut candidates, run, family, prefix, upper_bound)?;
            run_ends.push(candidates.len());
        }
        let admissions = self.filter_admissions(&candidates, filter_probe).await?;
        let mut run_start = 0;
        for run_end in run_ends {
            let admitted: Vec<_> = candidates[run_start..run_end]
                .iter()
                .zip(&admissions[run_start..run_end])
                .filter_map(|(candidate, admits)| admits.then_some(*candidate))
                .collect();
            run_start = run_end;
            if admitted.is_empty() {
                continue;
            }
            let page = self
                .merge_segment_rows(admitted, prefix, upper_bound, limit, readahead, true)
                .await?;
            if !page.is_empty() {
                return Ok(strip_row_keys(page));
            }
        }
        Ok(Vec::new())
    }

    /// Whether a read at `visible_seq` sees every row of these runs. Every
    /// row of a run is at or below the run's sequence, which each block load
    /// checks.
    pub(crate) fn every_row_is_visible_at(&self, visible_seq: ChangeSeq) -> bool {
        self.scan_runs.iter().all(|run| run.run_seq <= visible_seq)
    }

    pub(crate) async fn scan_range_page(
        &self,
        family: MetadataRowFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        Ok(strip_row_keys(
            self.scan_range_page_with_keys(family, lower_bound, upper_bound, limit)
                .await?,
        ))
    }

    /// [`Self::scan_range_page`], returning each row with its stored row
    /// key, for callers that page by row key and would otherwise recompute
    /// every key from its row.
    pub(crate) async fn scan_range_page_with_keys(
        &self,
        family: MetadataRowFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.scan_range_page_rows(
            family,
            lower_bound,
            upper_bound,
            limit,
            None,
            Readahead::Enabled,
        )
        .await
    }

    /// [`Self::scan_range_page_for_lookup`] with each row's key, for a lookup
    /// that pages by row key.
    pub(crate) async fn scan_range_page_with_keys_for_lookup(
        &self,
        family: MetadataRowFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
        filter_probe: &str,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.scan_range_page_rows(
            family,
            lower_bound,
            upper_bound,
            limit,
            Some(filter_probe),
            Readahead::Enabled,
        )
        .await
    }

    /// [`Self::scan_range_page`] for a point lookup within one filter key's
    /// range; see [`Self::scan_prefix_for_lookup`] for the probe contract.
    pub(crate) async fn scan_range_page_for_lookup(
        &self,
        family: MetadataRowFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
        filter_probe: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        Ok(strip_row_keys(
            self.scan_range_page_with_keys_for_lookup(
                family,
                lower_bound,
                upper_bound,
                limit,
                filter_probe,
            )
            .await?,
        ))
    }

    /// A prefix scan is a range scan over `[prefix, upper_bound(prefix))`
    /// with no row limit; rows come back in row-key order.
    async fn scan_prefix_rows(
        &self,
        family: MetadataRowFamily,
        prefix: &str,
        filter_probe: Option<&str>,
        readahead: Readahead,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        let upper_bound = string_prefix_upper_bound(prefix);
        self.scan_range_page_rows(
            family,
            prefix,
            upper_bound.as_deref(),
            usize::MAX,
            filter_probe,
            readahead,
        )
        .await
    }

    /// Whether a lookup should touch this segment at all: false only when
    /// the segment's bloom filter proves the probe key absent. A skipped
    /// segment costs one tiny cached filter read instead of index and data
    /// fetches.
    async fn segment_filter_admits(
        &self,
        descriptor: &MetadataSegmentRef,
        filter_probe: &str,
    ) -> Result<bool, ManifestLoadError> {
        let filter =
            load_segment_filter(self.store, self.segment_cache, &self.block_memo, descriptor)
                .await?;
        if filter.may_contain(filter_probe) {
            return Ok(true);
        }
        if let Some(cache) = self.segment_cache {
            cache.record_filter_skip();
        }
        Ok(false)
    }

    /// Counts a segment whose filter admitted the probe but whose rows had
    /// no match — the filter's false-positive rate as observed by lookups.
    fn record_filter_false_positive_if_empty(&self, matched_rows: usize) {
        if matched_rows == 0 {
            if let Some(cache) = self.segment_cache {
                cache.record_filter_false_positive();
            }
        }
    }

    async fn scan_range_page_rows(
        &self,
        family: MetadataRowFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
        filter_probe: Option<&str>,
        readahead: Readahead,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        let mut candidates = Vec::new();
        for run in self.scan_runs.iter() {
            self.push_run_candidates(&mut candidates, run, family, lower_bound, upper_bound)?;
        }
        let admitted = self
            .filter_admitted_descriptors(candidates, filter_probe)
            .await?;
        self.merge_segment_rows(
            admitted,
            lower_bound,
            upper_bound,
            limit,
            readahead,
            filter_probe.is_some(),
        )
        .await
    }

    /// Adds the segments of `run` in `family` whose key range meets
    /// `[lower_bound, upper_bound)` to `candidates`.
    fn push_run_candidates<'r>(
        &self,
        candidates: &mut Vec<ScanDescriptor<'r>>,
        run: &'r MetadataRunManifest,
        family: MetadataRowFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
    ) -> Result<(), ManifestLoadError> {
        let family_segments =
            manifest_segment_for_family(&self.manifest_object_key, &run.segments, family)?;
        candidates.extend(
            family_segments
                .segments
                .iter()
                .filter(|descriptor| {
                    key_range_may_intersect(
                        &descriptor.min_row_key,
                        &descriptor.max_row_key,
                        descriptor.row_count,
                        lower_bound,
                        upper_bound,
                    )
                })
                .map(|descriptor| ScanDescriptor {
                    descriptor,
                    max_seq: run.run_seq,
                }),
        );
        Ok(())
    }

    /// Merges the rows of `descriptors` in `[lower_bound, upper_bound)` into
    /// one page of at most `limit` rows in row-key order. When a filter
    /// admitted the descriptors, a descriptor with no row in the range is
    /// recorded as a false positive.
    async fn merge_segment_rows(
        &self,
        mut descriptors: Vec<ScanDescriptor<'_>>,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
        readahead: Readahead,
        filter_admitted: bool,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        descriptors.sort_by(|left, right| {
            left.descriptor
                .min_row_key
                .cmp(&right.descriptor.min_row_key)
                .then(
                    left.descriptor
                        .max_row_key
                        .cmp(&right.descriptor.max_row_key),
                )
                // Segment ids are unique and provide the final tie-breaker.
                .then(left.descriptor.segment_id.cmp(&right.descriptor.segment_id))
        });

        let loader = ScanLoader {
            store: self.store,
            segment_cache: self.segment_cache,
            block_memo: &self.block_memo,
            readahead,
            upper_bound,
            // A window reads on past the range, so that the read's later
            // lookups and scans of nearby keys find their blocks in the memo.
            windows_stop_at_upper_bound: false,
        };
        let mut iterators = Vec::new();
        let mut descriptors = descriptors.into_iter();
        let mut rows: BTreeMap<String, MetadataRow> = BTreeMap::new();
        loop {
            let mut open_through = rows
                .last_key_value()
                .map_or(lower_bound, |(key, _)| key.as_str());
            if iterators.is_empty() && rows.len() < limit {
                if let Some(candidate) =
                    descriptors.as_slice()[..descriptors.len().min(SCAN_ITERATORS_OPEN)].last()
                {
                    open_through = open_through.max(&candidate.descriptor.min_row_key);
                }
            }
            let previously_open = iterators.len();
            while descriptors
                .as_slice()
                .first()
                .is_some_and(|candidate| candidate.descriptor.min_row_key.as_str() <= open_through)
            {
                let descriptor = descriptors
                    .next()
                    .expect("a pending descriptor should exist");
                iterators.push(SegmentRowIterator::new(
                    (),
                    vec![descriptor],
                    Some(lower_bound.to_owned()),
                ));
            }
            if iterators.is_empty() {
                break;
            }
            refill_iterators(&loader, &mut iterators, 1).await?;
            if filter_admitted {
                for iterator in &iterators[previously_open..] {
                    let matched = iterator
                        .head()
                        .is_some_and(|(key, _)| upper_bound.is_none_or(|upper| key < upper));
                    self.record_filter_false_positive_if_empty(usize::from(matched));
                }
            }
            // Keeping only the smallest page lets exhausted blocks refill together.
            for iterator in &mut iterators {
                while let Some((key, _)) = iterator.head() {
                    if upper_bound.is_some_and(|upper| key >= upper) {
                        break;
                    }
                    if rows.len() == limit {
                        if rows
                            .last_key_value()
                            .is_some_and(|(last, _)| key >= last.as_str())
                        {
                            break;
                        }
                        rows.pop_last();
                    }
                    rows.insert(key.to_owned(), iterator.take_head());
                    #[cfg(test)]
                    self.peak_page_rows
                        .fetch_max(rows.len(), std::sync::atomic::Ordering::Relaxed);
                }
            }
            iterators.retain(|iterator| {
                let Some(last_key) = iterator.loaded_through_key() else {
                    return false;
                };
                iterator.head().is_none()
                    && iterator
                        .current_segment()
                        .is_some_and(|segment| last_key < segment.descriptor.max_row_key.as_str())
                    && upper_bound.is_none_or(|upper| last_key < upper)
                    && (rows.len() < limit
                        || rows
                            .last_key_value()
                            .is_some_and(|(last, _)| last_key < last.as_str()))
            });
        }

        Ok(rows.into_iter().collect())
    }

    /// Drops the descriptors whose bloom filter rules the probe out; with no
    /// probe every descriptor is admitted untouched.
    async fn filter_admitted_descriptors<'d>(
        &self,
        descriptors: Vec<ScanDescriptor<'d>>,
        filter_probe: Option<&str>,
    ) -> Result<Vec<ScanDescriptor<'d>>, ManifestLoadError> {
        let Some(filter_probe) = filter_probe else {
            return Ok(descriptors);
        };
        let admissions = self.filter_admissions(&descriptors, filter_probe).await?;
        Ok(descriptors
            .into_iter()
            .zip(admissions)
            .filter_map(|(descriptor, admits)| admits.then_some(descriptor))
            .collect())
    }

    /// Whether each descriptor's bloom filter admits the probe, in order.
    async fn filter_admissions(
        &self,
        descriptors: &[ScanDescriptor<'_>],
        filter_probe: &str,
    ) -> Result<Vec<bool>, ManifestLoadError> {
        let mut admissions = Vec::with_capacity(descriptors.len());
        let mut checks = stream::iter(0..descriptors.len())
            .map(|index| {
                let descriptor = descriptors[index];
                async move {
                    self.segment_filter_admits(descriptor.descriptor, filter_probe)
                        .await
                }
            })
            .buffered(STORE_READS_IN_FLIGHT);
        while let Some(admits) = checks.try_next().await? {
            admissions.push(admits);
        }
        Ok(admissions)
    }
}

fn strip_row_keys(rows: Vec<(String, MetadataRow)>) -> Vec<MetadataRow> {
    rows.into_iter().map(|(_, row)| row).collect()
}

pub(super) fn ordered_manifest_segments<'a>(
    manifest_object_key: &str,
    segments_by_family: &'a [MetadataFamilySegments],
) -> Result<Vec<&'a MetadataFamilySegments>, ManifestLoadError> {
    let mut ordered = Vec::with_capacity(MANIFEST_ROW_FAMILIES.len());
    for family in MANIFEST_ROW_FAMILIES {
        let mut matching = segments_by_family
            .iter()
            .filter(|family_segments| family_segments.family == family);
        let Some(family_segments) = matching.next() else {
            return Err(ManifestLoadError::MissingRowFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        };
        if matching.next().is_some() {
            return Err(ManifestLoadError::DuplicateRowFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        }
        ordered.push(family_segments);
    }
    Ok(ordered)
}

pub(super) fn manifest_segment_for_family<'a>(
    manifest_object_key: &str,
    segments_by_family: &'a [MetadataFamilySegments],
    family: MetadataRowFamily,
) -> Result<&'a MetadataFamilySegments, ManifestLoadError> {
    // Family-set integrity is validated once when the segments are loaded;
    // scans only need the lookup.
    segments_by_family
        .iter()
        .find(|family_segments| family_segments.family == family)
        .ok_or(ManifestLoadError::MissingRowFamily {
            object_key: manifest_object_key.to_owned(),
            family,
        })
}
