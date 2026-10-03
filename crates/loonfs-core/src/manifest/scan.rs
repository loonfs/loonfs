//! Verified row scans over a loaded manifest's segments, with per-segment
//! caching and prefix-window pruning.

use super::cache::MetadataSegmentCache;
use super::compaction_merge::{refill_iterators, SegmentRowIterator};
use super::error::ManifestLoadError;
use super::load::{load_segment_filter, SessionBlockMemo};
use super::runs::{MetadataFamilySegments, MetadataRunManifest, MANIFEST_ROW_FAMILIES};
use super::scan_load::{ScanDescriptor, ScanLoader};
#[cfg(test)]
use crate::metadata::MetadataState;
use crate::store_waves::STORE_READ_WAVE;
use futures::future::try_join_all;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    MetadataRow, MetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope,
};
use loonfs_types::format::sst_blocks::{key_range_may_intersect, string_prefix_upper_bound};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Readahead {
    Enabled,
    Stored,
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
    /// The manifest's runs, grouped once during load validation and shared
    /// through the manifest cache entry. Scans merge globally unique row keys
    /// and do not depend on this order.
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
            block_memo: SessionBlockMemo::default(),
            peak_page_rows: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl<S: ObjectStore + ?Sized> VerifiedMetadataSegments<'_, S> {
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
            .scan_prefix_rows(family, key, Some(filter_probe), Readahead::Stored)
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
            if family == MetadataRowFamily::Commits {
                Readahead::Stored
            } else {
                Readahead::Enabled
            },
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
        if limit == 0 {
            return Ok(Vec::new());
        }
        Ok(strip_row_keys(
            self.scan_range_page_rows(
                family,
                lower_bound,
                upper_bound,
                limit,
                Some(filter_probe),
                Readahead::Stored,
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
        }
        let mut matching_descriptors = self
            .filter_admitted_descriptors(candidates, filter_probe)
            .await?;
        matching_descriptors.sort_by(|left, right| {
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
            segments: self,
            readahead,
            upper_bound,
        };
        let mut iterators = Vec::new();
        let mut descriptors = matching_descriptors.into_iter();
        let mut rows: BTreeMap<String, MetadataRow> = BTreeMap::new();
        loop {
            let mut open_through = rows
                .last_key_value()
                .map_or(lower_bound, |(key, _)| key.as_str());
            if iterators.is_empty() && rows.len() < limit {
                if let Some(candidate) =
                    descriptors.as_slice()[..descriptors.len().min(STORE_READ_WAVE)].last()
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
            if filter_probe.is_some() {
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
        let mut admitted = Vec::with_capacity(descriptors.len());
        for chunk in descriptors.chunks(STORE_READ_WAVE) {
            let checks =
                try_join_all(chunk.iter().map(|descriptor| {
                    self.segment_filter_admits(descriptor.descriptor, filter_probe)
                }))
                .await?;
            admitted.extend(
                chunk
                    .iter()
                    .zip(checks)
                    .filter(|(_, admits)| *admits)
                    .map(|(descriptor, _)| *descriptor),
            );
        }
        Ok(admitted)
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
