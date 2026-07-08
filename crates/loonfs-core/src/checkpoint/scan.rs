//! Verified row scans over a loaded manifest's tables, with per-segment
//! caching and prefix-window pruning.

use super::cache::{DecodedSegmentRowSet, MetadataTableCache};
use super::error::ManifestLoadError;
use super::load::{
    load_manifest_segment_rows_in_key_range_with_cache, load_segment_filter,
    metadata_file_object_key, MetadataSstSeqExpectation, MetadataTableCacheMode,
    MetadataTableLoadContext, SessionBlockMemo,
};
use super::runs::{runs_in_scan_order, MetadataTableManifest, CHECKPOINT_TABLE_FAMILIES};
#[cfg(test)]
use crate::metadata::MetadataState;
use futures::future::try_join_all;
use loonfs_api::wire::manifest::{
    MetadataFileRef, MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope,
};
use loonfs_objectstore::ObjectStore;
#[cfg(test)]
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Widest scan a cache without a byte budget may populate. A count-bounded
/// cache evicts per entry, so admitting a wide scan would flush it; a
/// byte-budgeted cache ignores this limit and admits every scan.
pub(super) const SMALL_SCAN_CACHE_SEGMENT_LIMIT: usize = 4;
/// Segment fetches issued per wave during a scan. Wide directories touch
/// hundreds of segments per page; deeper waves amortize the per-wave await
/// without unbounded fan-out.
pub(super) const MAX_MATERIALIZED_TABLE_FETCHES: usize = 16;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManifestMaterializationForInspection {
    pub(crate) manifest: NamespaceManifestEnvelope,
    pub(crate) metadata_state: MetadataState,
}

pub(crate) struct VerifiedMetadataTables<'a, S: ObjectStore + ?Sized> {
    pub(super) store: &'a S,
    pub(super) table_cache: Option<&'a MetadataTableCache>,
    pub(super) manifest_object_key: String,
    pub(super) manifest: NamespaceManifestEnvelope,
    /// Per-view memo of decoded blocks: one operation never re-fetches a
    /// block it already saw, with or without a shared cache attached.
    pub(super) block_memo: SessionBlockMemo,
}

impl<S: ObjectStore + ?Sized> VerifiedMetadataTables<'_, S> {
    pub(crate) fn manifest(&self) -> &NamespaceManifestEnvelope {
        &self.manifest
    }

    pub(crate) async fn get_for_lookup(
        &self,
        family: MetadataTableFamily,
        key: &str,
        filter_probe: &str,
    ) -> Result<Option<MetadataRow>, ManifestLoadError> {
        Ok(self
            .scan_prefix_with_cache_mode(
                family,
                key,
                MetadataTableCacheMode::Populate,
                Some(filter_probe),
            )
            .await?
            .into_iter()
            .find(|row| row.row_key_for_family(family) == key))
    }

    pub(crate) async fn scan_prefix(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        let matching_segment_count = self.matching_segment_count(family, prefix)?;
        let cache_mode = self.scan_cache_mode(matching_segment_count);
        self.scan_prefix_with_cache_mode(family, prefix, cache_mode, None)
            .await
    }

    /// [`Self::scan_prefix`] for a point lookup: `filter_probe` is the
    /// family's exact filter key for the value being looked up, so each
    /// candidate segment's bloom filter is consulted before its index or
    /// data — a negative skips the segment without fetching either. Scans
    /// coarser than the filter key must use [`Self::scan_prefix`] instead.
    pub(crate) async fn scan_prefix_for_lookup(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
        filter_probe: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        let matching_segment_count = self.matching_segment_count(family, prefix)?;
        let cache_mode = self.scan_cache_mode(matching_segment_count);
        self.scan_prefix_with_cache_mode(family, prefix, cache_mode, Some(filter_probe))
            .await
    }

    /// Cache admission for a scan matching `matching_segment_count` segments.
    /// A byte-budgeted table cache admits every scan — byte-based eviction
    /// makes wide admissions safe, and list preloads are exactly the wide
    /// scans that must land in the cache. Without a byte budget, wide scans
    /// stay read-only so they cannot flush a count-bounded cache.
    fn scan_cache_mode(&self, matching_segment_count: usize) -> MetadataTableCacheMode {
        let byte_budgeted = self
            .table_cache
            .is_some_and(MetadataTableCache::byte_budgeted);
        if byte_budgeted || matching_segment_count <= SMALL_SCAN_CACHE_SEGMENT_LIMIT {
            MetadataTableCacheMode::Populate
        } else {
            MetadataTableCacheMode::ReadOnly
        }
    }

    pub(crate) async fn scan_range_page(
        &self,
        family: MetadataTableFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.scan_range_page_rows(family, lower_bound, upper_bound, limit, None)
            .await
    }

    /// [`Self::scan_range_page`] for a point lookup within one filter key's
    /// range; see [`Self::scan_prefix_for_lookup`] for the probe contract.
    pub(crate) async fn scan_range_page_for_lookup(
        &self,
        family: MetadataTableFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
        filter_probe: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.scan_range_page_rows(family, lower_bound, upper_bound, limit, Some(filter_probe))
            .await
    }

    async fn scan_prefix_with_cache_mode(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
        cache_mode: MetadataTableCacheMode,
        filter_probe: Option<&str>,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        let mut rows = Vec::new();
        for run in runs_in_scan_order(&self.manifest.payload) {
            self.scan_manifest_tables(
                family,
                prefix,
                MetadataTableLoadContext {
                    manifest_object_key: &self.manifest_object_key,
                    segment_seq_expectation: MetadataSstSeqExpectation::Descriptor,
                    row_seq_min: None,
                    row_seq_max: run.run_seq,
                },
                &run.tables,
                MetadataTableScanOutput {
                    rows: &mut rows,
                    cache_mode,
                },
                filter_probe,
            )
            .await?;
        }
        Ok(rows)
    }

    /// Whether a lookup should touch this segment at all: false only when
    /// the segment's bloom filter proves the probe key absent. A skipped
    /// segment costs one tiny cached filter read instead of index and data
    /// fetches.
    async fn segment_filter_admits(
        &self,
        descriptor: &MetadataFileRef,
        cache_mode: MetadataTableCacheMode,
        filter_probe: &str,
    ) -> Result<bool, ManifestLoadError> {
        let filter = load_segment_filter(
            self.store,
            self.table_cache,
            &self.block_memo,
            descriptor,
            cache_mode,
        )
        .await?;
        if filter.may_contain(filter_probe) {
            return Ok(true);
        }
        if let Some(cache) = self.table_cache {
            cache.record_filter_skip();
        }
        Ok(false)
    }

    /// Counts a segment whose filter admitted the probe but whose rows had
    /// no match — the filter's false-positive rate as observed by lookups.
    fn record_filter_false_positive_if_empty(&self, matched_rows: usize) {
        if matched_rows == 0 {
            if let Some(cache) = self.table_cache {
                cache.record_filter_false_positive();
            }
        }
    }

    fn matching_segment_count(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
    ) -> Result<usize, ManifestLoadError> {
        let mut count = 0;
        for run in runs_in_scan_order(&self.manifest.payload) {
            count +=
                count_matching_segments(&self.manifest_object_key, &run.tables, family, prefix)?;
        }
        Ok(count)
    }

    async fn scan_range_page_rows(
        &self,
        family: MetadataTableFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
        filter_probe: Option<&str>,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        let mut matching_descriptors = Vec::new();
        for run in runs_in_scan_order(&self.manifest.payload) {
            let context = MetadataTableLoadContext {
                manifest_object_key: &self.manifest_object_key,
                segment_seq_expectation: MetadataSstSeqExpectation::Descriptor,
                row_seq_min: None,
                row_seq_max: run.run_seq,
            };
            let table =
                manifest_table_for_family(context.manifest_object_key, &run.tables, family)?;
            for descriptor in &table.segments {
                context.expected_segment_seq(descriptor)?;
                let expected_key = metadata_file_object_key(descriptor);
                if descriptor.object_key != expected_key {
                    return Err(ManifestLoadError::SegmentObjectKeyMismatch {
                        object_key: descriptor.object_key.clone(),
                        expected: expected_key,
                    });
                }
                if !descriptor_may_intersect_range(descriptor, lower_bound, upper_bound) {
                    continue;
                }
                if let Some(filter_probe) = filter_probe {
                    if !self
                        .segment_filter_admits(
                            descriptor,
                            MetadataTableCacheMode::Populate,
                            filter_probe,
                        )
                        .await?
                    {
                        continue;
                    }
                }
                matching_descriptors.push((context, descriptor.clone()));
            }
        }
        matching_descriptors.sort_by(|(_, left), (_, right)| {
            left.min_key
                .cmp(&right.min_key)
                .then(left.max_key.cmp(&right.max_key))
                .then(left.object_key.cmp(&right.object_key))
        });
        let cache_mode = self.scan_cache_mode(matching_descriptors.len());

        let mut rows = Vec::<(String, MetadataRow)>::new();
        let mut next_descriptor_index = 0;
        while next_descriptor_index < matching_descriptors.len() {
            let should_load_next = if rows.len() < limit {
                true
            } else {
                let boundary_key = &rows[limit - 1].0;
                matching_descriptors[next_descriptor_index].1.min_key <= *boundary_key
            };
            if !should_load_next {
                break;
            }

            let chunk_end = (next_descriptor_index + MAX_MATERIALIZED_TABLE_FETCHES)
                .min(matching_descriptors.len());
            let loaded_segments = try_join_all(
                matching_descriptors[next_descriptor_index..chunk_end]
                    .iter()
                    .map(|(context, descriptor)| {
                        self.segment_rows(
                            *context,
                            family,
                            descriptor,
                            cache_mode,
                            lower_bound,
                            upper_bound,
                        )
                    }),
            )
            .await?;
            next_descriptor_index = chunk_end;

            rows.extend(loaded_segments.iter().flat_map(|segment_rows| {
                segment_rows
                    .rows_in_key_range(lower_bound, upper_bound)
                    .map(|(row_key, row)| (row_key.to_owned(), row.clone()))
            }));
            rows.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
        }

        rows.truncate(limit);
        Ok(rows.into_iter().map(|(_, row)| row).collect())
    }

    async fn scan_manifest_tables(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
        context: MetadataTableLoadContext<'_>,
        tables: &[MetadataTableManifest],
        output: MetadataTableScanOutput<'_>,
        filter_probe: Option<&str>,
    ) -> Result<(), ManifestLoadError> {
        let table = manifest_table_for_family(context.manifest_object_key, tables, family)?;
        let mut matching_descriptors = Vec::new();
        for descriptor in &table.segments {
            context.expected_segment_seq(descriptor)?;
            let expected_key = metadata_file_object_key(descriptor);
            if descriptor.object_key != expected_key {
                return Err(ManifestLoadError::SegmentObjectKeyMismatch {
                    object_key: descriptor.object_key.clone(),
                    expected: expected_key,
                });
            }
            if !descriptor_may_contain_prefix(descriptor, prefix) {
                continue;
            }
            matching_descriptors.push(descriptor);
        }
        let matching_descriptors = self
            .filter_admitted_descriptors(matching_descriptors, output.cache_mode, filter_probe)
            .await?;

        let prefix_upper_bound = string_prefix_upper_bound(prefix);
        let mut loaded_segments = Vec::new();
        for chunk in matching_descriptors.chunks(MAX_MATERIALIZED_TABLE_FETCHES) {
            loaded_segments.extend(
                try_join_all(chunk.iter().map(|descriptor| {
                    self.segment_rows(
                        context,
                        family,
                        descriptor,
                        output.cache_mode,
                        prefix,
                        prefix_upper_bound.as_deref(),
                    )
                }))
                .await?,
            );
        }

        for segment_rows in loaded_segments {
            let matched = output.rows.len();
            output.rows.extend(
                segment_rows
                    .rows_in_key_range(prefix, prefix_upper_bound.as_deref())
                    .map(|(_, row)| row.clone()),
            );
            if filter_probe.is_some() {
                self.record_filter_false_positive_if_empty(output.rows.len() - matched);
            }
        }
        Ok(())
    }

    /// Drops the descriptors whose bloom filter rules the probe out; with no
    /// probe every descriptor is admitted untouched.
    async fn filter_admitted_descriptors<'d>(
        &self,
        descriptors: Vec<&'d MetadataFileRef>,
        cache_mode: MetadataTableCacheMode,
        filter_probe: Option<&str>,
    ) -> Result<Vec<&'d MetadataFileRef>, ManifestLoadError> {
        let Some(filter_probe) = filter_probe else {
            return Ok(descriptors);
        };
        let mut admitted = Vec::with_capacity(descriptors.len());
        for chunk in descriptors.chunks(MAX_MATERIALIZED_TABLE_FETCHES) {
            let checks = try_join_all(chunk.iter().map(|descriptor| {
                self.segment_filter_admits(descriptor, cache_mode, filter_probe)
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

    async fn segment_rows(
        &self,
        context: MetadataTableLoadContext<'_>,
        family: MetadataTableFamily,
        descriptor: &MetadataFileRef,
        cache_mode: MetadataTableCacheMode,
        lower_bound: &str,
        upper_bound: Option<&str>,
    ) -> Result<Arc<DecodedSegmentRowSet>, ManifestLoadError> {
        load_manifest_segment_rows_in_key_range_with_cache(
            self.store,
            self.table_cache,
            &self.block_memo,
            context,
            family,
            descriptor,
            cache_mode,
            lower_bound,
            upper_bound,
        )
        .await
    }
}

pub(super) struct MetadataTableScanOutput<'a> {
    rows: &'a mut Vec<MetadataRow>,
    cache_mode: MetadataTableCacheMode,
}

pub(super) fn ordered_manifest_tables<'a>(
    manifest_object_key: &str,
    tables: &'a [MetadataTableManifest],
) -> Result<Vec<&'a MetadataTableManifest>, ManifestLoadError> {
    let mut ordered = Vec::with_capacity(CHECKPOINT_TABLE_FAMILIES.len());
    for family in CHECKPOINT_TABLE_FAMILIES {
        let mut matching = tables.iter().filter(|table| table.family == family);
        let Some(table) = matching.next() else {
            return Err(ManifestLoadError::MissingTableFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        };
        if matching.next().is_some() {
            return Err(ManifestLoadError::DuplicateTableFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        }
        ordered.push(table);
    }
    Ok(ordered)
}

pub(super) fn manifest_table_for_family<'a>(
    manifest_object_key: &str,
    tables: &'a [MetadataTableManifest],
    family: MetadataTableFamily,
) -> Result<&'a MetadataTableManifest, ManifestLoadError> {
    ordered_manifest_tables(manifest_object_key, tables)?
        .into_iter()
        .find(|table| table.family == family)
        .ok_or(ManifestLoadError::MissingTableFamily {
            object_key: manifest_object_key.to_owned(),
            family,
        })
}

pub(super) fn count_matching_segments(
    manifest_object_key: &str,
    tables: &[MetadataTableManifest],
    family: MetadataTableFamily,
    prefix: &str,
) -> Result<usize, ManifestLoadError> {
    let table = manifest_table_for_family(manifest_object_key, tables, family)?;
    Ok(table
        .segments
        .iter()
        .filter(|descriptor| descriptor_may_contain_prefix(descriptor, prefix))
        .count())
}

pub(super) fn descriptor_may_contain_prefix(descriptor: &MetadataFileRef, prefix: &str) -> bool {
    if descriptor.row_count == 0 {
        return false;
    }
    if prefix.is_empty() {
        return true;
    }
    if descriptor.max_key.as_str() < prefix {
        return false;
    }
    if let Some(upper_bound) = string_prefix_upper_bound(prefix) {
        if descriptor.min_key >= upper_bound {
            return false;
        }
    }
    true
}

pub(super) fn descriptor_may_intersect_range(
    descriptor: &MetadataFileRef,
    lower_bound: &str,
    upper_bound: Option<&str>,
) -> bool {
    if descriptor.row_count == 0 {
        return false;
    }
    if descriptor.max_key.as_str() < lower_bound {
        return false;
    }
    if upper_bound
        .map(|upper_bound| descriptor.min_key.as_str() >= upper_bound)
        .unwrap_or(false)
    {
        return false;
    }
    true
}

pub(crate) fn string_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for index in (0..bytes.len()).rev() {
        if bytes[index] != u8::MAX {
            bytes[index] += 1;
            bytes.truncate(index + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}
