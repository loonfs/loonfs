//! Verified row scans over a loaded manifest's tables, with per-segment
//! caching and prefix-window pruning.

use super::cache::MetadataTableCache;
use super::error::ManifestLoadError;
use super::load::{
    load_manifest_segment_rows_in_key_range_with_cache, load_segment_filter, SegmentKeyRangeBlocks,
    SessionBlockMemo,
};
use super::runs::{MetadataRunManifest, MetadataTableManifest, CHECKPOINT_TABLE_FAMILIES};
#[cfg(test)]
use crate::metadata::MetadataState;
use futures::future::try_join_all;
use loonfs_api::wire::manifest::{
    MetadataFileRef, MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope,
};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_objectstore::ObjectStore;
#[cfg(test)]
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Segment fetches issued per wave during a scan. Wide directories touch
/// hundreds of segments per page; deeper waves amortize the per-wave await
/// without unbounded fan-out.
pub(super) const MAX_MATERIALIZED_TABLE_LOADS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Readahead {
    Enabled,
    Disabled,
}

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
    pub(super) manifest: Arc<NamespaceManifestEnvelope>,
    /// The manifest's runs in scan order, derived once at load validation
    /// and shared through the manifest cache entry. A run list is immutable
    /// per manifest object; scans must not regroup it per page.
    pub(super) scan_runs: Arc<Vec<MetadataRunManifest>>,
    /// Per-view memo of decoded blocks: one operation never re-fetches a
    /// block it already saw, with or without a shared cache attached.
    pub(super) block_memo: SessionBlockMemo,
}

impl<'a, S: ObjectStore + ?Sized> VerifiedMetadataTables<'a, S> {
    /// Wraps a manifest that was synthesized rather than loaded: the
    /// genesis basis of a namespace that has published none. It names no
    /// metadata files, so no scan it backs ever reaches the store, and its
    /// object key is never read or written.
    pub(crate) fn synthesized(store: &'a S, manifest: NamespaceManifestEnvelope) -> Self {
        debug_assert!(
            manifest.payload.metadata_files.is_empty(),
            "a synthesized manifest must name no durable metadata files"
        );
        let scan_runs = Arc::new(Vec::new());
        Self {
            store,
            table_cache: None,
            manifest_object_key: String::new(),
            manifest: Arc::new(manifest),
            scan_runs,
            block_memo: SessionBlockMemo::default(),
        }
    }
}

impl<S: ObjectStore + ?Sized> VerifiedMetadataTables<'_, S> {
    pub(crate) fn manifest(&self) -> &NamespaceManifestEnvelope {
        self.manifest.as_ref()
    }

    pub(crate) async fn get_for_lookup(
        &self,
        family: MetadataTableFamily,
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

    /// Whole-prefix scan without a bloom probe: per-row filters cannot
    /// answer a prefix, so every segment in range is read. Production use
    /// is deliberate and rare — the trash listing enumerates the immortal
    /// tombstone family this way.
    pub(crate) async fn scan_prefix(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        Ok(strip_row_keys(
            self.scan_prefix_rows(family, prefix, None, Readahead::Enabled)
                .await?,
        ))
    }

    /// Scans only `runs`, for a reorganization unit that is replacing that
    /// complete run subset while leaving every other descriptor untouched.
    pub(super) async fn scan_prefix_in_runs(
        &self,
        runs: &[MetadataRunManifest],
        family: MetadataTableFamily,
        prefix: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        Ok(strip_row_keys(
            self.scan_prefix_rows_in_runs(runs, family, prefix, None, Readahead::Disabled)
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
        family: MetadataTableFamily,
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
        family: MetadataTableFamily,
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
        family: MetadataTableFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.scan_range_page_rows(
            self.scan_runs.as_ref(),
            family,
            lower_bound,
            upper_bound,
            limit,
            None,
            Readahead::Enabled,
        )
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
        Ok(strip_row_keys(
            self.scan_range_page_rows(
                self.scan_runs.as_ref(),
                family,
                lower_bound,
                upper_bound,
                limit,
                Some(filter_probe),
                Readahead::Enabled,
            )
            .await?,
        ))
    }

    /// A prefix scan is a range scan over `[prefix, upper_bound(prefix))`
    /// with no row limit; rows come back in row-key order.
    async fn scan_prefix_rows(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
        filter_probe: Option<&str>,
        readahead: Readahead,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        self.scan_prefix_rows_in_runs(
            self.scan_runs.as_ref(),
            family,
            prefix,
            filter_probe,
            readahead,
        )
        .await
    }

    async fn scan_prefix_rows_in_runs(
        &self,
        runs: &[MetadataRunManifest],
        family: MetadataTableFamily,
        prefix: &str,
        filter_probe: Option<&str>,
        readahead: Readahead,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        let upper_bound = string_prefix_upper_bound(prefix);
        self.scan_range_page_rows(
            runs,
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
        descriptor: &MetadataFileRef,
        filter_probe: &str,
    ) -> Result<bool, ManifestLoadError> {
        let filter =
            load_segment_filter(self.store, self.table_cache, &self.block_memo, descriptor).await?;
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

    #[allow(clippy::too_many_arguments)]
    async fn scan_range_page_rows(
        &self,
        runs: &[MetadataRunManifest],
        family: MetadataTableFamily,
        lower_bound: &str,
        upper_bound: Option<&str>,
        limit: usize,
        filter_probe: Option<&str>,
        readahead: Readahead,
    ) -> Result<Vec<(String, MetadataRow)>, ManifestLoadError> {
        let mut candidates = Vec::new();
        for run in runs {
            let table = manifest_table_for_family(&self.manifest_object_key, &run.tables, family)?;
            candidates.extend(table.segments.iter().filter(|descriptor| {
                descriptor_may_intersect_range(descriptor, lower_bound, upper_bound)
            }));
        }
        let mut matching_descriptors = self
            .filter_admitted_descriptors(candidates, filter_probe)
            .await?;
        matching_descriptors.sort_by(|left, right| {
            left.min_key
                .cmp(&right.min_key)
                .then(left.max_key.cmp(&right.max_key))
                .then(left.object_key.cmp(&right.object_key))
        });

        let mut rows = Vec::<(String, MetadataRow)>::new();
        let mut next_descriptor_index = 0;
        while next_descriptor_index < matching_descriptors.len() {
            let should_load_next = if rows.len() < limit {
                true
            } else {
                let boundary_key = &rows[limit - 1].0;
                matching_descriptors[next_descriptor_index].min_key <= *boundary_key
            };
            if !should_load_next {
                break;
            }

            let chunk_end = (next_descriptor_index + MAX_MATERIALIZED_TABLE_LOADS)
                .min(matching_descriptors.len());
            let loaded_segments = try_join_all(
                matching_descriptors[next_descriptor_index..chunk_end]
                    .iter()
                    .map(|descriptor| {
                        self.segment_rows(family, descriptor, lower_bound, upper_bound, readahead)
                    }),
            )
            .await?;
            next_descriptor_index = chunk_end;

            for segment_rows in loaded_segments {
                let matched_before = rows.len();
                // Rows are ascending within a segment, so a row past the
                // segment's first `limit` matches can never survive the
                // merged truncation below; stopping there bounds how many
                // rows a page clones out of the shared blocks.
                rows.extend(
                    segment_rows
                        .rows_in_key_range(lower_bound, upper_bound)
                        .take(limit)
                        .map(|(row_key, row)| (row_key.to_owned(), row.clone())),
                );
                if filter_probe.is_some() {
                    self.record_filter_false_positive_if_empty(rows.len() - matched_before);
                }
            }
            rows.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
        }

        rows.truncate(limit);
        Ok(rows)
    }

    /// Drops the descriptors whose bloom filter rules the probe out; with no
    /// probe every descriptor is admitted untouched.
    async fn filter_admitted_descriptors<'d>(
        &self,
        descriptors: Vec<&'d MetadataFileRef>,
        filter_probe: Option<&str>,
    ) -> Result<Vec<&'d MetadataFileRef>, ManifestLoadError> {
        let Some(filter_probe) = filter_probe else {
            return Ok(descriptors);
        };
        let mut admitted = Vec::with_capacity(descriptors.len());
        for chunk in descriptors.chunks(MAX_MATERIALIZED_TABLE_LOADS) {
            let checks = try_join_all(
                chunk
                    .iter()
                    .map(|descriptor| self.segment_filter_admits(descriptor, filter_probe)),
            )
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
        family: MetadataTableFamily,
        descriptor: &MetadataFileRef,
        lower_bound: &str,
        upper_bound: Option<&str>,
        readahead: Readahead,
    ) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
        load_manifest_segment_rows_in_key_range_with_cache(
            self.store,
            self.table_cache,
            &self.block_memo,
            family,
            descriptor,
            lower_bound,
            upper_bound,
            readahead,
        )
        .await
    }
}

fn strip_row_keys(rows: Vec<(String, MetadataRow)>) -> Vec<MetadataRow> {
    rows.into_iter().map(|(_, row)| row).collect()
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
    // Family-set integrity is validated once when the tables are loaded;
    // scans only need the lookup.
    tables.iter().find(|table| table.family == family).ok_or(
        ManifestLoadError::MissingTableFamily {
            object_key: manifest_object_key.to_owned(),
            family,
        },
    )
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
