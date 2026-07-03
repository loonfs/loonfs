//! Verified row scans over a loaded manifest's tables, with per-segment
//! caching and prefix-window pruning.

use super::cache::MetadataTableCache;
use super::error::ManifestLoadError;
use super::load::{
    load_manifest_segment_rows_with_cache, metadata_file_object_key, MetadataSstSeqExpectation,
    MetadataTableCacheMode, MetadataTableLoadContext,
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
use std::collections::HashMap;
use std::sync::Mutex;

pub(super) const SMALL_SCAN_CACHE_SEGMENT_LIMIT: usize = 4;
pub(super) const MAX_MATERIALIZED_TABLE_FETCHES: usize = 8;

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
    pub(super) segment_cache: Mutex<HashMap<String, Vec<MetadataRow>>>,
}

impl<S: ObjectStore + ?Sized> VerifiedMetadataTables<'_, S> {
    pub(crate) fn manifest(&self) -> &NamespaceManifestEnvelope {
        &self.manifest
    }

    pub(crate) async fn get(
        &self,
        family: MetadataTableFamily,
        key: &str,
    ) -> Result<Option<MetadataRow>, ManifestLoadError> {
        Ok(self
            .scan_prefix_with_cache_mode(family, key, MetadataTableCacheMode::Populate)
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
        let cache_mode = if matching_segment_count <= SMALL_SCAN_CACHE_SEGMENT_LIMIT {
            MetadataTableCacheMode::Populate
        } else {
            MetadataTableCacheMode::ReadOnly
        };
        self.scan_prefix_with_cache_mode(family, prefix, cache_mode)
            .await
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
        self.scan_range_page_rows(family, lower_bound, upper_bound, limit)
            .await
    }

    async fn scan_prefix_with_cache_mode(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
        cache_mode: MetadataTableCacheMode,
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
            )
            .await?;
        }
        Ok(rows)
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
                if descriptor_may_intersect_range(descriptor, lower_bound, upper_bound) {
                    matching_descriptors.push((context, descriptor.clone()));
                }
            }
        }
        matching_descriptors.sort_by(|(_, left), (_, right)| {
            left.min_key
                .cmp(&right.min_key)
                .then(left.max_key.cmp(&right.max_key))
                .then(left.object_key.cmp(&right.object_key))
        });
        let cache_mode = if matching_descriptors.len() <= SMALL_SCAN_CACHE_SEGMENT_LIMIT {
            MetadataTableCacheMode::Populate
        } else {
            MetadataTableCacheMode::ReadOnly
        };

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
                        self.segment_rows(*context, family, descriptor, cache_mode)
                    }),
            )
            .await?;
            next_descriptor_index = chunk_end;

            rows.extend(loaded_segments.into_iter().flatten().filter_map(|row| {
                let row_key = row.row_key_for_family(family);
                if row_key.as_str() < lower_bound {
                    return None;
                }
                if upper_bound
                    .map(|upper_bound| row_key.as_str() >= upper_bound)
                    .unwrap_or(false)
                {
                    return None;
                }
                Some((row_key, row))
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

        let mut loaded_segments = Vec::new();
        for chunk in matching_descriptors.chunks(MAX_MATERIALIZED_TABLE_FETCHES) {
            loaded_segments.extend(
                try_join_all(chunk.iter().map(|descriptor| {
                    self.segment_rows(context, family, descriptor, output.cache_mode)
                }))
                .await?,
            );
        }

        for segment_rows in loaded_segments {
            output.rows.extend(
                segment_rows
                    .into_iter()
                    .filter(|row| row.row_key_for_family(family).starts_with(prefix)),
            );
        }
        Ok(())
    }

    async fn segment_rows(
        &self,
        context: MetadataTableLoadContext<'_>,
        family: MetadataTableFamily,
        descriptor: &MetadataFileRef,
        cache_mode: MetadataTableCacheMode,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        if let Some(rows) = self
            .segment_cache
            .lock()
            .expect("manifest segment cache lock poisoned")
            .get(&descriptor.object_key)
        {
            return Ok(rows.clone());
        }
        let rows = load_manifest_segment_rows_with_cache(
            self.store,
            self.table_cache,
            context,
            family,
            descriptor,
            cache_mode,
        )
        .await?;
        self.segment_cache
            .lock()
            .expect("manifest segment cache lock poisoned")
            .insert(descriptor.object_key.clone(), rows.clone());
        Ok(rows)
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
