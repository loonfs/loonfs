//! Bounded pages and merges for the collector's durable mark index.
//!
//! Tables are immutable and sorted. A merge emits one page per step and keeps
//! only two input positions. Point reads binary-search pages; the bounded cache
//! also makes ordered sweep traversal reuse the same pages.

use super::validate::table as validate_extent;
use crate::error::{CoreError, Result};
use bytes::Bytes;
use loonfs_api::wire::gc::{
    decode_gc_mark_page, encode_gc_mark_page, GcMarkEntry, GcMarkPage, GcMarkPosition, GcMarkTable,
    GcMarkValue, GC_MARK_PAGE_ENTRIES,
};
use loonfs_api::{GcMarkTableId, GcRunId, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::collections::VecDeque;
use std::sync::Arc;

const CACHED_PAGE_BYTES: usize = 16 * 1024 * 1024;
const CACHED_PAGES: usize = 64;

pub(super) struct MarkTables<'a, S: ?Sized> {
    store: &'a S,
    namespace_id: &'a NamespaceId,
    gc_run_id: &'a GcRunId,
    pages: VecDeque<(usize, Arc<GcMarkPage>)>,
    cached_bytes: usize,
}

impl<'a, S: ObjectStore + ?Sized> MarkTables<'a, S> {
    pub(super) fn new(store: &'a S, namespace_id: &'a NamespaceId, gc_run_id: &'a GcRunId) -> Self {
        Self {
            store,
            namespace_id,
            gc_run_id,
            pages: VecDeque::new(),
            cached_bytes: 0,
        }
    }

    fn key(&self, table_id: &GcMarkTableId, page_no: u64) -> String {
        loonfs_objectstore::keys::gc_mark_page(self.namespace_id, self.gc_run_id, table_id, page_no)
    }

    async fn page(&mut self, table: &GcMarkTable, page_no: u64) -> Result<Arc<GcMarkPage>> {
        validate_extent(table)?;
        if page_no >= table.page_count {
            return Err(corrupt("invalid GC mark table extent"));
        }
        if let Some(index) = self
            .pages
            .iter()
            .position(|(_, page)| page.table_id == table.table_id && page.page_no == page_no)
        {
            let (size, page) = self.pages.remove(index).expect("cached page");
            validate_page_length(table, &page)?;
            self.pages.push_back((size, Arc::clone(&page)));
            return Ok(page);
        }
        let key = self.key(&table.table_id, page_no);
        let body = self
            .store
            .get_with_metadata(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?
            .ok_or_else(|| corrupt(&format!("missing GC mark page: {key}")))?;
        let page = decode_gc_mark_page(&body.bytes)
            .map_err(|error| corrupt(&format!("{key}: {error}")))?
            .into_payload();
        if page.namespace_id != *self.namespace_id
            || page.gc_run_id != *self.gc_run_id
            || page.table_id != table.table_id
            || page.page_no != page_no
        {
            return Err(corrupt(&format!(
                "GC mark page identity disagrees with {key}"
            )));
        }
        validate_page_length(table, &page)?;
        for entry in &page.entries {
            validate_entry(entry)?;
        }
        let page = Arc::new(page);
        let size = body.bytes.len();
        while self.pages.len() >= CACHED_PAGES
            || self.cached_bytes.saturating_add(size) > CACHED_PAGE_BYTES
        {
            let Some((removed, _)) = self.pages.pop_front() else {
                break;
            };
            self.cached_bytes -= removed;
        }
        if size <= CACHED_PAGE_BYTES {
            self.cached_bytes += size;
            self.pages.push_back((size, Arc::clone(&page)));
        }
        Ok(page)
    }

    /// Publishes a deterministic page. Ambiguous and duplicate writes must
    /// resolve to the same bytes before a caller may commit progress.
    pub(super) async fn write_page(
        &self,
        table_id: &GcMarkTableId,
        page_no: u64,
        entries: Vec<GcMarkEntry>,
    ) -> Result<()> {
        let key = self.key(table_id, page_no);
        let encoded = encode_gc_mark_page(GcMarkPage {
            namespace_id: self.namespace_id.clone(),
            gc_run_id: self.gc_run_id.clone(),
            table_id: table_id.clone(),
            page_no,
            entries,
        })
        .map_err(|error| corrupt(&error.to_string()))?;
        let bytes = Bytes::from(encoded.into_bytes());
        match self.store.put_if_absent(&key, bytes.clone()).await {
            Ok(_) => Ok(()),
            Err(
                error @ (ObjectStoreError::PreconditionFailed { .. }
                | ObjectStoreError::Transport { .. }),
            ) => {
                let confirmed = self
                    .store
                    .get_with_metadata(&key)
                    .await
                    .map_err(|error| CoreError::store(&key, &error))?;
                match confirmed {
                    Some(body) if body.bytes == bytes => Ok(()),
                    Some(_) => Err(corrupt(&format!(
                        "different bytes already occupy GC mark page {key}"
                    ))),
                    None => Err(CoreError::store(&key, &error)),
                }
            }
            Err(error) => Err(CoreError::store(&key, &error)),
        }
    }

    pub(super) async fn lookup(
        &mut self,
        table: &GcMarkTable,
        key: &str,
    ) -> Result<Option<GcMarkEntry>> {
        validate_extent(table)?;
        let (mut low, mut high) = (0, table.page_count);
        while low < high {
            let middle = low + (high - low) / 2;
            let page = self.page(table, middle).await?;
            if key < page.entries[0].key.as_str() {
                high = middle;
            } else if key > page.entries.last().expect("nonempty page").key.as_str() {
                low = middle + 1;
            } else {
                return Ok(page
                    .entries
                    .binary_search_by(|entry| entry.key.as_str().cmp(key))
                    .ok()
                    .map(|i| page.entries[i].clone()));
            }
        }
        Ok(None)
    }

    pub(super) async fn peek(
        &mut self,
        table: &GcMarkTable,
        position: GcMarkPosition,
    ) -> Result<Option<GcMarkEntry>> {
        validate_extent(table)?;
        if position.page_no == table.page_count && position.entry_no == 0 {
            return Ok(None);
        }
        let page = self.page(table, position.page_no).await?;
        page.entries
            .get(position.entry_no as usize)
            .cloned()
            .map(Some)
            .ok_or_else(|| corrupt("GC mark cursor exceeds page"))
    }

    pub(super) fn advance(table: &GcMarkTable, position: &mut GcMarkPosition) {
        position.entry_no += 1;
        let consumed =
            position.page_no * GC_MARK_PAGE_ENTRIES as u64 + u64::from(position.entry_no);
        if position.entry_no as usize == GC_MARK_PAGE_ENTRIES || consumed == table.entry_count {
            position.page_no += 1;
            position.entry_no = 0;
        }
    }

    /// Consumes at most one output page. Equal keys must have equal meanings;
    /// contradictory revision descriptors are corruption, not a winner choice.
    pub(super) async fn merge_page(
        &mut self,
        inputs: &[GcMarkTable; 2],
        positions: &mut [GcMarkPosition; 2],
    ) -> Result<Vec<GcMarkEntry>> {
        let mut output = Vec::with_capacity(GC_MARK_PAGE_ENTRIES);
        while output.len() < GC_MARK_PAGE_ENTRIES {
            let left = self.peek(&inputs[0], positions[0]).await?;
            let right = self.peek(&inputs[1], positions[1]).await?;
            let (entry, consume) = match (left, right) {
                (None, None) => break,
                (Some(left), None) => (left, [true, false]),
                (None, Some(right)) => (right, [false, true]),
                (Some(left), Some(right)) => match left.key.cmp(&right.key) {
                    std::cmp::Ordering::Less => (left, [true, false]),
                    std::cmp::Ordering::Greater => (right, [false, true]),
                    std::cmp::Ordering::Equal => (merge_equal(left, right)?, [true, true]),
                },
            };
            output.push(entry);
            for i in 0..2 {
                if consume[i] {
                    Self::advance(&inputs[i], &mut positions[i]);
                }
            }
        }
        Ok(output)
    }
}

/// Identical segment bytes may be covered by several manifest sequence bounds.
/// Keep the strictest bound, and reject every other disagreement.
pub(super) fn merge_equal(mut left: GcMarkEntry, right: GcMarkEntry) -> Result<GcMarkEntry> {
    if left.key != right.key {
        return Err(corrupt("cannot merge distinct GC mark keys"));
    }
    if left.value == right.value {
        return Ok(left);
    }
    match (&mut left.value, right.value) {
        (
            GcMarkValue::RevisionSegment { segment, max_seq },
            GcMarkValue::RevisionSegment {
                segment: other,
                max_seq: other_max,
            },
        ) if *segment == other => {
            *max_seq = (*max_seq).min(other_max);
            Ok(left)
        }
        _ => Err(corrupt("conflicting values for one GC mark key")),
    }
}

fn validate_page_length(table: &GcMarkTable, page: &GcMarkPage) -> Result<()> {
    let expected = (table.entry_count - page.page_no * GC_MARK_PAGE_ENTRIES as u64)
        .min(GC_MARK_PAGE_ENTRIES as u64);
    if page.entries.len() as u64 != expected {
        return Err(corrupt("GC mark page length disagrees with table extent"));
    }
    Ok(())
}

fn corrupt(message: &str) -> CoreError {
    CoreError::NamespaceCorrupt(message.to_owned())
}

fn validate_entry(entry: &GcMarkEntry) -> Result<()> {
    use loonfs_objectstore::keys::{metadata_manifest_object, metadata_segment_object_key};
    let valid =
        match &entry.value {
            GcMarkValue::Object {} => entry
                .key
                .strip_prefix("object/")
                .is_some_and(|key| loonfs_objectstore::layout::parse_object_key(key).is_some()),
            GcMarkValue::Manifest { manifest } => {
                entry.key
                    == format!(
                        "object/{}",
                        metadata_manifest_object(
                            &manifest.owner_namespace_id,
                            &manifest.manifest_object_id
                        )
                    )
            }
            GcMarkValue::Content {} => entry
                .key
                .strip_prefix("content/")
                .is_some_and(|id| loonfs_api::ContentId::parse(id).is_ok()),
            GcMarkValue::RevisionSegment { segment, .. } => {
                entry.key == format!("revision/{}", metadata_segment_object_key(segment))
                    && segment.family == loonfs_api::wire::manifest::MetadataRowFamily::Revisions
            }
            GcMarkValue::MissingBasisCheckpoint {} => {
                entry.key.strip_prefix("missing-basis/").is_some_and(|key| {
                    loonfs_objectstore::layout::parse_object_key(key).is_some_and(|parsed| {
                        parsed.family()
                            == loonfs_objectstore::layout::DurableObjectFamily::CheckpointRecord
                    })
                })
            }
            GcMarkValue::MissingManifest {} => entry
                .key
                .strip_prefix("missing-manifest/")
                .is_some_and(|key| {
                    matches!(
                        loonfs_objectstore::layout::manifest_object_id_of(key),
                        Some(Ok(_))
                    )
                }),
        };
    if !valid {
        return Err(corrupt("GC mark key disagrees with its value"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::gc::GcMarkValue;
    use loonfs_objectstore::local_fs_store::LocalFsStore;

    fn entry(number: u64) -> GcMarkEntry {
        GcMarkEntry {
            key: format!(
                "object/namespaces/demo/wal/segments/wal_{number:020}-0000000000000000.wal.zst"
            ),
            value: GcMarkValue::Object {},
        }
    }

    async fn table<S: ObjectStore + ?Sized>(
        tables: &MarkTables<'_, S>,
        id: &str,
        entries: &[GcMarkEntry],
    ) -> GcMarkTable {
        let table_id = GcMarkTableId::parse(id).expect("table id");
        for (page_no, page) in entries.chunks(GC_MARK_PAGE_ENTRIES).enumerate() {
            tables
                .write_page(&table_id, page_no as u64, page.to_vec())
                .await
                .expect("write input page");
        }
        GcMarkTable {
            table_id,
            page_count: entries.len().div_ceil(GC_MARK_PAGE_ENTRIES) as u64,
            entry_count: entries.len() as u64,
        }
    }

    #[tokio::test]
    async fn merges_resume_from_serialized_positions_without_loading_the_whole_set() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(dir.path()).expect("store");
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let run = GcRunId::parse("gcr_0123456789abcdef0123456789abcdef").expect("run");
        let mut tables = MarkTables::new(&store, &namespace, &run);
        let left: Vec<_> = (0..4000).step_by(2).map(entry).collect();
        let right: Vec<_> = (0..4000).step_by(3).map(entry).collect();
        let inputs = [
            table(&tables, "gct_11111111111111111111111111111111", &left).await,
            table(&tables, "gct_22222222222222222222222222222222", &right).await,
        ];
        let mut positions = [GcMarkPosition::default(); 2];
        let mut merged = Vec::new();
        loop {
            let page = tables
                .merge_page(&inputs, &mut positions)
                .await
                .expect("merge step");
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= GC_MARK_PAGE_ENTRIES);
            merged.extend(page);
            let saved = serde_json::to_vec(&positions).expect("save progress");
            positions = serde_json::from_slice(&saved).expect("resume progress");
            tables = MarkTables::new(&store, &namespace, &run);
        }
        let expected: Vec<_> = (0..4000)
            .filter(|n| n % 2 == 0 || n % 3 == 0)
            .map(entry)
            .collect();
        assert_eq!(merged, expected);
        let output = table(&tables, "gct_33333333333333333333333333333333", &merged).await;
        for n in 0..4001 {
            assert_eq!(
                tables
                    .lookup(&output, &entry(n).key)
                    .await
                    .expect("point read"),
                (n < 4000 && (n % 2 == 0 || n % 3 == 0)).then(|| entry(n))
            );
        }
        assert!(tables.pages.len() <= CACHED_PAGES);
    }

    #[tokio::test]
    async fn missing_pages_and_conflicting_meanings_never_prove_absence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(dir.path()).expect("store");
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let run = GcRunId::parse("gcr_0123456789abcdef0123456789abcdef").expect("run");
        let mut tables = MarkTables::new(&store, &namespace, &run);
        let left = table(&tables, "gct_11111111111111111111111111111111", &[entry(1)]).await;
        let mut other = entry(1);
        other.value = GcMarkValue::Content {};
        let right = table(&tables, "gct_22222222222222222222222222222222", &[other]).await;
        assert!(tables
            .merge_page(&[left.clone(), right], &mut [GcMarkPosition::default(); 2])
            .await
            .is_err());
        store
            .delete(&tables.key(&left.table_id, 0))
            .await
            .expect("remove page");
        let mut resumed = MarkTables::new(&store, &namespace, &run);
        assert!(resumed.lookup(&left, &entry(1).key).await.is_err());
    }
}
