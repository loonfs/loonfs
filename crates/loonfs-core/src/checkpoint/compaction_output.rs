//! Writing a streaming compaction's output: one segment builder per family,
//! rolled to the store every time the segment row budget fills.
//!
//! Segments go to the job's own prefix rather than to `metadata/tables/`
//! (format spec, "Compaction"), and nothing references them until the job's
//! one manifest publication names them. What a builder holds is one segment's
//! worth of rows, so the job's output residency is the segment row budget and
//! not the size of the group it rebuilds.

use super::build::{write_manifest_segment, MetadataSstWriteRequest};
use super::runs::MetadataLsmPolicy;
use super::streaming_compaction::MetadataCompactionSpec;
use crate::error::Result;
use loonfs_api::wire::manifest::{MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::{MetadataTableId, NamespaceId};
use loonfs_objectstore::keys::metadata_compaction_table;
use loonfs_objectstore::ObjectStore;

/// Accumulates one family's surviving rows and uploads a segment every time
/// the segment row budget fills.
pub(super) struct StagedSegmentWriter {
    family: MetadataTableFamily,
    rows: Vec<MetadataRow>,
    next_segment_index: u32,
    segments: Vec<MetadataFileRef>,
}

impl StagedSegmentWriter {
    pub(super) fn new(family: MetadataTableFamily) -> Self {
        Self {
            family,
            rows: Vec::new(),
            next_segment_index: 0,
            segments: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, row: MetadataRow) {
        self.rows.push(row);
    }

    pub(super) async fn roll_full_segments<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
        policy: MetadataLsmPolicy,
    ) -> Result<()> {
        let max_rows = policy.max_rows_per_segment.get();
        while self.rows.len() >= max_rows {
            let rest = self.rows.split_off(max_rows);
            let full = std::mem::replace(&mut self.rows, rest);
            self.write_segment(store, namespace_id, spec, full).await?;
        }
        Ok(())
    }

    pub(super) async fn finish<S: ObjectStore + ?Sized>(
        mut self,
        store: &S,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
    ) -> Result<Vec<MetadataFileRef>> {
        let rest = std::mem::take(&mut self.rows);
        if !rest.is_empty() {
            self.write_segment(store, namespace_id, spec, rest).await?;
        }
        Ok(self.segments)
    }

    async fn write_segment<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
        rows: Vec<MetadataRow>,
    ) -> Result<()> {
        let table_id = MetadataTableId::generate();
        let object_key = metadata_compaction_table(
            namespace_id.as_str(),
            spec.job_id().as_str(),
            table_id.as_str(),
        );
        let descriptor = write_manifest_segment(
            store,
            MetadataSstWriteRequest::new(
                namespace_id,
                table_id,
                object_key,
                spec.placement().output_seq(),
                spec.placement().output_level(),
                self.family,
                self.next_segment_index,
                rows,
            ),
        )
        .await?;
        self.next_segment_index += 1;
        self.segments.push(descriptor);
        Ok(())
    }
}
