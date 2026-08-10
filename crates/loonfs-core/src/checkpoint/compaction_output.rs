//! Writing a merge's output: one segment builder per family, rolled to the
//! store every time the segment row budget fills.
//!
//! Both reorganization paths write through this. What a builder holds is one
//! segment's worth of rows, so output residency is the segment row budget and
//! not the size of the group being merged.
//!
//! The paths differ in one thing, and [`MergeOutput`] is it: the key a segment
//! lands under. A background job's segments are written long before the
//! manifest that names them, so they go under the job's own prefix where a
//! lease speaks for them (format spec, "Compaction"). A merge that runs inside
//! a maintenance step publishes in that same step, so its segments go to
//! `metadata/tables/` like any other freshly written run and are covered by
//! the ordinary write-time grace.

use super::build::{write_manifest_segment, MetadataSstWriteRequest};
use super::reorganize::MergePlacement;
use super::runs::MetadataLsmPolicy;
use crate::error::Result;
use loonfs_api::wire::manifest::{MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::{MetadataCompactionId, MetadataTableId, NamespaceId};
use loonfs_objectstore::keys::{metadata_compaction_table, metadata_table};
use loonfs_objectstore::ObjectStore;

/// Where a merge's output segments are written.
#[derive(Debug, Clone, Copy)]
pub(super) enum MergeOutput<'a> {
    /// Under the job's own prefix, for a merge whose publication comes much
    /// later. The lease beside the segments is what tells garbage collection
    /// that objects far older than any grace window still have an owner.
    JobPrefix(&'a MetadataCompactionId),
    /// At ordinary table keys, for a merge that publishes inside the step that
    /// ran it. Its segments are seconds old when the manifest names them, so
    /// the ordinary write-time grace already covers them and there is nothing
    /// for a lease to say.
    Tables,
}

impl MergeOutput<'_> {
    fn object_key(&self, namespace_id: &NamespaceId, table_id: &MetadataTableId) -> String {
        match self {
            Self::JobPrefix(job_id) => {
                metadata_compaction_table(namespace_id.as_str(), job_id.as_str(), table_id.as_str())
            }
            Self::Tables => metadata_table(namespace_id.as_str(), table_id.as_str()),
        }
    }
}

/// Accumulates one family's surviving rows and uploads a segment every time
/// the segment row budget fills.
pub(super) struct MergeSegmentWriter<'a> {
    family: MetadataTableFamily,
    output: MergeOutput<'a>,
    placement: MergePlacement,
    rows: Vec<MetadataRow>,
    next_segment_index: u32,
    segments: Vec<MetadataFileRef>,
}

impl<'a> MergeSegmentWriter<'a> {
    pub(super) fn new(
        family: MetadataTableFamily,
        output: MergeOutput<'a>,
        placement: MergePlacement,
    ) -> Self {
        Self {
            family,
            output,
            placement,
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
        policy: MetadataLsmPolicy,
    ) -> Result<()> {
        let max_rows = policy.max_rows_per_segment.get();
        while self.rows.len() >= max_rows {
            let rest = self.rows.split_off(max_rows);
            let full = std::mem::replace(&mut self.rows, rest);
            self.write_segment(store, namespace_id, full).await?;
        }
        Ok(())
    }

    pub(super) async fn finish<S: ObjectStore + ?Sized>(
        mut self,
        store: &S,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<MetadataFileRef>> {
        let rest = std::mem::take(&mut self.rows);
        if !rest.is_empty() {
            self.write_segment(store, namespace_id, rest).await?;
        }
        Ok(self.segments)
    }

    async fn write_segment<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        rows: Vec<MetadataRow>,
    ) -> Result<()> {
        let table_id = MetadataTableId::generate();
        let object_key = self.output.object_key(namespace_id, &table_id);
        let descriptor = write_manifest_segment(
            store,
            MetadataSstWriteRequest::new(
                namespace_id,
                table_id,
                object_key,
                self.placement.output_seq(),
                self.placement.output_level(),
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
