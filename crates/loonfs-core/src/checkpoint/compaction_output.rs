//! Writing a merge's output: one segment builder per family, rolled to the
//! store every time the segment row budget fills.
//!
//! Both reorganization paths write through this module. Each builder holds at
//! most one segment, so memory use is bounded by the segment row limit rather
//! than the size of the family group.
//!
//! Background jobs write segments under a leased staging prefix before
//! publishing them. A merge completed within one maintenance step writes to
//! `metadata/segments/` because it publishes those segments in the same step.

use super::build::{write_manifest_segment_with_encoded_rows, MetadataSegmentDestination};
use super::reorganize::MergePlacement;
use super::runs::MetadataLsmPolicy;
use crate::error::Result;
use loonfs_api::wire::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
use loonfs_api::RunNo;
use loonfs_objectstore::ObjectStore;

/// Accumulates one family's surviving rows and uploads a segment every time
/// the segment row budget fills.
pub(super) struct MergeSegmentWriter<'a> {
    family: MetadataRowFamily,
    destination: MetadataSegmentDestination<'a>,
    run_no: RunNo,
    placement: MergePlacement,
    rows: Vec<MetadataRow>,
    next_segment_index: u32,
    segments: Vec<MetadataSegmentRef>,
}

impl<'a> MergeSegmentWriter<'a> {
    pub(super) fn new(
        family: MetadataRowFamily,
        destination: MetadataSegmentDestination<'a>,
        run_no: RunNo,
        placement: MergePlacement,
    ) -> Self {
        Self {
            family,
            destination,
            run_no,
            placement,
            rows: Vec::new(),
            next_segment_index: 0,
            segments: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, row: MetadataRow) {
        self.rows.push(row);
    }

    pub(super) async fn roll_full_segments<
        S: ObjectStore + ?Sized,
        FoldEncodedRow: FnMut(&[u8]),
    >(
        &mut self,
        store: &S,
        policy: MetadataLsmPolicy,
        fold_encoded_row: &mut FoldEncodedRow,
    ) -> Result<()> {
        let max_rows = policy.max_rows_per_segment.get();
        while self.rows.len() >= max_rows {
            let rest = self.rows.split_off(max_rows);
            let full = std::mem::replace(&mut self.rows, rest);
            self.write_segment(store, full, fold_encoded_row).await?;
        }
        Ok(())
    }

    pub(super) async fn finish<S: ObjectStore + ?Sized, FoldEncodedRow: FnMut(&[u8])>(
        mut self,
        store: &S,
        fold_encoded_row: &mut FoldEncodedRow,
    ) -> Result<Vec<MetadataSegmentRef>> {
        let rest = std::mem::take(&mut self.rows);
        if !rest.is_empty() {
            self.write_segment(store, rest, fold_encoded_row).await?;
        }
        Ok(self.segments)
    }

    async fn write_segment<S: ObjectStore + ?Sized, FoldEncodedRow: FnMut(&[u8])>(
        &mut self,
        store: &S,
        rows: Vec<MetadataRow>,
        fold_encoded_row: &mut FoldEncodedRow,
    ) -> Result<()> {
        let descriptor = write_manifest_segment_with_encoded_rows(
            store,
            self.destination.write_request(
                self.run_no,
                self.placement.output_seq(),
                self.placement.output_level(),
                self.family,
                self.next_segment_index,
                rows,
            ),
            fold_encoded_row,
        )
        .await?;
        self.next_segment_index += 1;
        self.segments.push(descriptor);
        Ok(())
    }
}
