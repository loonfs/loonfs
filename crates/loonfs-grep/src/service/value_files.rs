//! The files of one candidate value that a page examines: the index's
//! `content_revisions` rows for the value, combined with the value's tail and
//! scan files.

use super::candidates::CandidateValue;
use super::page_walk::GrepResume;
use super::{QueryPlan, GREP_READS_IN_FLIGHT};
use crate::codec::{lookup, ContentValue, IndexRow};
use crate::{GrepError, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys::after_row_key;
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{InodeId, RevisionNo};
use std::collections::{BTreeMap, VecDeque};

/// Data blocks of each segment that one read of a value's `content_revisions`
/// rows reads. A data block stores several hundred of these rows, and the
/// read is bounded so that a value that many retained revisions refer to
/// costs a page about the rows that the page examines.
const CONTENT_REVISION_READ_BLOCKS: usize = 4;

/// The files of one candidate value in inode order: the newest revision at
/// or below the query's cut that the index's `content_revisions` rows record
/// for each inode, combined with the value's tail and scan files.
pub(super) struct ValueFiles {
    pub(super) value: ContentValue,
    /// Where the next read of the value's rows starts, or `None` once the
    /// rows are exhausted or the value isn't indexed.
    next_lower: Option<String>,
    /// The inode whose rows the read is in, and whether its revision at or
    /// below the cut is found.
    current_inode: Option<(InodeId, bool)>,
    from_index: VecDeque<(InodeId, RevisionNo)>,
    /// The value's tail and scan files. A file here takes the place of the
    /// index's row for the same inode, because its revision is the inode's
    /// revision at the read sequence.
    files: BTreeMap<InodeId, RevisionNo>,
}

impl ValueFiles {
    /// Returns the files of `value` with the value's first rows read, so a
    /// page walk can read the rows of the values ahead of the one it
    /// examines.
    pub(super) async fn load<S: ObjectStore>(
        plan: &QueryPlan<'_, S>,
        candidate: CandidateValue,
    ) -> Result<Self> {
        let mut files = Self::new(plan, candidate);
        if let Some(lower) = files.next_lower.take() {
            files.read_rows(plan, &lower).await?;
        }
        Ok(files)
    }

    fn new<S: ObjectStore>(plan: &QueryPlan<'_, S>, candidate: CandidateValue) -> Self {
        let CandidateValue { value, indexed } = candidate;
        let resume = plan
            .resume
            .as_ref()
            .filter(|resume| resume.position().value == value);
        let content_id = value.content_id();
        let inode_prefix = |inode_id| {
            lookup::content_revision_inode_prefix(&content_id, value.size_bytes, inode_id)
        };
        let lower = match resume {
            None => lookup::content_revision_prefix(&content_id, value.size_bytes),
            Some(GrepResume::InFile { position, .. }) => inode_prefix(position.inode_id),
            Some(GrepResume::AfterFile { position }) => {
                string_prefix_upper_bound(&inode_prefix(position.inode_id))
                    .expect("a row key prefix should have an upper bound")
            }
        };
        let mut files = plan
            .candidates
            .files
            .get(&value)
            .cloned()
            .unwrap_or_default();
        if indexed {
            files.extend(
                plan.candidates
                    .index_decided
                    .get(&value)
                    .into_iter()
                    .flatten(),
            );
        }
        files.retain(|inode_id, _| match resume {
            None => true,
            Some(GrepResume::InFile { position, .. }) => *inode_id >= position.inode_id,
            Some(GrepResume::AfterFile { position }) => *inode_id > position.inode_id,
        });
        Self {
            next_lower: indexed.then_some(lower),
            value,
            current_inode: None,
            from_index: VecDeque::new(),
            files,
        }
    }

    /// Returns the value's next file and its revision, or `None` once the
    /// value has no more files.
    pub(super) async fn next<S: ObjectStore>(
        &mut self,
        plan: &QueryPlan<'_, S>,
    ) -> Result<Option<(InodeId, RevisionNo)>> {
        loop {
            if self.from_index.is_empty() {
                if let Some(lower) = self.next_lower.take() {
                    self.read_rows(plan, &lower).await?;
                    continue;
                }
            }
            let next_file = self.files.first_key_value().map(|(inode_id, _)| *inode_id);
            return Ok(match (self.from_index.front(), next_file) {
                (Some((indexed_inode, _)), Some(file_inode)) if file_inode < *indexed_inode => {
                    self.files.pop_first()
                }
                (Some(_), _) => self.from_index.pop_front(),
                (None, _) => self.files.pop_first(),
            });
        }
    }

    /// Reads the value's next rows from `lower`, a bounded number of data
    /// blocks in each segment, and keeps each inode's newest revision at or
    /// below the cut.
    ///
    /// The rows are complete up to the last key that every segment read
    /// through, so the next read starts after that key.
    async fn read_rows<S: ObjectStore>(
        &mut self,
        plan: &QueryPlan<'_, S>,
        lower: &str,
    ) -> Result<()> {
        let content_id = self.value.content_id();
        let prefix = lookup::content_revision_prefix(&content_id, self.value.size_bytes);
        let probe = lookup::content_revision_probe(&content_id, self.value.size_bytes);
        let upper = string_prefix_upper_bound(&prefix);
        let segments = &plan.index_runs;
        let mut reads = stream::iter(0..segments.len())
            .map(|index| {
                plan.index.rows_in_range(
                    &segments[index],
                    Some(&probe),
                    lower,
                    upper.as_deref(),
                    CONTENT_REVISION_READ_BLOCKS,
                )
            })
            .buffered(GREP_READS_IN_FLIGHT);
        let mut rows = BTreeMap::new();
        let mut read_through: Option<String> = None;
        while let Some(range) = reads.try_next().await? {
            if let Some(through) = range.read_through {
                if read_through
                    .as_ref()
                    .is_none_or(|current| through < *current)
                {
                    read_through = Some(through);
                }
            }
            rows.extend(range.rows);
        }
        if let Some(through) = &read_through {
            rows.retain(|row_key, _| row_key <= through);
            self.next_lower = Some(after_row_key(through));
        }
        for (row_key, row) in rows {
            let IndexRow::ContentRevision(row) = row else {
                return Err(GrepError::IndexCorrupt {
                    message: format!(
                        "the grep index has a row of another family under the \
                         `content_revisions` key `{row_key}`"
                    ),
                });
            };
            if let Some((inode_id, found)) = self.current_inode {
                if inode_id == row.inode_id && found {
                    continue;
                }
            }
            let found = row.committed_seq <= plan.cut_seq;
            self.current_inode = Some((row.inode_id, found));
            if found && !self.files.contains_key(&row.inode_id) {
                self.from_index.push_back((row.inode_id, row.revision_no));
            }
        }
        Ok(())
    }
}
