//! Inode allocation for one candidate of a publish batch.

use crate::error::{CoreError, Result};
use loonfs_types::InodeId;

/// Returns the next inode ID, or `None` at the limit.
pub(crate) fn next_inode_after(inode_id: InodeId) -> Option<InodeId> {
    inode_id.0.checked_add(1).map(InodeId)
}

/// A candidate-local fork of the batch's next inode ID.
///
/// Planning advances the fork while it assigns IDs to create operations. An
/// accepted candidate's plan carries the fork's final position, and a
/// rejected candidate leaves the batch's position unchanged.
#[derive(Debug)]
pub(crate) struct CandidateAllocation {
    next: InodeId,
}

impl CandidateAllocation {
    pub(crate) fn new(next: InodeId) -> Self {
        Self { next }
    }

    pub(crate) fn allocate(&mut self) -> Result<InodeId> {
        let allocated = self.next;
        self.next = next_inode_after(allocated).ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "the namespace's next inode ID is {allocated}, the largest inode ID, so no \
                 inode can be created"
            ))
        })?;
        Ok(allocated)
    }

    pub(crate) fn resulting_next_inode_id(&self) -> InodeId {
        self.next
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn multiple_creates_in_one_candidate_receive_consecutive_ids() {
        let mut candidate = CandidateAllocation::new(InodeId(2));

        assert_eq!(candidate.allocate().expect("first create"), InodeId(2));
        assert_eq!(candidate.allocate().expect("second create"), InodeId(3));
        assert_eq!(candidate.allocate().expect("third create"), InodeId(4));
        assert_eq!(candidate.resulting_next_inode_id(), InodeId(5));
    }

    #[test]
    fn allocation_refuses_to_overflow_the_next_inode_boundary() {
        let mut candidate = CandidateAllocation::new(InodeId(u64::MAX));

        let error = candidate.allocate().expect_err("counter must not wrap");
        assert_matches!(
            error,
            CoreError::NamespaceCorrupt(message) if message.contains("largest inode ID")
        );
        assert_eq!(candidate.resulting_next_inode_id(), InodeId(u64::MAX));
    }
}
