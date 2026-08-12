//! Transactional inode allocation for one publish batch.

use crate::error::{CoreError, Result};
use loonfs_api::InodeId;

/// The one authoritative next-inode position for a tentative publish batch.
#[derive(Debug)]
pub(crate) struct InodeAllocator {
    next: InodeId,
}

/// A candidate-local fork of the batch allocator.
///
/// Planning advances this fork while assigning IDs directly to create
/// operations. Acceptance commits its final position; rejection discards it
/// without changing the batch allocator.
#[derive(Debug)]
pub(crate) struct CandidateAllocation {
    start: InodeId,
    next: InodeId,
}

impl InodeAllocator {
    pub(crate) fn new(next: InodeId) -> Self {
        Self { next }
    }

    pub(crate) fn begin_candidate(&self) -> CandidateAllocation {
        CandidateAllocation {
            start: self.next,
            next: self.next,
        }
    }

    pub(crate) fn commit_candidate(&mut self, candidate: CandidateAllocation) -> Result<InodeId> {
        if candidate.start != self.next {
            // A candidate belongs to the allocator position it forked from;
            // accepting it later would rewind or skip IDs from another one.
            return Err(CoreError::Internal(
                "inode allocation candidate was committed out of order".to_owned(),
            ));
        }
        self.next = candidate.next;
        Ok(self.next)
    }

    pub(crate) fn discard_candidate(&self, _candidate: CandidateAllocation) {
        // Candidate allocation is forked state, so consuming it is the
        // rollback: the authoritative batch position never moved.
    }
}

impl CandidateAllocation {
    pub(crate) fn allocate(&mut self) -> Result<InodeId> {
        let allocated = self.next;
        self.next = self
            .next
            .0
            .checked_add(1)
            .map(InodeId)
            .ok_or_else(|| CoreError::Internal("next inode id counter overflow".to_owned()))?;
        Ok(allocated)
    }

    pub(crate) fn resulting_next_inode_id(&self) -> InodeId {
        self.next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multiple_creates_in_one_candidate_receive_consecutive_ids() {
        let mut allocator = InodeAllocator::new(InodeId(2));
        let mut candidate = allocator.begin_candidate();

        assert_eq!(candidate.allocate().expect("first create"), InodeId(2));
        assert_eq!(candidate.allocate().expect("second create"), InodeId(3));
        assert_eq!(candidate.allocate().expect("third create"), InodeId(4));
        assert_eq!(
            allocator.commit_candidate(candidate).expect("commit"),
            InodeId(5)
        );
    }

    #[test]
    fn accepted_candidate_followed_by_rejected_candidate_keeps_only_accepted_ids() {
        let mut allocator = InodeAllocator::new(InodeId(2));
        let mut accepted = allocator.begin_candidate();
        assert_eq!(accepted.allocate().expect("accepted create"), InodeId(2));
        allocator
            .commit_candidate(accepted)
            .expect("commit accepted");

        let mut rejected = allocator.begin_candidate();
        assert_eq!(rejected.allocate().expect("rejected create"), InodeId(3));
        allocator.discard_candidate(rejected);

        let next = allocator.begin_candidate();
        assert_eq!(next.resulting_next_inode_id(), InodeId(3));
    }

    #[test]
    fn rejected_candidate_followed_by_accepted_candidate_reuses_the_discarded_id() {
        let mut allocator = InodeAllocator::new(InodeId(2));
        let mut rejected = allocator.begin_candidate();
        assert_eq!(rejected.allocate().expect("rejected create"), InodeId(2));
        allocator.discard_candidate(rejected);

        let mut accepted = allocator.begin_candidate();
        assert_eq!(accepted.allocate().expect("accepted create"), InodeId(2));
        assert_eq!(
            allocator
                .commit_candidate(accepted)
                .expect("commit accepted"),
            InodeId(3)
        );
    }

    #[test]
    fn multiple_accepted_candidates_advance_one_batch_allocator() {
        let mut allocator = InodeAllocator::new(InodeId(2));
        for expected in [InodeId(2), InodeId(3), InodeId(4)] {
            let mut candidate = allocator.begin_candidate();
            assert_eq!(candidate.allocate().expect("create"), expected);
            allocator.commit_candidate(candidate).expect("commit");
        }

        let next = allocator.begin_candidate();
        assert_eq!(next.resulting_next_inode_id(), InodeId(5));
    }

    #[test]
    fn retry_from_the_same_head_replays_the_same_allocations() {
        let allocator = InodeAllocator::new(InodeId(7));
        let mut first_attempt = allocator.begin_candidate();
        let first_ids = [
            first_attempt.allocate().expect("first id"),
            first_attempt.allocate().expect("second id"),
        ];
        allocator.discard_candidate(first_attempt);

        let mut retry = allocator.begin_candidate();
        let retry_ids = [
            retry.allocate().expect("retry first id"),
            retry.allocate().expect("retry second id"),
        ];

        assert_eq!(retry_ids, first_ids);
    }

    #[test]
    fn allocation_refuses_to_overflow_the_next_inode_boundary() {
        let allocator = InodeAllocator::new(InodeId(u64::MAX));
        let mut candidate = allocator.begin_candidate();

        let error = candidate.allocate().expect_err("counter must not wrap");
        assert!(matches!(error, CoreError::Internal(message) if message.contains("overflow")));
        assert_eq!(candidate.resulting_next_inode_id(), InodeId(u64::MAX));
    }
}
