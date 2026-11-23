//! The numbering of one commit's operations and deltas.

use crate::commit::CommitValidationError;

/// The next operation index and the next delta index of one commit.
#[derive(Debug, Default)]
pub(super) struct CommitNumbering {
    next_op_index: u32,
    next_delta_index: u32,
}

/// The index of one write in its commit, which the WAL stores as the delta's
/// semantic operation index. A copy to a vacant name makes two writes, so it
/// reserves two indexes.
///
/// Only [`CommitNumbering::reserve_op_index`] makes one, and recording deltas
/// consumes it, so a write records its deltas only under an index that it
/// reserved, and only once.
#[derive(Debug)]
pub(super) struct OperationIndex(u32);

impl OperationIndex {
    pub(super) fn into_u32(self) -> u32 {
        self.0
    }
}

impl CommitNumbering {
    pub(super) fn reserve_op_index(
        &mut self,
    ) -> std::result::Result<OperationIndex, CommitValidationError> {
        let op_index = self.next_op_index;
        self.next_op_index = self
            .next_op_index
            .checked_add(1)
            .ok_or(CommitValidationError::OperationIndexOverflow)?;
        Ok(OperationIndex(op_index))
    }

    pub(super) fn reserve_delta_index(
        &mut self,
    ) -> std::result::Result<u32, CommitValidationError> {
        let delta_index = self.next_delta_index;
        self.next_delta_index = self
            .next_delta_index
            .checked_add(1)
            .ok_or(CommitValidationError::DeltaIndexOverflow)?;
        Ok(delta_index)
    }
}
