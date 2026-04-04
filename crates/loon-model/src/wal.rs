use crate::metadata::{MetadataApplyError, MetadataState};
use loon_api::{ChangeSeq, HeadState, InodeId, WalOp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayedWalTail {
    pub resulting_head: HeadState,
    pub resulting_metadata_state: MetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalReplayError {
    MetadataApply(MetadataApplyError),
}

pub fn replay_wal_tail_with_metadata(
    basis_head: &HeadState,
    basis_metadata_state: &MetadataState,
    wal_tail: &[(ChangeSeq, Vec<WalOp>)],
) -> Result<ReplayedWalTail, WalReplayError> {
    let mut current_head = basis_head.clone();
    let mut current_metadata_state = basis_metadata_state.clone();

    for (seq, ops) in wal_tail {
        let applied = current_metadata_state
            .apply_committed_wal_ops(*seq, ops)
            .map_err(WalReplayError::MetadataApply)?;
        current_head.seq = *seq;
        current_head.next_inode_id = replay_next_inode_id(current_head.next_inode_id, ops);
        current_metadata_state = applied.metadata_state;
    }

    Ok(ReplayedWalTail {
        resulting_head: current_head,
        resulting_metadata_state: current_metadata_state,
    })
}

fn replay_next_inode_id(current: InodeId, ops: &[WalOp]) -> InodeId {
    let create_count = ops
        .iter()
        .filter(|op| matches!(op, WalOp::CreateDir { .. } | WalOp::CreateFile { .. }))
        .count() as u64;
    InodeId(current.0.saturating_add(create_count))
}
