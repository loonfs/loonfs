use crate::metadata::{MetadataApplyError, MetadataState};
use loon_api::wire::control::HeadState;
use loon_api::wire::wal::WalDelta;
use loon_api::{ChangeSeq, InodeId};
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
    wal_tail: &[(ChangeSeq, Vec<WalDelta>)],
) -> Result<ReplayedWalTail, WalReplayError> {
    let mut current_head = basis_head.clone();
    let mut current_metadata_state = basis_metadata_state.clone();

    for (seq, deltas) in wal_tail {
        let applied = current_metadata_state
            .apply_committed_wal_deltas(*seq, deltas)
            .map_err(WalReplayError::MetadataApply)?;
        current_head.seq = *seq;
        current_head.next_inode_id = replay_next_inode_id(current_head.next_inode_id, deltas);
        current_metadata_state = applied.metadata_state;
    }

    Ok(ReplayedWalTail {
        resulting_head: current_head,
        resulting_metadata_state: current_metadata_state,
    })
}

fn replay_next_inode_id(current: InodeId, deltas: &[WalDelta]) -> InodeId {
    deltas.iter().fold(current, |next, delta| match delta {
        WalDelta::CreateInode { inode_id, .. } => InodeId(next.0.max(inode_id.0.saturating_add(1))),
        WalDelta::BindDirentry { .. }
        | WalDelta::UnbindDirentry { .. }
        | WalDelta::AppendFileRevision { .. }
        | WalDelta::TombstoneSubtree { .. } => next,
    })
}
