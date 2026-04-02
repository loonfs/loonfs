use loon_types::FenceToken;
pub use loon_types::{HeadState, HeadStateEnvelope, LeaseState, LeaseStateEnvelope};
use thiserror::Error;

pub fn head_and_lease_fence_tokens_agree(head: &HeadState, lease: &LeaseState) -> bool {
    head.namespace_id == lease.namespace_id && head.active_fence_token == lease.fence_token
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HeadFenceTakeoverError {
    #[error("head fence token overflow from `{active:?}`")]
    FenceTokenOverflow { active: FenceToken },
}

pub fn next_takeover_head(current_head: &HeadState) -> Result<HeadState, HeadFenceTakeoverError> {
    let next_fence = current_head.active_fence_token.0.checked_add(1).ok_or(
        HeadFenceTakeoverError::FenceTokenOverflow {
            active: current_head.active_fence_token,
        },
    )?;

    Ok(HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: current_head.seq,
        active_fence_token: FenceToken(next_fence),
        next_inode_id: current_head.next_inode_id,
        snapshot_hint_seq: current_head.snapshot_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
    })
}

#[cfg(test)]
mod tests {
    use super::{head_and_lease_fence_tokens_agree, next_takeover_head};
    use loon_types::{ChangeSeq, FenceToken, HeadState, InodeId, LeaseState, NamespaceId};

    #[test]
    fn head_and_lease_tokens_must_match() {
        let head = HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(41),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };
        let lease = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(8),
            lease_expires_at_ms: 1_000,
        };

        assert!(head_and_lease_fence_tokens_agree(&head, &lease));
    }

    #[test]
    fn takeover_head_rotates_only_active_fence_token() {
        let head = HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(41),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };

        let rotated = next_takeover_head(&head).expect("rotate takeover head");

        assert_eq!(rotated.namespace_id, head.namespace_id);
        assert_eq!(rotated.seq, head.seq);
        assert_eq!(rotated.active_fence_token, FenceToken(9));
        assert_eq!(rotated.next_inode_id, head.next_inode_id);
        assert_eq!(rotated.snapshot_hint_seq, head.snapshot_hint_seq);
        assert_eq!(rotated.retention_floor_seq, head.retention_floor_seq);
    }
}
