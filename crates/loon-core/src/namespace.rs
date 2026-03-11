pub use loon_types::{HeadState, HeadStateEnvelope, LeaseState, LeaseStateEnvelope};

pub fn head_and_lease_fence_tokens_agree(head: &HeadState, lease: &LeaseState) -> bool {
    head.namespace_id == lease.namespace_id && head.active_fence_token == lease.fence_token
}

#[cfg(test)]
mod tests {
    use super::head_and_lease_fence_tokens_agree;
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
}
