use crate::{ChangeSeq, FenceToken, InodeId, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadState {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub snapshot_hint_seq: Option<ChangeSeq>,
    pub retention_floor_seq: ChangeSeq,
}

impl HeadState {
    pub fn initial(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            seq: ChangeSeq(0),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(2),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseState {
    pub namespace_id: NamespaceId,
    pub holder_id: String,
    pub fence_token: FenceToken,
    pub lease_expires_at_ms: u64,
}

impl LeaseState {
    pub fn is_valid_at(&self, now_ms: u64) -> bool {
        self.lease_expires_at_ms > now_ms
    }
}
