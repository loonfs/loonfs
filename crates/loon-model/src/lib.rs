#![forbid(unsafe_code)]

use loon_types::{ChangeSeq, FenceToken, InodeId, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelNamespace {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub active_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelWalCommit {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub base_head_seq: ChangeSeq,
    pub commit_id: String,
    pub writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelAction {
    CreateDir {
        inode_id: InodeId,
        writer_fence_token: FenceToken,
    },
    DeleteSubtree {
        root_inode: InodeId,
        writer_fence_token: FenceToken,
    },
    BumpSeq {
        writer_fence_token: FenceToken,
    },
    RotateFence {
        new_fence_token: FenceToken,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelError {
    StaleWriterFenceToken {
        expected: FenceToken,
        actual: FenceToken,
    },
}

impl ModelNamespace {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            head_seq: ChangeSeq(0),
            active_fence_token: FenceToken(0),
        }
    }

    pub fn apply(&mut self, action: ModelAction) -> Result<(), ModelError> {
        match action {
            ModelAction::CreateDir {
                writer_fence_token, ..
            }
            | ModelAction::DeleteSubtree {
                writer_fence_token, ..
            }
            | ModelAction::BumpSeq { writer_fence_token } => {
                if writer_fence_token != self.active_fence_token {
                    return Err(ModelError::StaleWriterFenceToken {
                        expected: self.active_fence_token,
                        actual: writer_fence_token,
                    });
                }
                self.head_seq = ChangeSeq(self.head_seq.0 + 1);
                Ok(())
            }
            ModelAction::RotateFence { new_fence_token } => {
                self.active_fence_token = new_fence_token;
                Ok(())
            }
        }
    }

    pub fn prepare_wal_commit(
        &self,
        commit_id: impl Into<String>,
        writer_fence_token: FenceToken,
    ) -> Result<ModelWalCommit, ModelError> {
        if writer_fence_token != self.active_fence_token {
            return Err(ModelError::StaleWriterFenceToken {
                expected: self.active_fence_token,
                actual: writer_fence_token,
            });
        }

        Ok(ModelWalCommit {
            namespace_id: self.namespace_id.clone(),
            seq: ChangeSeq(self.head_seq.0 + 1),
            base_head_seq: self.head_seq,
            commit_id: commit_id.into(),
            writer_fence_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_advances_seq() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        assert_eq!(ns.head_seq.0, 1);
    }

    #[test]
    fn model_rejects_stale_writer_after_fence_rotation() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::RotateFence {
            new_fence_token: FenceToken(9),
        })
        .expect("fence rotation should succeed");

        let error = ns
            .apply(ModelAction::BumpSeq {
                writer_fence_token: FenceToken(8),
            })
            .expect_err("stale writer should be rejected");

        assert_eq!(
            error,
            ModelError::StaleWriterFenceToken {
                expected: FenceToken(9),
                actual: FenceToken(8),
            }
        );
    }

    #[test]
    fn model_prepares_next_wal_commit_seq_for_active_writer() {
        let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let wal = ns
            .prepare_wal_commit("req-20260311-0001", FenceToken(0))
            .expect("active writer should prepare WAL");

        assert_eq!(wal.seq, ChangeSeq(1));
        assert_eq!(wal.base_head_seq, ChangeSeq(0));
        assert_eq!(wal.commit_id, "req-20260311-0001");
    }
}
