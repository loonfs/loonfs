#![forbid(unsafe_code)]

use loon_types::{ChangeSeq, InodeId, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelNamespace {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelAction {
    CreateDir { inode_id: InodeId },
    DeleteSubtree { root_inode: InodeId },
    BumpSeq,
}

impl ModelNamespace {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            head_seq: ChangeSeq(0),
        }
    }

    pub fn apply(&mut self, action: ModelAction) {
        match action {
            ModelAction::CreateDir { .. }
            | ModelAction::DeleteSubtree { .. }
            | ModelAction::BumpSeq => {
                self.head_seq = ChangeSeq(self.head_seq.0 + 1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_advances_seq() {
        let mut ns = ModelNamespace::new(NamespaceId(1));
        ns.apply(ModelAction::BumpSeq);
        assert_eq!(ns.head_seq.0, 1);
    }
}
