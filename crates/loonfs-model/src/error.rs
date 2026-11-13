//! Rule violations the model reports instead of a result.

use crate::metadata::DeltaPosition;
use loonfs_types::{CommitSeq, InodeId, NameKey};

/// A history or state that breaks a rule the model checks.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelError {
    #[error("inode `{child_inode_id}` has two parent bindings at seq {seq}")]
    ChildHasTwoParents {
        seq: CommitSeq,
        child_inode_id: InodeId,
        bindings: [DeltaPosition; 2],
    },
    #[error("slot `{parent_inode_id}` `{name_key}` has two children at seq {seq}")]
    SlotHasTwoChildren {
        seq: CommitSeq,
        parent_inode_id: InodeId,
        name_key: NameKey,
        bindings: [DeltaPosition; 2],
    },
    /// A bound value is current in slot order but not in child order, or the
    /// reverse.
    #[error("the binding indexes disagree at seq {seq}")]
    BindingIndexesDisagree {
        seq: CommitSeq,
        binding: DeltaPosition,
    },
    /// The walk up through parent bindings reached `inode_id` twice.
    #[error("inode `{inode_id}` is its own ancestor at seq {seq}")]
    AncestorCycle { seq: CommitSeq, inode_id: InodeId },
}

pub type Result<T> = std::result::Result<T, ModelError>;
