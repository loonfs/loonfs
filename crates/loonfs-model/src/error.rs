//! Rule violations the model reports instead of an answer.

use crate::metadata::DeltaPosition;
use loonfs_types::{ChangeSeq, InodeId, NameKey};

/// A history or state that breaks a rule the model checks.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("inode `{child_inode_id}` has two parent bindings at seq {seq}")]
    ChildHasTwoParents {
        seq: ChangeSeq,
        child_inode_id: InodeId,
        bindings: [DeltaPosition; 2],
    },
    #[error("slot `{parent_inode_id}` `{name_key}` has two children at seq {seq}")]
    SlotHasTwoChildren {
        seq: ChangeSeq,
        parent_inode_id: InodeId,
        name_key: NameKey,
        bindings: [DeltaPosition; 2],
    },
    /// A bound value is current in slot order but not in child order, or the
    /// reverse.
    #[error("the binding indexes disagree at seq {seq}")]
    BindingIndexesDisagree {
        seq: ChangeSeq,
        binding: DeltaPosition,
    },
    /// The walk up through parent bindings reached `inode_id` twice.
    #[error("inode `{inode_id}` is its own ancestor at seq {seq}")]
    AncestorCycle { seq: ChangeSeq, inode_id: InodeId },
}

pub type Result<T> = std::result::Result<T, Error>;
