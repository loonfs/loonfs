//! Independent implementation of the visibility semantics — on purpose.
//!
//! This module re-states the metadata visibility rules (binding identity,
//! active bindings, tombstone coverage, visible-path resolution) from
//! scratch, deliberately NOT sharing code with `loonfs-core`'s
//! `metadata::visibility` module. The differential suite replays the same
//! logical commits through both implementations and requires identical
//! outcomes; that comparison only catches bugs while the two sides remain
//! independent. Do not "deduplicate" this into core — collapsing them would
//! turn the differential tests into a tautology.

use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{ChangeSeq, ContentRef, InodeId, InodeKind, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetadataState {
    #[serde(default)]
    pub inodes: Vec<InodeRecord>,
    #[serde(default)]
    pub direntry_binds: Vec<DirentryBindRecord>,
    #[serde(default)]
    pub direntry_unbinds: Vec<DirentryUnbindRecord>,
    #[serde(default)]
    pub revisions: Vec<RevisionRecord>,
    #[serde(default)]
    pub subtree_tombstones: Vec<SubtreeTombstoneRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeRecord {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub created_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryBindRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryUnbindRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
    pub unbind_seq: ChangeSeq,
    pub unbind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionRecord {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    pub revision_delta_index: u32,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
    pub tombstone_delta_index: u32,
    /// What this event did, mirroring the core row semantics: the newest
    /// event per root wins, and a revoke as the newest means no active
    /// tombstone.
    pub action: SubtreeTombstoneAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubtreeTombstoneAction {
    Set,
    Revoke {
        target_seq: ChangeSeq,
        target_delta_index: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMetadataState {
    pub metadata_state: MetadataState,
    pub checked_invariants: Vec<String>,
}

impl MetadataState {
    pub fn apply_committed_wal_deltas(
        &self,
        committed_seq: ChangeSeq,
        deltas: &[WalDelta],
    ) -> AppliedMetadataState {
        let mut metadata_state = self.clone();
        let mut checked_invariants = Vec::new();

        for delta in deltas {
            match delta {
                WalDelta::CreateInode {
                    delta_index: _,
                    inode_id,
                    inode_kind,
                } => {
                    metadata_state.inodes.push(InodeRecord {
                        inode_id: *inode_id,
                        inode_kind: *inode_kind,
                        created_seq: committed_seq,
                    });
                    push_unique_invariant(&mut checked_invariants, "create_inode_writes_inode_row");
                }
                WalDelta::BindDirentry {
                    delta_index,
                    parent_inode_id,
                    name_key,
                    display_name,
                    child_inode_id,
                } => {
                    metadata_state.direntry_binds.push(DirentryBindRecord {
                        parent_inode_id: *parent_inode_id,
                        name_key: name_key.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *child_inode_id,
                        bind_seq: committed_seq,
                        bind_delta_index: *delta_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "bind_direntry_writes_direntry_bind_row",
                    );
                }
                WalDelta::UnbindDirentry {
                    delta_index,
                    parent_inode_id,
                    name_key,
                    child_inode_id,
                    bind_seq,
                    bind_delta_index,
                } => {
                    metadata_state.direntry_unbinds.push(DirentryUnbindRecord {
                        parent_inode_id: *parent_inode_id,
                        name_key: name_key.clone(),
                        child_inode_id: *child_inode_id,
                        bind_seq: *bind_seq,
                        bind_delta_index: *bind_delta_index,
                        unbind_seq: committed_seq,
                        unbind_delta_index: *delta_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "unbind_direntry_writes_unbind_row",
                    );
                }
                WalDelta::AppendFileRevision {
                    delta_index,
                    inode_id,
                    revision_no,
                    content_ref,
                } => {
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: *revision_no,
                        committed_seq,
                        revision_delta_index: *delta_index,
                        content_ref: content_ref.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "append_file_revision_writes_revision_row",
                    );
                }
                WalDelta::TombstoneSubtree {
                    delta_index,
                    root_inode_id,
                } => {
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode_id,
                            tombstone_seq: committed_seq,
                            tombstone_delta_index: *delta_index,
                            action: SubtreeTombstoneAction::Set,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "tombstone_subtree_writes_tombstone_row",
                    );
                }
                WalDelta::RevokeSubtreeTombstone {
                    delta_index,
                    root_inode_id,
                    target_seq,
                    target_delta_index,
                } => {
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode_id,
                            tombstone_seq: committed_seq,
                            tombstone_delta_index: *delta_index,
                            action: SubtreeTombstoneAction::Revoke {
                                target_seq: *target_seq,
                                target_delta_index: *target_delta_index,
                            },
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "revoke_subtree_tombstone_writes_revoke_row",
                    );
                }
            }
        }

        AppliedMetadataState {
            metadata_state,
            checked_invariants,
        }
    }
}

fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_direntry_replay_uses_persisted_name_key() {
        let applied = MetadataState::default().apply_committed_wal_deltas(
            ChangeSeq(1),
            &[WalDelta::BindDirentry {
                delta_index: 7,
                parent_inode_id: InodeId(1),
                name_key: "persisted-key".to_owned(),
                display_name: "Report.TXT".to_owned(),
                child_inode_id: InodeId(2),
            }],
        );

        assert_eq!(applied.metadata_state.direntry_binds.len(), 1);
        assert_eq!(
            applied.metadata_state.direntry_binds[0].name_key,
            "persisted-key"
        );
    }
}
