//! Independent reference implementation of metadata visibility.
//!
//! This module intentionally does not share code with
//! `loonfs-core::metadata::visibility`. Differential tests replay the same
//! commits through both implementations and compare their results. Merging
//! the implementations would remove the independence those tests require.
//!
//! Inodes, file revisions, tombstones, and stored attribute revisions copy
//! their commit identity, actor, and timestamp from the WAL commit. Directory
//! bindings do not store attribution. The initial root inode uses
//! `ActorRef::loonfs_system()`, and the initial empty attribute state has no
//! actor or timestamp because it is not stored as a revision.

use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{ActorRef, ChangeSeq, CommitId, ContentRef, InodeId, InodeKind, RevisionNo};
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
    #[serde(default)]
    pub attribute_revisions: Vec<ModelAttributeRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeRecord {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub created_seq: ChangeSeq,
    pub commit_id: CommitId,
    pub created_by: ActorRef,
    pub created_at_ms: u64,
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
    /// User-facing spelling the retired binding carried. The unbind row is
    /// the durable home of a deleted name while it is retained.
    pub display_name: String,
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
    pub commit_id: CommitId,
    pub committed_at_ms: u64,
    pub actor: ActorRef,
    pub revision_delta_index: u32,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
    pub tombstone_delta_index: u32,
    pub commit_id: CommitId,
    pub deleted_at_ms: u64,
    pub actor: ActorRef,
    /// Action recorded by this event. The newest event for each root determines
    /// state; a newest `Revoke` means no tombstone is active.
    pub action: SubtreeTombstoneAction,
}

/// Complete attribute map for one inode revision, represented with this
/// model's independent key, value, and field types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAttributeRevision {
    pub inode_id: InodeId,
    pub revision: u64,
    pub committed_seq: ChangeSeq,
    pub commit_id: CommitId,
    pub delta_index: u32,
    pub actor: ActorRef,
    pub updated_at_ms: u64,
    /// The map after the update, in key order. An empty list is the cleared
    /// state, not a missing record.
    pub entries: Vec<AttributeEntry>,
}

/// One key and its value inside an attribute map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeEntry {
    pub key: String,
    pub value: String,
}

/// Commit position of a deletion event. A revoke uses this value to identify
/// the exact deletion it cancels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionGeneration {
    pub seq: ChangeSeq,
    pub delta_index: u32,
}

/// Directory binding removed by a path deletion, represented with this
/// model's independent types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedBinding {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubtreeTombstoneAction {
    /// Deletes the subtree rooted at the record's inode. Only a delete has
    /// a binding to remove, so this is the one action that can carry one —
    /// absent when the delete was addressed by inode.
    Set {
        deleted_binding: Option<DeletedBinding>,
    },
    /// Cancels exactly the deletion recorded at `target`.
    Revoke { target: DeletionGeneration },
}

impl MetadataState {
    pub fn apply_committed_wal_deltas(
        &self,
        committed_seq: ChangeSeq,
        commit_id: &CommitId,
        actor: &ActorRef,
        committed_at_ms: u64,
        deltas: &[WalDelta],
    ) -> MetadataState {
        let mut metadata_state = self.clone();

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
                        commit_id: commit_id.clone(),
                        created_by: actor.clone(),
                        created_at_ms: committed_at_ms,
                    });
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
                        name_key: name_key.as_str().to_owned(),
                        display_name: display_name.as_str().to_owned(),
                        child_inode_id: *child_inode_id,
                        bind_seq: committed_seq,
                        bind_delta_index: *delta_index,
                    });
                }
                WalDelta::UnbindDirentry {
                    delta_index,
                    parent_inode_id,
                    name_key,
                    display_name,
                    child_inode_id,
                    bind_seq,
                    bind_delta_index,
                } => {
                    metadata_state.direntry_unbinds.push(DirentryUnbindRecord {
                        parent_inode_id: *parent_inode_id,
                        name_key: name_key.as_str().to_owned(),
                        display_name: display_name.as_str().to_owned(),
                        child_inode_id: *child_inode_id,
                        bind_seq: *bind_seq,
                        bind_delta_index: *bind_delta_index,
                        unbind_seq: committed_seq,
                        unbind_delta_index: *delta_index,
                    });
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
                        commit_id: commit_id.clone(),
                        committed_at_ms,
                        actor: actor.clone(),
                        revision_delta_index: *delta_index,
                        content_ref: content_ref.clone(),
                    });
                }
                WalDelta::TombstoneSubtree {
                    delta_index,
                    root_inode_id,
                    deleted_direntry,
                } => {
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode_id,
                            tombstone_seq: committed_seq,
                            tombstone_delta_index: *delta_index,
                            commit_id: commit_id.clone(),
                            deleted_at_ms: committed_at_ms,
                            actor: actor.clone(),
                            action: SubtreeTombstoneAction::Set {
                                deleted_binding: deleted_direntry.as_ref().map(|direntry| {
                                    DeletedBinding {
                                        parent_inode_id: direntry.parent_inode_id,
                                        name_key: direntry.name_key.as_str().to_owned(),
                                        display_name: direntry.display_name.as_str().to_owned(),
                                    }
                                }),
                            },
                        });
                }
                WalDelta::RevokeSubtreeTombstone {
                    delta_index,
                    root_inode_id,
                    target,
                } => {
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode_id,
                            tombstone_seq: committed_seq,
                            tombstone_delta_index: *delta_index,
                            commit_id: commit_id.clone(),
                            deleted_at_ms: committed_at_ms,
                            actor: actor.clone(),
                            action: SubtreeTombstoneAction::Revoke {
                                target: DeletionGeneration {
                                    seq: target.seq,
                                    delta_index: target.delta_index,
                                },
                            },
                        });
                }
                WalDelta::AppendAttributesRevision {
                    delta_index,
                    inode_id,
                    attributes_revision_no,
                    attributes,
                } => {
                    metadata_state
                        .attribute_revisions
                        .push(ModelAttributeRevision {
                            inode_id: *inode_id,
                            revision: attributes_revision_no.0,
                            committed_seq,
                            commit_id: commit_id.clone(),
                            delta_index: *delta_index,
                            actor: actor.clone(),
                            updated_at_ms: committed_at_ms,
                            entries: attributes
                                .iter()
                                .map(|(key, value)| AttributeEntry {
                                    key: key.as_str().to_owned(),
                                    value: value.as_str().to_owned(),
                                })
                                .collect(),
                        });
                }
            }
        }

        metadata_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::manifest::{DeletedDirentry, TombstoneGeneration};
    use loonfs_api::NameKey;

    fn commit_id() -> CommitId {
        CommitId::parse("c_model_metadata").expect("commit id")
    }

    #[test]
    fn bind_direntry_replay_uses_persisted_name_key() {
        let applied = MetadataState::default().apply_committed_wal_deltas(
            ChangeSeq(1),
            &commit_id(),
            &ActorRef::loonfs_system(),
            4_200,
            &[WalDelta::BindDirentry {
                delta_index: 7,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("persisted-key").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Report.TXT")
                    .expect("valid display name"),
                child_inode_id: InodeId(2),
            }],
        );

        assert_eq!(applied.direntry_binds.len(), 1);
        assert_eq!(applied.direntry_binds[0].name_key, "persisted-key");
    }

    #[test]
    fn tombstone_replay_restates_the_deleted_binding_and_the_revoked_generation() {
        let applied = MetadataState::default().apply_committed_wal_deltas(
            ChangeSeq(9),
            &commit_id(),
            &ActorRef::loonfs_system(),
            4_200,
            &[
                WalDelta::TombstoneSubtree {
                    delta_index: 1,
                    root_inode_id: InodeId(2),
                    deleted_direntry: Some(DeletedDirentry {
                        parent_inode_id: InodeId(1),
                        name_key: NameKey::parse("report.txt").expect("valid name key"),
                        display_name: loonfs_api::DisplayName::parse("Report.TXT")
                            .expect("valid display name"),
                    }),
                },
                WalDelta::RevokeSubtreeTombstone {
                    delta_index: 2,
                    root_inode_id: InodeId(2),
                    target: TombstoneGeneration {
                        seq: ChangeSeq(4),
                        delta_index: 3,
                    },
                },
            ],
        );

        assert_eq!(
            applied
                .subtree_tombstones
                .iter()
                .map(|tombstone| tombstone.action.clone())
                .collect::<Vec<_>>(),
            vec![
                SubtreeTombstoneAction::Set {
                    deleted_binding: Some(DeletedBinding {
                        parent_inode_id: InodeId(1),
                        name_key: "report.txt".to_owned(),
                        display_name: "Report.TXT".to_owned(),
                    }),
                },
                SubtreeTombstoneAction::Revoke {
                    target: DeletionGeneration {
                        seq: ChangeSeq(4),
                        delta_index: 3,
                    },
                },
            ]
        );
    }
}
