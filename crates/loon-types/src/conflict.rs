use crate::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictClass {
    SameInodeStaleBaseEdit,
    PathBindingCollision,
    DeleteVsEdit,
    RenameVsEdit,
    SubtreeDeleteVsLocalChanges,
    SubtreeRenameVsLocalChanges,
}

impl ConflictClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SameInodeStaleBaseEdit => "same_inode_stale_base_edit",
            Self::PathBindingCollision => "path_binding_collision",
            Self::DeleteVsEdit => "delete_vs_edit",
            Self::RenameVsEdit => "rename_vs_edit",
            Self::SubtreeDeleteVsLocalChanges => "subtree_delete_vs_local_changes",
            Self::SubtreeRenameVsLocalChanges => "subtree_rename_vs_local_changes",
        }
    }

    pub fn artifact_kind(self) -> ConflictArtifactKind {
        match self {
            Self::SameInodeStaleBaseEdit
            | Self::PathBindingCollision
            | Self::DeleteVsEdit
            | Self::RenameVsEdit => ConflictArtifactKind::File,
            Self::SubtreeDeleteVsLocalChanges | Self::SubtreeRenameVsLocalChanges => {
                ConflictArtifactKind::Subtree
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictArtifactKind {
    File,
    Subtree,
}

impl ConflictArtifactKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Subtree => "subtree",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictArtifactLifecycleState {
    Active,
    Archived,
}

impl ConflictArtifactLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }
}

fn inode_kind_token(inode_kind: InodeKind) -> &'static str {
    match inode_kind {
        InodeKind::File => "file",
        InodeKind::Dir => "dir",
        InodeKind::Symlink => "symlink",
        InodeKind::Mount => "mount",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    StablePaths,
}

impl ConflictPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StablePaths => "stable_paths",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictArtifactWinnerSummary {
    pub inode_id: InodeId,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub revision_no: RevisionNo,
    pub content_manifest_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictArtifactLoserSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode_id: Option<InodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_file_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_revision_no: Option<RevisionNo>,
    pub content_manifest_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictArtifactEnvelope {
    pub conflict_id: String,
    pub namespace_id: NamespaceId,
    pub conflict_class: ConflictClass,
    pub policy_applied: ConflictPolicy,
    pub detected_seq: ChangeSeq,
    pub winner: ConflictArtifactWinnerSummary,
    pub loser: ConflictArtifactLoserSummary,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeConflictArtifactRootSummary {
    pub inode_id: InodeId,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub revision_no: Option<RevisionNo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeConflictArtifactEntry {
    pub relative_path: String,
    pub inode_kind: InodeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode_id: Option<InodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_file_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_revision_no: Option<RevisionNo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_manifest_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeConflictArtifactEnvelope {
    pub conflict_id: String,
    pub namespace_id: NamespaceId,
    pub conflict_class: ConflictClass,
    pub policy_applied: ConflictPolicy,
    pub detected_seq: ChangeSeq,
    pub winner: ConflictArtifactWinnerSummary,
    pub loser_root: SubtreeConflictArtifactRootSummary,
    pub entries: Vec<SubtreeConflictArtifactEntry>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictArtifactArchiveEnvelope {
    pub namespace_id: NamespaceId,
    pub conflict_id: String,
    pub archived_at_ms: u64,
}

pub fn deterministic_conflict_id(
    namespace_id: &NamespaceId,
    conflict_class: ConflictClass,
    winner: &ConflictArtifactWinnerSummary,
    loser: &ConflictArtifactLoserSummary,
    detected_seq: ChangeSeq,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(namespace_id.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(conflict_class.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(detected_seq.0.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(winner.inode_id.0.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(
        winner
            .parent_inode_id
            .map(|inode_id| inode_id.0.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(winner.display_name.as_bytes());
    hasher.update(b"\n");
    hasher.update(winner.revision_no.0.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(
        winner
            .content_manifest_digest
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(
        loser
            .inode_id
            .map(|inode_id| inode_id.0.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(
        loser
            .client_file_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(
        loser
            .base_revision_no
            .map(|revision| revision.0.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(
        loser
            .content_digest
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(
        loser
            .parent_inode_id
            .map(|inode_id| inode_id.0.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(loser.display_name.as_bytes());
    format!("conflict-{:x}", hasher.finalize())
}

pub fn deterministic_subtree_conflict_id(
    namespace_id: &NamespaceId,
    conflict_class: ConflictClass,
    winner: &ConflictArtifactWinnerSummary,
    loser_root: &SubtreeConflictArtifactRootSummary,
    entries: &[SubtreeConflictArtifactEntry],
    detected_seq: ChangeSeq,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(namespace_id.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(conflict_class.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(detected_seq.0.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(winner.inode_id.0.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(
        winner
            .parent_inode_id
            .map(|inode_id| inode_id.0.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(winner.display_name.as_bytes());
    hasher.update(b"\n");
    hasher.update(winner.revision_no.0.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(
        winner
            .content_manifest_digest
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(loser_root.inode_id.0.to_string().as_bytes());
    hasher.update(b"\n");
    hasher.update(
        loser_root
            .parent_inode_id
            .map(|inode_id| inode_id.0.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(b"\n");
    hasher.update(loser_root.display_name.as_bytes());
    hasher.update(b"\n");
    hasher.update(
        loser_root
            .revision_no
            .map(|revision| revision.0.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    for entry in entries {
        hasher.update(b"\nentry\n");
        hasher.update(entry.relative_path.as_bytes());
        hasher.update(b"\n");
        hasher.update(inode_kind_token(entry.inode_kind.clone()).as_bytes());
        hasher.update(b"\n");
        hasher.update(
            entry
                .inode_id
                .map(|inode_id| inode_id.0.to_string())
                .unwrap_or_default()
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(
            entry
                .client_file_id
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(
            entry
                .base_revision_no
                .map(|revision| revision.0.to_string())
                .unwrap_or_default()
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(
            entry
                .content_digest
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(
            entry
                .content_manifest_digest
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(
            entry
                .parent_inode_id
                .map(|inode_id| inode_id.0.to_string())
                .unwrap_or_default()
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(entry.display_name.as_bytes());
    }
    format!("conflict-{:x}", hasher.finalize())
}
