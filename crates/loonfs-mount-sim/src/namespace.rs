//! One simulated namespace: its model state, its committed history, its receipts, and its
//! snapshots.

use crate::error::{MountSimError, Result};
use crate::model_view::ModelView;
use crate::query::ReadPoint;
use loonfs_model::bootstrap_metadata_state;
use loonfs_model::metadata::MetadataState;
use loonfs_types::api::v0::{Commit, NamespaceAccessMode, NamespaceMetadata, Snapshot};
use loonfs_types::{
    ActorId, ChangeSeq, CommitFingerprint, CommitId, ErrorCode, NamespaceId, NamespaceNaming,
    PinId, FIRST_ALLOCATABLE_INODE_ID,
};
use std::collections::BTreeMap;

/// One commit in a namespace's history, as the change feed and a replay return it.
#[derive(Debug)]
pub(crate) struct CommitRecord {
    pub(crate) commit: Commit,
    pub(crate) fingerprint: CommitFingerprint,
    /// The size of the commit's JSON encoding, which the change feed's page budget counts.
    pub(crate) encoded_bytes: usize,
}

impl CommitRecord {
    pub(crate) fn new(commit: Commit, fingerprint: CommitFingerprint) -> Self {
        let encoded_bytes = serde_json::to_vec(&commit)
            .expect("a commit should encode as JSON")
            .len();
        Self {
            commit,
            fingerprint,
            encoded_bytes,
        }
    }
}

/// The state of one namespace.
#[derive(Debug)]
pub(crate) struct NamespaceState {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) naming: NamespaceNaming,
    pub(crate) created_at_ms: u64,
    /// The model's rows for every commit, which answer reads at any sequence.
    pub(crate) metadata: MetadataState,
    /// Every commit, where `commits[i]` is the commit at sequence `i + 1`.
    pub(crate) commits: Vec<CommitRecord>,
    /// The sequence of each commit whose receipt the retention floor hasn't removed.
    pub(crate) receipts: BTreeMap<CommitId, ChangeSeq>,
    pub(crate) retention_floor_seq: ChangeSeq,
    pub(crate) snapshots: BTreeMap<PinId, Snapshot>,
    pub(crate) next_inode_id: u64,
}

impl NamespaceState {
    pub(crate) fn new(namespace_id: NamespaceId, naming: NamespaceNaming, now_ms: u64) -> Self {
        Self {
            namespace_id,
            naming,
            created_at_ms: now_ms,
            metadata: bootstrap_metadata_state(now_ms),
            commits: Vec::new(),
            receipts: BTreeMap::new(),
            retention_floor_seq: ChangeSeq(0),
            snapshots: BTreeMap::new(),
            next_inode_id: FIRST_ALLOCATABLE_INODE_ID.0,
        }
    }

    pub(crate) fn head_seq(&self) -> ChangeSeq {
        ChangeSeq(u64::try_from(self.commits.len()).expect("a commit count fits in u64"))
    }

    pub(crate) fn commit_at(&self, seq: ChangeSeq) -> &CommitRecord {
        let index = usize::try_from(seq.0 - 1).expect("a sequence fits in usize");
        &self.commits[index]
    }

    /// Returns the commits after `after_seq`, up to and including `last_seq`.
    pub(crate) fn commits_between(
        &self,
        after_seq: ChangeSeq,
        last_seq: ChangeSeq,
    ) -> &[CommitRecord] {
        let start = usize::try_from(after_seq.0).expect("a sequence fits in usize");
        let end = usize::try_from(last_seq.0).expect("a sequence fits in usize");
        &self.commits[start..end]
    }

    /// Appends a commit and the model state after it.
    pub(crate) fn publish(
        &mut self,
        record: CommitRecord,
        metadata: MetadataState,
        next_inode_id: u64,
    ) {
        assert_eq!(
            record.commit.committed_seq,
            ChangeSeq(self.head_seq().0 + 1),
            "commits should be published in sequence order"
        );
        self.receipts
            .insert(record.commit.commit_id.clone(), record.commit.committed_seq);
        self.commits.push(record);
        self.metadata = metadata;
        self.next_inode_id = next_inode_id;
    }

    pub(crate) fn metadata_response(&self) -> NamespaceMetadata {
        NamespaceMetadata {
            access: NamespaceAccessMode::Unrestricted {},
            naming: self.naming,
            namespace_id: self.namespace_id.clone(),
            created_at_ms: self.created_at_ms,
            created_by: ActorId::loonfs(),
            fork_basis: None,
            head_seq: self.head_seq(),
            retention_floor_seq: self.retention_floor_seq,
            retention_window_ms: None,
        }
    }

    /// Returns a view of the metadata at `seq`.
    pub(crate) fn view(&self, seq: ChangeSeq) -> ModelView<'_> {
        ModelView {
            state: &self.metadata,
            seq,
            namespace_id: &self.namespace_id,
            naming: self.naming,
        }
    }

    /// Returns the sequence that a read at `at` reads: the head, or the sequence that an unexpired
    /// snapshot captured.
    pub(crate) fn read_seq(&self, at: &ReadPoint, now_ms: u64) -> Result<ChangeSeq> {
        match at {
            ReadPoint::Live => Ok(self.head_seq()),
            ReadPoint::Snapshot(snapshot_id) => self
                .live_snapshot(snapshot_id, now_ms)
                .map(|snapshot| snapshot.captured_seq),
        }
    }

    /// Returns a snapshot that exists and hasn't expired at `now_ms`.
    pub(crate) fn live_snapshot(&self, snapshot_id: &PinId, now_ms: u64) -> Result<&Snapshot> {
        self.snapshots
            .get(snapshot_id)
            .filter(|snapshot| snapshot.is_live(now_ms))
            .ok_or_else(|| {
                MountSimError::new(
                    ErrorCode::SnapshotNotFound,
                    format!("snapshot `{snapshot_id}` doesn't exist or has expired"),
                )
            })
    }
}
