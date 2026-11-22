//! [`CommitSession`]: the state of one commit that its writes check and
//! change, over the loaded metadata and the rows that the commit's earlier
//! writes would persist.

use super::checks::CommitNumbering;
use crate::commit::engine::CommitCandidate;
use crate::commit::AppendedContent;
use crate::metadata::{MetadataState, MetadataView};
use crate::storage::content_admission::PreparedContent;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{WalCommitDelta, WalDelta};
use loonfs_types::{ActorId, CommitId, CommitSeq, ContentLayout, ContentRef, NamespaceId};
use std::collections::HashMap;

/// One commit's state: the loaded [`MetadataView`], the rows the batch's
/// earlier commits and this commit's earlier writes would persist, the
/// commit's numbering and header, the deltas that its writes built, and the
/// content that it appends.
pub(crate) struct CommitSession<'a, S: ObjectStore + ?Sized> {
    base_view: MetadataView<'a, 'a, S>,
    batch_accepted: &'a MetadataState,
    pub(super) committed_seq: CommitSeq,
    overlay: MetadataState,
    pub(super) numbering: CommitNumbering,
    commit_id: &'a CommitId,
    pub(super) actor_id: &'a ActorId,
    pub(super) committed_at_ms: u64,
    namespace_id: &'a NamespaceId,
    proofs: HashMap<&'a ContentRef, &'a PreparedContent>,
    appended: Vec<AppendedContent>,
    deltas: Vec<WalCommitDelta>,
}

impl<'a, S: ObjectStore + ?Sized> CommitSession<'a, S> {
    pub(crate) fn new(
        base_view: MetadataView<'a, 'a, S>,
        batch_accepted: &'a MetadataState,
        namespace_id: &'a NamespaceId,
        candidate: &'a CommitCandidate,
        committed_seq: CommitSeq,
        committed_at_ms: u64,
    ) -> Self {
        let request = candidate.intent();
        Self {
            base_view,
            batch_accepted,
            committed_seq,
            overlay: MetadataState::default(),
            numbering: CommitNumbering::default(),
            commit_id: &request.commit_id,
            actor_id: &request.actor_id,
            committed_at_ms,
            namespace_id,
            proofs: candidate.proofs_by_reference(),
            appended: Vec::new(),
            deltas: Vec::new(),
        }
    }

    /// The metadata the next read resolves against: the loaded publish view
    /// plus every row that this commit's earlier writes would persist. The
    /// planner reads through this, so an operation resolves paths against
    /// what the writes before it built.
    pub(crate) fn view(&self) -> MetadataView<'_, 'a, S> {
        self.base_view
            .with_overlay(Some(&self.overlay), self.batch_accepted, self.committed_seq)
    }

    /// The deltas that the commit's writes have built so far, in order.
    pub(crate) fn deltas(&self) -> &[WalCommitDelta] {
        &self.deltas
    }

    /// Records the bytes that an append adds, so that the revision that
    /// refers to `value.content_ref` includes `value.layout`.
    pub(crate) fn record_appended(&mut self, value: AppendedContent) {
        self.appended.push(value);
    }

    /// Moves the written deltas out of the session, for the commit's plan.
    pub(crate) fn take_deltas(&mut self) -> Vec<WalCommitDelta> {
        std::mem::take(&mut self.deltas)
    }

    /// Moves the recorded appends out of the session, for the commit's plan.
    pub(crate) fn take_appended(&mut self) -> Vec<AppendedContent> {
        std::mem::take(&mut self.appended)
    }

    /// Records the deltas of the operation that `op_index` identifies. It
    /// sets the layout of each revision that the deltas write, applies the
    /// deltas to the overlay so that later reads see them, and records them
    /// with the index. Each write reserves its index before it checks
    /// anything, so an index overflow is reported before the write's own
    /// errors.
    pub(super) fn record_operation(&mut self, op_index: u32, mut deltas: Vec<WalDelta>) {
        for delta in &mut deltas {
            if let WalDelta::AppendFileRevision {
                content_ref,
                layout,
                ..
            } = delta
            {
                *layout = self.revision_layout(content_ref);
            }
        }
        for delta in &deltas {
            self.overlay.apply_committed_wal_delta_mut(
                self.committed_seq,
                self.commit_id,
                self.actor_id,
                self.committed_at_ms,
                delta,
            );
        }
        self.deltas
            .extend(deltas.into_iter().map(|delta| WalCommitDelta {
                semantic_operation_index: op_index,
                delta,
            }));
    }

    /// Returns the layout that a revision of `content_ref` includes: the
    /// layout of the content that this commit appended under that reference,
    /// or else the layout that the reference's preparation proof records.
    fn revision_layout(&self, content_ref: &ContentRef) -> Option<ContentLayout> {
        if let Some(value) = self
            .appended
            .iter()
            .find(|value| value.content_ref == *content_ref)
        {
            return value.layout.clone();
        }
        self.proofs
            .get(content_ref)
            .and_then(|proof| proof.revision_layout(self.namespace_id))
    }
}
