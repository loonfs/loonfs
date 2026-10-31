//! [`CommitValidationSession`]: validates one commit's operations over the
//! loaded metadata and the rows the commit's earlier operations would
//! persist.

use super::checks::CommitNumbering;
use crate::commit::CommitOperation;
use crate::error::Result;
use crate::metadata::{MetadataState, MetadataView};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{WalCommitDelta, WalDelta};
use loonfs_types::{ActorId, ChangeSeq, CommitId, ContentLayout, ContentRef};

/// One commit's validation: the loaded [`MetadataView`], the rows the
/// batch's earlier commits and this commit's earlier operations would
/// persist, the commit's numbering, and the commit's header.
pub(crate) struct CommitValidationSession<'a, S: ObjectStore + ?Sized> {
    base_view: MetadataView<'a, 'a, S>,
    batch_accepted: &'a MetadataState,
    pub(super) committed_seq: ChangeSeq,
    overlay: MetadataState,
    pub(super) numbering: CommitNumbering,
    commit_id: &'a CommitId,
    pub(super) actor_id: &'a ActorId,
    pub(super) committed_at_ms: u64,
}

impl<'a, S: ObjectStore + ?Sized> CommitValidationSession<'a, S> {
    pub(crate) fn new(
        base_view: MetadataView<'a, 'a, S>,
        batch_accepted: &'a MetadataState,
        committed_seq: ChangeSeq,
        commit_id: &'a CommitId,
        actor_id: &'a ActorId,
        committed_at_ms: u64,
    ) -> Self {
        Self {
            base_view,
            batch_accepted,
            committed_seq,
            overlay: MetadataState::default(),
            numbering: CommitNumbering::default(),
            commit_id,
            actor_id,
            committed_at_ms,
        }
    }

    /// The metadata the next operation resolves against: the loaded publish
    /// view plus every row this commit's earlier operations would persist.
    /// The planner reads through this so an operation resolves paths against
    /// what its predecessors did.
    pub(crate) fn view(&self) -> MetadataView<'_, 'a, S> {
        self.base_view
            .with_overlay(Some(&self.overlay), self.batch_accepted, self.committed_seq)
    }

    /// Validates `ops` in order, applying each operation's deltas before the
    /// next operation is validated, and returns the deltas.
    ///
    /// `revision_layout` returns the layout that a revision of a content
    /// reference includes, when the commit planned a reference.
    pub(crate) async fn validate(
        &mut self,
        ops: &[CommitOperation],
        revision_layout: impl Fn(&ContentRef) -> Option<ContentLayout>,
    ) -> Result<Vec<WalCommitDelta>> {
        let mut deltas = Vec::new();
        for op in ops {
            let op_index = self.numbering.reserve_op_index()?;
            let mut operation_deltas = self.validate_operation(op).await?;
            for delta in &mut operation_deltas {
                if let WalDelta::AppendFileRevision {
                    content_ref,
                    layout,
                    ..
                } = delta
                {
                    let carried_layout = revision_layout(content_ref);
                    *layout = carried_layout.or_else(|| layout.take());
                }
            }
            for delta in &operation_deltas {
                self.apply(delta);
            }
            deltas.extend(operation_deltas.into_iter().map(|delta| WalCommitDelta {
                semantic_operation_index: op_index,
                delta,
            }));
        }
        Ok(deltas)
    }

    /// Adds the rows of one accepted delta to the rows this commit would
    /// persist.
    pub(super) fn apply(&mut self, delta: &WalDelta) {
        self.overlay.apply_committed_wal_delta_mut(
            self.committed_seq,
            self.commit_id,
            self.actor_id,
            self.committed_at_ms,
            delta,
        );
    }
}
