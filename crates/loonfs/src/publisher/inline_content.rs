//! Inline placement and permit-held fallback staging.

use super::{AdmissionPermit, NamespacePublisher, PreparedCandidate};
use crate::publish::CommitCandidate;
use crate::{CoreError, Result};
use loonfs_core::publish::InlineContent;
use loonfs_types::format::wal::MAX_WAL_INLINE_CONTENT_BYTES;

pub(super) struct InlineCandidatePlan {
    pub(super) candidate: PreparedCandidate,
    ordered_inline_content: Vec<InlineContent>,
    wal_object_inline_values: usize,
}

impl NamespacePublisher {
    pub(super) fn plan_inline_candidate(
        &self,
        candidate: CommitCandidate,
    ) -> Result<InlineCandidatePlan> {
        let namespace_id = &self.namespace_id;
        let values = candidate.ordered_inline_content(namespace_id)?;
        let mut remaining = self.inline_content.inline_content_wal_object_budget_bytes;
        let wal_object_inline_values = values
            .iter()
            .take_while(|value| {
                let size = value.bytes().len();
                if size > remaining || size > MAX_WAL_INLINE_CONTENT_BYTES {
                    return false;
                }
                remaining -= size;
                true
            })
            .count();
        let candidate = PreparedCandidate::with_inline_placement(
            candidate,
            namespace_id,
            &values[..wal_object_inline_values],
            &values[wal_object_inline_values..],
        )?;
        Ok(InlineCandidatePlan {
            candidate,
            ordered_inline_content: values,
            wal_object_inline_values,
        })
    }

    pub(super) async fn stage_inline_candidate(
        &self,
        mut plan: InlineCandidatePlan,
        permit: &AdmissionPermit,
    ) -> Result<PreparedCandidate> {
        if plan.ordered_inline_content.is_empty() {
            return Ok(plan.candidate);
        }
        let kept = {
            let slot = self.engine.lock().await;
            // Until a publish observes the tail, admit at most one WAL object budget.
            let unfolded_bytes = slot.wal_tail_inline_bytes().unwrap_or(
                self.inline_content
                    .inline_content_tail_limit_bytes
                    .saturating_sub(self.inline_content.inline_content_wal_object_budget_bytes),
            );
            permit.reserve_inline(
                plan.ordered_inline_content[..plan.wal_object_inline_values]
                    .iter()
                    .map(|value| value.bytes().len()),
                unfolded_bytes,
                self.inline_content.inline_content_tail_limit_bytes,
            )
        };
        self.stage_inline_values(
            &mut plan.candidate.candidate,
            &plan.ordered_inline_content[kept..],
        )
        .await?;
        PreparedCandidate::new(plan.candidate.candidate).map_err(Into::into)
    }

    async fn stage_inline_values(
        &self,
        candidate: &mut CommitCandidate,
        values: &[InlineContent],
    ) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let namespace_id = &self.namespace_id;
        {
            let slot = self.engine.lock().await;
            if slot
                .engine
                .as_ref()
                .is_some_and(|engine| engine.retains_commit_receipt(candidate.commit_id()))
            {
                return Ok(());
            }
        }
        let (reader, context) = self.runtime_core.pinned_read(namespace_id).await?;
        if reader
            .find_commit_receipt(&context, candidate.commit_id())
            .await?
            .is_some()
        {
            return Ok(());
        }
        let writer = self.writer.upgrade().ok_or(CoreError::ShuttingDown)?;
        let catalog = self
            .runtime_core
            .load_namespace_catalog_cached(namespace_id)
            .await?;
        let engine = self
            .runtime_core
            .writer_engine(&writer.identity, namespace_id);
        for value in values {
            let proof = engine
                .stage_owned_bytes(
                    &catalog,
                    candidate.subject().map(|subject| &subject.subject_id),
                    value.bytes(),
                )
                .await?;
            candidate.stage_inline_content(&value.content_ref().content_id, proof);
        }
        Ok(())
    }
}
