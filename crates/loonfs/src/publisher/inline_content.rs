//! Inline placement and permit-held fallback staging.

use super::{AdmissionPermit, NamespacePublisher, PreparedCandidate, PublisherRegistry};
use crate::publish::CommitCandidate;
use crate::{CoreError, NamespaceId, Result};
use loonfs_api::wire::wal::MAX_WAL_INLINE_CONTENT_BYTES;
use loonfs_core::publish::InlineContent;

pub(super) struct InlineCandidatePlan {
    pub(super) candidate: PreparedCandidate,
    ordered_inline_content: Vec<InlineContent>,
    segment_inline_values: usize,
}

impl PublisherRegistry {
    pub(super) fn plan_inline_candidate(
        &self,
        namespace_id: &NamespaceId,
        candidate: CommitCandidate,
        publisher: &NamespacePublisher,
    ) -> Result<InlineCandidatePlan> {
        let values = candidate.ordered_inline_content(namespace_id)?;
        let mut remaining = publisher.inline_content.inline_content_segment_budget_bytes;
        let segment_inline_values = values
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
            &values[..segment_inline_values],
            &values[segment_inline_values..],
        )?;
        Ok(InlineCandidatePlan {
            candidate,
            ordered_inline_content: values,
            segment_inline_values,
        })
    }

    pub(super) async fn stage_inline_candidate(
        &self,
        namespace_id: &NamespaceId,
        mut plan: InlineCandidatePlan,
        publisher: &NamespacePublisher,
        permit: &AdmissionPermit,
    ) -> Result<PreparedCandidate> {
        if plan.ordered_inline_content.is_empty() {
            return Ok(plan.candidate);
        }
        let kept = {
            let slot = publisher.engine.lock().await;
            let unfolded_bytes = slot
                .engine
                .as_ref()
                .and_then(|engine| engine.wal_fold_input())
                .map(|input| input.wal_tail_inline_bytes)
                .or(slot.last_known_wal_tail_inline_bytes)
                .unwrap_or(0);
            permit.reserve_inline(
                plan.ordered_inline_content[..plan.segment_inline_values]
                    .iter()
                    .map(|value| value.bytes().len()),
                unfolded_bytes,
                publisher.inline_content.inline_content_tail_limit_bytes,
            )
        };
        self.stage_inline_values(
            namespace_id,
            &mut plan.candidate.candidate,
            &plan.ordered_inline_content[kept..],
            publisher,
        )
        .await?;
        PreparedCandidate::new(plan.candidate.candidate).map_err(Into::into)
    }

    async fn stage_inline_values(
        &self,
        namespace_id: &NamespaceId,
        candidate: &mut CommitCandidate,
        values: &[InlineContent],
        publisher: &NamespacePublisher,
    ) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        {
            let slot = publisher.engine.lock().await;
            if slot
                .engine
                .as_ref()
                .is_some_and(|engine| engine.retains_commit_receipt(candidate.commit_id()))
            {
                return Ok(());
            }
        }
        let (reader, context) = self.read_core.pinned_read(namespace_id).await?;
        if reader
            .find_commit_receipt(&context, candidate.commit_id())
            .await?
            .is_some()
        {
            return Ok(());
        }
        let writer = self.writer.upgrade().ok_or(CoreError::ShuttingDown)?;
        let catalog = self
            .read_core
            .load_namespace_catalog_cached(namespace_id)
            .await?;
        let engine = self.read_core.writer_engine(&writer.identity, namespace_id);
        for value in values {
            let proof = engine.stage_owned_bytes(&catalog, value.bytes()).await?;
            candidate.stage_inline_content(&value.content_ref().content_id, proof);
        }
        Ok(())
    }
}
