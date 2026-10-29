//! Inline placement and fallback staging under a permit.

use super::admission::AdmissionPermit;
use super::queue::PreparedCandidate;
use super::NamespacePublisher;
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
        let mut remaining = self
            .context
            .runtime_core
            .config()
            .inline_content
            .inline_content_wal_object_budget_bytes
            .saturating_sub(candidate.appended_bytes());
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
            let slot = self.lock_engine().await;
            // Without a position the tail is unknown, so admit at most one WAL
            // object budget until the next publish reads the tail.
            let unfolded_bytes = slot.wal_tail_inline_bytes().unwrap_or(
                self.context
                    .runtime_core
                    .config()
                    .inline_content
                    .inline_content_tail_limit_bytes
                    .saturating_sub(
                        self.context
                            .runtime_core
                            .config()
                            .inline_content
                            .inline_content_wal_object_budget_bytes,
                    ),
            );
            permit.reserve_inline(
                plan.ordered_inline_content[..plan.wal_object_inline_values]
                    .iter()
                    .map(|value| value.bytes().len()),
                unfolded_bytes,
                self.context
                    .runtime_core
                    .config()
                    .inline_content
                    .inline_content_tail_limit_bytes,
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
            let slot = self.lock_engine().await;
            if slot
                .engine
                .as_ref()
                .is_some_and(|engine| engine.retains_commit_receipt(candidate.commit_id()))
            {
                return Ok(());
            }
        }
        let crate::pinned_read::PinnedRead {
            engine: reader,
            context,
        } = self
            .context
            .runtime_core
            .pinned_read(namespace_id, crate::runtime_core::HeadConfirmation::View)
            .await?;
        if reader
            .find_commit_receipt(&context, candidate.commit_id())
            .await?
            .is_some()
        {
            return Ok(());
        }
        let writer = self
            .context
            .writer
            .upgrade()
            .ok_or(CoreError::ShuttingDown)?;
        let catalog = self
            .context
            .runtime_core
            .load_namespace_catalog_cached(namespace_id)
            .await?;
        let engine = self
            .context
            .runtime_core
            .writer_engine(&writer.writer_id, namespace_id);
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
