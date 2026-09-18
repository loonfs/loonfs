//! Inline policy fallback before a candidate enters the publication queue.

use super::{AdmissionPermit, NamespacePublisher, PreparedCandidate, PublisherRegistry};
use crate::publish::CommitCandidate;
use crate::{CoreError, NamespaceId, Result};
use loonfs_api::wire::wal::MAX_WAL_INLINE_CONTENT_BYTES;
use loonfs_core::publish::InlineContent;

impl PublisherRegistry {
    pub(super) async fn prepare_segment_candidate(
        &self,
        namespace_id: &NamespaceId,
        mut candidate: CommitCandidate,
        publisher: &NamespacePublisher,
    ) -> Result<PreparedCandidate> {
        if !candidate.inline_content().is_empty() {
            let values = candidate.ordered_inline_content(namespace_id)?;
            let mut remaining = publisher.inline_content.inline_content_segment_budget_bytes;
            let kept = values
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
            self.stage_inline_values(namespace_id, &mut candidate, &values[kept..])
                .await?;
        }
        PreparedCandidate::new(candidate).map_err(Into::into)
    }

    pub(super) async fn admit_inline_candidate(
        &self,
        namespace_id: &NamespaceId,
        mut candidate: PreparedCandidate,
        publisher: &NamespacePublisher,
        permit: &AdmissionPermit,
    ) -> Result<PreparedCandidate> {
        if candidate.candidate.inline_content().is_empty() {
            return Ok(candidate);
        }
        let values = candidate.candidate.ordered_inline_content(namespace_id)?;
        let kept = {
            let slot = publisher.engine.lock().await;
            // An unknown count permits one commit's overshoot per namespace at process start; this limit is a preference.
            let unfolded_bytes = slot
                .engine
                .as_ref()
                .and_then(|engine| engine.wal_fold_input())
                .map_or(0, |input| input.wal_tail_inline_bytes);
            permit.reserve_inline(
                values.iter().map(|value| value.bytes().len()),
                unfolded_bytes,
                publisher.inline_content.inline_content_tail_limit_bytes,
            )
        };
        self.stage_inline_values(namespace_id, &mut candidate.candidate, &values[kept..])
            .await?;
        PreparedCandidate::new(candidate.candidate).map_err(Into::into)
    }

    async fn stage_inline_values(
        &self,
        namespace_id: &NamespaceId,
        candidate: &mut CommitCandidate,
        values: &[InlineContent],
    ) -> Result<()> {
        if values.is_empty() {
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
