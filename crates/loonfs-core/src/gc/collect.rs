//! One namespace collection call with a fixed clock and no durable progress.

use super::families::CandidateFamily;
use super::live_set::{GenerationState, LiveSet};
use super::reclaim::reclaim_generations;
use super::sweep::Sweep;
use super::uploads::{PublicationView, UploadSweepContext};
use super::GcConfig;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::read_anchor::load_read_anchor;
use futures::StreamExt;
use loonfs_api::{GcResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcResponse> {
    config.validate()?;
    let mut report = GcResponse::empty(namespace_id.clone());
    let anchor = match load_read_anchor(store, namespace_id).await {
        Ok(anchor) => anchor,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(report),
        Err(error) => return Err(error.into()),
    };
    let live = LiveSet::load(
        store,
        namespace_id,
        &anchor,
        config.grace_window_ms,
        context.now_ms,
    )
    .await?;
    report.reclaim_after_ms = live
        .current_tombstone
        .as_ref()
        .map(|tombstone| live.deadline(tombstone));
    report.next_reclamation_at_ms = live
        .tombstones()
        .filter_map(
            |tombstone| match live.generation_state(tombstone.generation) {
                GenerationState::Pending { deadline_ms } => Some(deadline_ms),
                _ => None,
            },
        )
        .min();
    let basis = anchor.basis();
    let view = PublicationView::new(
        store,
        namespace_id,
        (!live.namespace_deleted).then_some(&anchor),
        &basis,
    );
    let mut retained_sessions = BTreeSet::new();
    for family in CandidateFamily::ALL {
        let mut sweep = Sweep {
            store,
            namespace_id,
            grace_window_ms: config.grace_window_ms,
            mutation: context,
            live: &live,
            view: &view,
            upload_sweep: UploadSweepContext::new(store, &live, config.grace_window_ms, context),
            retained_sessions: &mut retained_sessions,
            report: &mut report,
        };
        let prefix = family.prefix(namespace_id);
        let mut listing = store.list_prefix_stream(&prefix);
        while let Some(key) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            sweep.candidate(family, &key).await?;
        }
    }
    reclaim_generations(store, namespace_id, &live, &retained_sessions, &mut report).await?;
    Ok(report)
}
