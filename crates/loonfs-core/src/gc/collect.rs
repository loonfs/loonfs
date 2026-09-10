//! One namespace collection call with a fixed clock and no durable progress.

use super::families::CandidateFamily;
use super::fork_checkpoints::release_source_checkpoint;
use super::live_set::LiveSet;
use super::sweep::Sweep;
use super::uploads::{PublicationView, UploadSweepContext};
use super::GcConfig;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_namespace_read_state;
use crate::namespace::read_anchor::load_read_anchor;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use futures::StreamExt;
use loonfs_api::{GcResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcResponse> {
    let timer = StdMonotonicTimer::default();
    gc_namespace_with_timer(store, namespace_id, config, context, &timer).await
}

pub(super) async fn gc_namespace_with_timer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<GcResponse> {
    let started_ms = timer.monotonic_now_ms();
    config.validate()?;
    let mut report = GcResponse::empty(namespace_id.clone());
    let anchor = match load_read_anchor(store, namespace_id).await {
        Ok(anchor) => anchor,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(report),
        Err(error) => return Err(error.into()),
    };
    let live = LiveSet::load(store, namespace_id, &anchor).await?;
    let basis = anchor.basis();
    let view = PublicationView::new(
        store,
        namespace_id,
        (!live.namespace_deleted).then_some(&anchor),
        &basis,
    );
    let retired_content = live.retired_content(context.now_ms);
    let mut checkpoints_retained = false;
    for family in CandidateFamily::ALL {
        if family == CandidateFamily::OwnedContent {
            if !retired_content {
                continue;
            }
            verify_retired_owner(store, namespace_id, &live, context.now_ms).await?;
            if let Some(basis) = &anchor.read_state.fork_basis {
                if release_source_checkpoint(store, basis).await? {
                    report.released_checkpoints.fork += 1;
                    report.deleted.checkpoint_records += 1;
                }
            }
        }
        let mut sweep = Sweep {
            store,
            namespace_id,
            grace_window_ms: config.grace_window_ms,
            mutation: context,
            live: &live,
            view: &view,
            upload_sweep: UploadSweepContext::new(
                store,
                namespace_id,
                live.content_store_id.clone(),
                retired_content,
                config.grace_window_ms,
                context,
            ),
            checkpoints_retained: &mut checkpoints_retained,
            report: &mut report,
        };
        let prefix = family.prefix(namespace_id, &live);
        let mut listing = store.list_prefix_stream(&prefix);
        while let Some(key) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            sweep.candidate(family, &key).await?;
        }
        if family == CandidateFamily::Checkpoints
            && live.namespace_deleted
            && live.reclaim_after_ms.is_none()
            && !*sweep.checkpoints_retained
        {
            crate::namespace::delete::retire_namespace(
                store,
                namespace_id,
                config.grace_window_ms,
                context.now_ms,
                timer,
                started_ms,
            )
            .await?;
        }
    }
    retirement_report(store, namespace_id, context.now_ms, report).await
}

async fn verify_retired_owner<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    live: &LiveSet,
    now_ms: u64,
) -> Result<()> {
    let head = load_namespace_read_state(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if !head.status.is_deleted()
        || head.content_store_id != live.content_store_id
        || head
            .status
            .reclaim_after_ms()
            .is_none_or(|deadline| now_ms < deadline)
    {
        return Err(CoreError::NamespaceCorrupt(
            "retired namespace head does not match content sweep roots".to_owned(),
        ));
    }
    Ok(())
}

async fn retirement_report<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    now_ms: u64,
    mut report: GcResponse,
) -> Result<GcResponse> {
    let head = load_namespace_read_state(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    report.reclaim_after_ms = head.status.reclaim_after_ms();
    if let Some(deadline) = report
        .reclaim_after_ms
        .filter(|deadline| *deadline > now_ms)
    {
        report.next_reclamation_at_ms = Some(
            report
                .next_reclamation_at_ms
                .map_or(deadline, |current| current.min(deadline)),
        );
    }
    Ok(report)
}
