//! One namespace collection call with a fixed clock and no durable progress.

use super::families::CandidateFamily;
use super::live_set::{LiveSet, RetirementState};
use super::reclaim::reclaim_namespace;
use super::sweep::Sweep;
use super::GcOptions;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::manifest::MetadataSegmentCache;
use futures::StreamExt;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{GcResponse, NamespaceId};

/// Collects one namespace. The root scan reads through `segment_cache`
/// and charges the content ids it holds to that cache's read working
/// memory, so a pass never holds more than the configured budget.
pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    options: &GcOptions,
    context: &MutationContext,
) -> Result<GcResponse> {
    options.validate()?;
    let mut report = GcResponse::empty(namespace_id.clone());
    let live = LiveSet::load(
        store,
        segment_cache,
        namespace_id,
        options.grace_window_ms,
        context,
    )
    .await?;
    report.reclaimable_at_ms = live
        .current_tombstone
        .as_ref()
        .map(|tombstone| live.deadline(tombstone));
    report.next_reclamation_at_ms = match live.retirement_state() {
        RetirementState::Retained { until_ms } => until_ms,
        _ => None,
    };
    let mut sweep = Sweep {
        store,
        namespace_id,
        grace_window_ms: options.grace_window_ms,
        mutation: context,
        live: &live,
        report: &mut report,
    };
    for family in CandidateFamily::ALL {
        if family == CandidateFamily::Content && live.namespace_deleted {
            continue;
        }
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
    reclaim_namespace(store, &live, &mut report).await?;
    Ok(report)
}
