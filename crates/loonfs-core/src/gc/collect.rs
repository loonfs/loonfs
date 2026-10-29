//! One namespace collection call with a fixed clock and no durable progress.

use super::content::ContentSweep;
use super::deletes::apply_decisions;
use super::families::CandidateFamily;
use super::live_set::{LiveSet, RetirementStatus};
use super::reclaim::reclaim_namespace;
use super::session_cache::CompletedUploadSessionCache;
use super::sweep::Sweep;
use super::uploads::list_uploads;
use super::GcOptions;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::limits::MAX_BLOOM_FILTER_BYTES;
use crate::manifest::MetadataSegmentCache;
use futures::{future, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::content_prefix;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{GcResponse, NamespaceId};
use std::sync::atomic::AtomicU64;

/// Collects one namespace.
///
/// The root scan reads each distinct layout segment of the rooted manifests
/// once per scan, through `segment_cache`, and charges exact content roots to
/// that cache's read working memory. The shared base filter has a
/// separate byte cap. The pass doesn't read the record of a completed upload
/// session that `session_cache` has, and it adds each completed session that
/// it keeps to `session_cache`.
pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    session_cache: Option<&CompletedUploadSessionCache>,
    namespace_id: &NamespaceId,
    options: &GcOptions,
    context: &MutationContext,
    timer: std::sync::Arc<dyn crate::time::MonotonicTimer>,
) -> Result<GcResponse> {
    options.validate()?;
    let mut report = GcResponse::empty(namespace_id.clone());
    let live = LiveSet::load(
        store,
        segment_cache,
        session_cache,
        namespace_id,
        options.grace_window_ms,
        context,
        timer,
    )
    .await?;
    report.reclaimable_at_ms = live
        .current_tombstone
        .as_ref()
        .map(|tombstone| live.deadline(tombstone));
    report.next_reclamation_at_ms = match live.retirement_status() {
        RetirementStatus::Retained { reclaimable_at_ms } => reclaimable_at_ms,
        _ => None,
    };
    let sweep = Sweep {
        store,
        segment_cache,
        namespace_id,
        grace_window_ms: options.grace_window_ms,
        mutation: context,
        live: &live,
        session_cache,
        listed_bytes: AtomicU64::new(0),
    };
    let content =
        ContentSweep::load(&sweep, options.content_shard_rows, MAX_BLOOM_FILTER_BYTES).await?;
    for family in CandidateFamily::ALL {
        if family == CandidateFamily::Content {
            if let Some(content) = &content {
                content.sweep(&sweep, &mut report).await?;
            }
            continue;
        }
        let prefix = family.prefix(namespace_id);
        // Unboxed, rustc cannot prove the pass future `Send` through this stream.
        let decisions = store
            .list_entries_from_stream(&prefix, None)
            .map_err(|error| CoreError::store(&prefix, &error))
            .and_then(|entry| sweep.candidate(family, entry))
            .boxed();
        apply_decisions(store, decisions, &mut report).await?;
    }
    // Upload sessions are swept as pairs of records, from their own listing.
    let decisions = list_uploads(store, namespace_id)
        .and_then(|listed| sweep.upload(listed))
        .boxed();
    apply_decisions(store, decisions, &mut report).await?;
    let content_listed = reclaim_namespace(store, &live, &mut report).await? || content.is_some();
    // A skipped content sweep or a retained tombstone leaves the content
    // unlisted, but the content still occupies storage.
    if !content_listed {
        report.listed_bytes += list_content_bytes(store, namespace_id).await?;
    }
    report.listed_bytes += sweep.listed_bytes.into_inner();
    Ok(report)
}

/// Lists the namespace's content prefix and returns the sum of the listed
/// object sizes.
async fn list_content_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<u64> {
    let prefix = content_prefix(namespace_id);
    store
        .list_entries_from_stream(&prefix, None)
        .map_err(|error| CoreError::store(&prefix, &error))
        .try_fold(0, |total, entry| future::ok(total + entry.size_bytes))
        .await
}
