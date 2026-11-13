//! Retention floor advancement through a floor object in the WAL, and the
//! namespace's retention window through a verified successor manifest.

use super::error::ManifestLoadError;
use super::load::metadata_basis_from_manifest;
use super::publish::ManifestChange;
use super::published::PublicationReport;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::{GC_GRACE_WINDOW_MS, MAX_CONTENTION_ATTEMPTS};
use crate::metadata::manifest_index::commits_after_page;
use crate::namespace::control::{ensure_namespace_live, LoadedManifest};
use crate::namespace::store::NamespaceStore;
use crate::segment::cache::MetadataSegmentCache;
use crate::time::Observation;
use crate::wal::{prepare_floor_object, publish_wal_object, WalPublishError};
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CommitSeq, SetRetentionWindowResponse};
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;

/// Where a retention advance moves the floor.
///
/// The floor never moves down and never passes the folded manifest head.
/// Whatever the target, the advance also treats the grace window
/// ([`GC_GRACE_WINDOW_MS`]) before its clock as a cutoff, so the floor stops
/// at or below the last commit at or before that time, found by the same walk as a
/// [`Self::Cutoff`] target (format spec, "Advancing the retention floor"). A
/// target at or below the current floor leaves the floor where it is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RetentionTarget {
    /// The folded manifest head.
    #[default]
    Head,
    /// This sequence.
    Seq(CommitSeq),
    /// The last commit committed at or before `cutoff_at_ms`.
    ///
    /// Commit times come from writers' clocks and need not increase with
    /// sequence, so the advance reads commits upward from the floor and stops
    /// at the first one committed after the cutoff.
    Cutoff {
        /// A Unix time in milliseconds.
        cutoff_at_ms: u64,
    },
    /// The last commit committed at or before the current time minus the
    /// namespace's retention window. When the namespace has no window, the
    /// floor stays where it is.
    ///
    /// The advance reads the namespace's window from the manifest it loads
    /// to find the target, so a host that advances floors on a schedule
    /// needs no extra request to read each namespace's window.
    Window,
}

/// Checks that every segment that `manifest` lists exists, with one HEAD
/// request for each segment.
pub(crate) async fn verify_manifest_segments_exist<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestPayload,
) -> std::result::Result<(), ManifestLoadError> {
    let object_keys = manifest
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect::<BTreeSet<_>>();
    stream::iter(object_keys)
        .map(|object_key| async move {
            match store
                .head(&object_key)
                .await
                .map_err(|error| ManifestLoadError::ReadSegment {
                    object_key: object_key.clone(),
                    message: error.public_message().into_owned(),
                    kind: error.kind(),
                })? {
                Some(_) => Ok(()),
                None => Err(ManifestLoadError::MissingSegment { object_key }),
            }
        })
        .buffer_unordered(STORE_READS_IN_FLIGHT)
        .try_collect()
        .await
}

/// What a retention advance does, planned from one view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPlan {
    /// The floor stays at this sequence, the view's floor.
    Stays(CommitSeq),
    /// The floor moves up to this sequence.
    Moves(CommitSeq),
}

/// What one floor object publication did: the retention floor of the view
/// after the call, and the floor of the manifest below that view.
///
/// While `manifest_retention_floor_seq` is below `retention_floor_seq`, no
/// manifest records the view's floor yet, and the next fold of the namespace
/// publishes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorObjectReport {
    /// The view's retention floor: the floor that this call created a floor
    /// object for, or a higher floor that the call found.
    pub retention_floor_seq: CommitSeq,
    /// The retention floor of the manifest that the view replays its WAL tail
    /// above.
    pub manifest_retention_floor_seq: CommitSeq,
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Plans where a retention advance toward `target` moves the floor of a
    /// view whose manifest is `current` and whose floor is
    /// `retention_floor_seq`, and publishes nothing (format spec, "Advancing
    /// the retention floor"). The view is a writer position, or the current
    /// manifest and the WAL tail after that manifest. The plan uses the
    /// namespace's wall clock as the current time for the grace window's
    /// cutoff and for a [`RetentionTarget::Window`] cutoff.
    ///
    /// The plan makes no request when the target is at or below the view's
    /// floor. Otherwise, it reads commits upward from the manifest's floor
    /// once, and it stops at the target or at the first commit after the
    /// cutoff, which is the earlier of the target's own cutoff, if it has
    /// one, and the current time minus the grace window. A floor that
    /// isn't above the view's floor stays at the view's floor.
    pub async fn plan_retention_floor(
        &self,
        current: &LoadedManifest,
        retention_floor_seq: CommitSeq,
        target: RetentionTarget,
    ) -> Result<RetentionPlan> {
        let now_ms = self.start_clock().request_at_ms();
        let manifest = current.payload();
        ensure_namespace_live(&manifest.namespace_id, manifest.status)?;
        let age_cutoff_at_ms = now_ms.saturating_sub(GC_GRACE_WINDOW_MS);
        let (through, cutoff_at_ms) = match target {
            RetentionTarget::Head => (manifest.head_seq, age_cutoff_at_ms),
            RetentionTarget::Seq(seq) => (seq.min(manifest.head_seq), age_cutoff_at_ms),
            RetentionTarget::Cutoff { cutoff_at_ms } => {
                (manifest.head_seq, cutoff_at_ms.min(age_cutoff_at_ms))
            }
            RetentionTarget::Window => match manifest.retention_window_ms {
                Some(window_ms) => (
                    manifest.head_seq,
                    now_ms.saturating_sub(window_ms.get()).min(age_cutoff_at_ms),
                ),
                None => (manifest.retention_floor_seq, age_cutoff_at_ms),
            },
        };
        if through <= retention_floor_seq {
            return Ok(RetentionPlan::Stays(retention_floor_seq));
        }
        let floor = last_commit_at_or_before(
            &self.store,
            &self.segment_cache,
            current,
            through,
            cutoff_at_ms,
        )
        .await?;
        Ok(if floor <= retention_floor_seq {
            RetentionPlan::Stays(retention_floor_seq)
        } else {
            RetentionPlan::Moves(floor)
        })
    }

    /// Advances the retention floor to `target` by creating a floor object
    /// at the WAL tip, for a process without a writer session for the
    /// namespace (format spec, "Advancing the retention floor").
    ///
    /// The call first checks that the segments of `planned`, the manifest
    /// that the advance was planned from, exist. Each attempt then runs
    /// discovery, and when the view's floor is below `target`, puts a floor
    /// object at the number after the tip with put-if-absent, with the
    /// writer epoch of the manifest that the attempt's discovery loaded and
    /// within `WAL_PUBLISH_BUDGET_MS` of the discovery's start. A put that
    /// finds the number taken, or that the budget refuses, starts the next
    /// attempt, for at most `MAX_CONTENTION_ATTEMPTS` attempts, and the last
    /// attempt's error is returned when every attempt failed. The call
    /// claims no epoch and writes nothing when the view's floor is already
    /// at or above `target`. An unknown put outcome returns
    /// `outcome_unknown`, and a deleted namespace returns
    /// `namespace_deleted`.
    pub async fn publish_floor_object(
        &self,
        planned: &LoadedManifest,
        target: CommitSeq,
    ) -> Result<FloorObjectReport> {
        verify_manifest_segments_exist(&self.store, planned.payload()).await?;
        self.publish_floor_object_at_tip(target).await
    }

    /// Creates a floor object for `target` at the WAL tip that each attempt's
    /// discovery finds, as [`Self::publish_floor_object`] describes, after
    /// the caller has checked the planned manifest's segments.
    pub(crate) async fn publish_floor_object_at_tip(
        &self,
        target: CommitSeq,
    ) -> Result<FloorObjectReport> {
        let mut last_error = None;
        for _attempt in 0..MAX_CONTENTION_ATTEMPTS {
            let anchor = self
                .load_live_read_anchor(Observation::now(Arc::clone(&self.timer)))
                .await?;
            let head = &anchor.read_state;
            let manifest_retention_floor_seq = anchor.current_manifest().retention_floor_seq();
            if head.retention_floor_seq >= target {
                return Ok(FloorObjectReport {
                    retention_floor_seq: head.retention_floor_seq,
                    manifest_retention_floor_seq,
                });
            }
            let floor_object = prepare_floor_object(head, head.writer_epoch, target)?;
            match publish_wal_object(&self.store, &floor_object, &anchor.checked).await {
                Ok(()) => {
                    return Ok(FloorObjectReport {
                        retention_floor_seq: head
                            .after_wal_object(floor_object.envelope().payload())
                            .retention_floor_seq,
                        manifest_retention_floor_seq,
                    })
                }
                Err(
                    error @ CoreError::WalPublish(
                        WalPublishError::NumberTaken
                        | WalPublishError::PublishBudgetExceeded { .. },
                    ),
                ) => last_error = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(last_error.expect("every attempt should have failed with an error"))
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Sets the namespace's retention window, or clears it when
    /// `retention_window_ms` is `None`, through a successor manifest that changes
    /// nothing else (format spec, "Advancing the retention floor").
    ///
    /// If the current manifest already has the requested window, the call
    /// publishes nothing. The report includes the manifest that records the
    /// change.
    pub async fn set_retention_window(
        &self,
        retention_window_ms: Option<NonZeroU64>,
    ) -> Result<PublicationReport<SetRetentionWindowResponse>> {
        let clock = self.namespace.start_clock();
        let update = self
            .namespace
            .update_live_manifest(
                None,
                &clock,
                |mut payload: NamespaceManifestPayload| async move {
                    if payload.retention_window_ms == retention_window_ms {
                        return Ok(ManifestChange::Finished(()));
                    }
                    payload.retention_window_ms = retention_window_ms;
                    Ok(ManifestChange::Next(Box::new(payload), ()))
                },
            )
            .await?;
        Ok(update.into_report().map(|()| SetRetentionWindowResponse {
            namespace_id: self.namespace.namespace_id.clone(),
            retention_window_ms: retention_window_ms.map(NonZeroU64::get),
        }))
    }
}

/// Returns the sequence of the last commit at or before `cutoff_at_ms` and at
/// or below `through`, read upward from `current`'s floor.
///
/// The walk stops at the first commit committed after the cutoff, even if a
/// later commit is older, because commit times come from writers' clocks and
/// need not increase with sequence. If the first commit above the floor is
/// later than the cutoff, the walk returns the floor.
async fn last_commit_at_or_before<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    current: &LoadedManifest,
    through: CommitSeq,
    cutoff_at_ms: u64,
) -> Result<CommitSeq> {
    let basis = metadata_basis_from_manifest(store, Some(segment_cache), current);
    let mut last = current.retention_floor_seq();
    // One commit per scan: a commit row can be large, and the memo keeps the
    // block for the next call within the caller's read budget.
    while last < through {
        let Some(commit) = commits_after_page(&basis.segments, last, 1, usize::MAX)
            .await?
            .pop()
        else {
            break;
        };
        if commit.committed_at_ms > cutoff_at_ms {
            break;
        }
        last = commit.committed_seq;
    }
    Ok(last)
}
