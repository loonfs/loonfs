//! The plan of a retention floor advance, which a writer session's floor
//! object records in the WAL, and the namespace's retention window through a
//! verified successor manifest.

use super::publish::ManifestChange;
use super::published::PublicationReport;
use crate::commit::publish_view::WriterPosition;
use crate::error::Result;
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::metadata::manifest_index::commits_after_page;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CommitSeq, RetentionWindow, SetRetentionWindowResponse};
use std::num::NonZeroU64;

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

/// What a retention advance does, planned from one manifest and floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPlan {
    /// The floor stays at this sequence, the floor that the plan started
    /// from.
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
    /// manifest of a live namespace with that manifest's floor. The plan
    /// uses the namespace's wall clock as the current time for the grace
    /// window's cutoff and for a [`RetentionTarget::Window`] cutoff.
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
        let floor = last_commit_at_or_before(self, current, through, cutoff_at_ms).await?;
        Ok(if floor <= retention_floor_seq {
            RetentionPlan::Stays(retention_floor_seq)
        } else {
            RetentionPlan::Moves(floor)
        })
    }

    /// Publishes the manifest after the manifest of `position`, a writer
    /// session's position, with the retention window set to
    /// `retention_window_ms`, or cleared when it's `None`, and nothing else
    /// changed (format spec, "Advancing the retention floor").
    ///
    /// The publication builds on the position's manifest, and returns
    /// `head_moved` when that manifest is missing from the store and at or
    /// below the manifest boundary (see [`Self::load_publication_basis`]). If
    /// the position's manifest already has the requested window, the call
    /// publishes nothing, and when a manifest that won its put has the
    /// window, the call returns `head_moved` instead of reporting the change
    /// done (see `ManifestUpdate::published_or_unchanged`). The report includes the manifest that records the change,
    /// which the session records as its newest published manifest, as it
    /// records a fold's (see
    /// [`NamespaceCommitEngine::published_manifest`](crate::publish::NamespaceCommitEngine::published_manifest)).
    ///
    /// Only a writer session's position can call this, so the change is
    /// published only while the session has its epoch, and the put doesn't
    /// wait for the session's commits.
    pub async fn set_retention_window_from(
        &self,
        position: &WriterPosition,
        retention_window_ms: Option<RetentionWindow>,
    ) -> Result<PublicationReport<SetRetentionWindowResponse>> {
        let retention_window_ms = retention_window_ms.map(NonZeroU64::from);
        let clock = self.start_clock();
        let basis = self.load_publication_basis(position).await?;
        let built_on = basis.manifest_no();
        let update = self
            .update_live_manifest(
                Some(basis),
                &clock,
                |mut payload: NamespaceManifestPayload| async move {
                    if payload.retention_window_ms == retention_window_ms {
                        return Ok(ManifestChange::Finished(()));
                    }
                    payload.retention_window_ms = retention_window_ms;
                    Ok(ManifestChange::Next(Box::new(payload), ()))
                },
            )
            .await?
            .published_or_unchanged(built_on)?;
        Ok(update.into_report().map(|()| SetRetentionWindowResponse {
            namespace_id: self.namespace_id.clone(),
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
async fn last_commit_at_or_before<S: ObjectStore>(
    namespace: &NamespaceStore<S>,
    current: &LoadedManifest,
    through: CommitSeq,
    cutoff_at_ms: u64,
) -> Result<CommitSeq> {
    let basis = namespace.current_segments(current);
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
