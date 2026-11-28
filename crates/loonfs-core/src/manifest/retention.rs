//! The plan of a retention floor advance and the manifest that raises the
//! floor, and the namespace's retention window through a verified successor
//! manifest.

use super::published::PublicationReport;
use crate::commit::publication_lane::{LaneGuard, LaneTurn};
use crate::commit::publish_view::WriterPosition;
use crate::error::Result;
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::metadata::manifest_index::commits_after_page;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
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

/// What a retention advance does, planned from one manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPlan {
    /// The floor stays at this sequence, the floor that the plan started
    /// from.
    Stays(CommitSeq),
    /// The floor moves up to this sequence.
    Moves(CommitSeq),
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Plans where a retention advance toward `target` moves the floor of
    /// `current`, the manifest of a writer position or the current manifest
    /// of a live namespace, and publishes nothing (format spec, "Advancing
    /// the retention floor"). The plan uses the namespace's wall clock as the
    /// current time for the grace window's cutoff and for a
    /// [`RetentionTarget::Window`] cutoff.
    ///
    /// The plan makes no request when the target is at or below the
    /// manifest's floor. Otherwise, it reads commits upward from that floor
    /// once, and it stops at the target or at the first commit after the
    /// cutoff, which is the earlier of the target's own cutoff, if it has
    /// one, and the current time minus the grace window. A floor that isn't
    /// above the manifest's floor stays at the manifest's floor.
    pub async fn plan_retention_floor(
        &self,
        current: &LoadedManifest,
        target: RetentionTarget,
    ) -> Result<RetentionPlan> {
        let now_ms = self.start_clock().request_at_ms();
        let manifest = current.payload();
        let retention_floor_seq = manifest.retention_floor_seq;
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

    /// Checks that every segment of `planned`, the manifest that a retention
    /// advance planned its floor from, exists, before the advance ends the
    /// replay guarantee for the history below the new floor (format spec,
    /// "Advancing the retention floor"). The check sends one HEAD for each
    /// segment. A missing segment returns `manifest_replaced` when a newer
    /// manifest replaced `planned`, so the caller plans again from the
    /// newest manifest, and `namespace_corrupt` otherwise.
    pub async fn verify_planned_segments(&self, planned: &LoadedManifest) -> Result<()> {
        self.current_segments(planned)
            .segments
            .verify_segments_exist()
            .await
    }

    /// Folds the WAL tail of `position`, a writer session's position, and
    /// publishes one manifest that includes that fold and whose floor is the
    /// higher of the old floor and `target`, as one publication of the
    /// session's publication lane (format spec, "Advancing the retention
    /// floor"). It is a retention advance's last step, the fold of what
    /// arrived during the advance's ordinary fold.
    ///
    /// The caller keeps `lane`, the session's publication lane, and the
    /// session's commit queue for this whole step, after the WAL put in
    /// flight was settled, so every commit written before the manifest is in
    /// it, and every commit after it is validated against the new floor. The
    /// manifest is published even when nothing arrived, because the floor is
    /// in it, and nothing is published only when nothing arrived and the old
    /// floor is already at or above `target`. The report includes the floor
    /// of the session's newest published manifest after the call, and the
    /// manifest that the call published.
    pub(crate) async fn raise_retention_floor(
        &self,
        position: &WriterPosition,
        lane: &LaneGuard<'_>,
        target: CommitSeq,
        clock: &OperationClock,
    ) -> Result<PublicationReport<CommitSeq>> {
        let published = self
            .fold_while_paused(position, lane, clock, |payload| {
                payload.retention_floor_seq = payload.retention_floor_seq.max(target);
            })
            .await?;
        Ok(PublicationReport {
            response: lane
                .acquired_turn()
                .basis()
                .manifest()
                .retention_floor_seq(),
            published,
        })
    }

    /// Publishes the manifest after the turn's basis, the newest manifest
    /// that a writer session published, as a publication of the session's
    /// publication lane, whose turn is `turn`, with the retention window set
    /// to `retention_window_ms`, or cleared when it's `None`, and nothing
    /// else changed (format spec, "Advancing the retention floor").
    ///
    /// The manifest is put once (see [`Self::publish_manifest_in_turn`]).
    /// If the basis already has the requested window, the call publishes
    /// nothing. The report includes the manifest that the call published.
    /// Only a publication of the session's lane can call this, so the
    /// change is published only while the session has its epoch, it takes
    /// its turn with the session's other manifest publications, and the put
    /// doesn't wait for the session's commits.
    pub async fn set_retention_window_from(
        &self,
        turn: LaneTurn<'_>,
        retention_window_ms: Option<RetentionWindow>,
    ) -> Result<PublicationReport<SetRetentionWindowResponse>> {
        let retention_window_ms = retention_window_ms.map(NonZeroU64::from);
        let clock = self.start_clock();
        let mut payload = turn.basis().manifest().payload().clone();
        let response = SetRetentionWindowResponse {
            namespace_id: self.namespace_id.clone(),
            retention_window_ms: retention_window_ms.map(NonZeroU64::get),
        };
        if payload.retention_window_ms == retention_window_ms {
            return Ok(PublicationReport {
                response,
                published: None,
            });
        }
        payload.retention_window_ms = retention_window_ms;
        let published = self.publish_manifest_in_turn(turn, payload, &clock).await?;
        Ok(PublicationReport {
            response,
            published: Some(published),
        })
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
