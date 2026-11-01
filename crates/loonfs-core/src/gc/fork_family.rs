//! The walk over fork pins that finds the other members of a family root's
//! fork family, so that the family root's collection pass reads every
//! member's roots before it sweeps the family's content.

use super::charged_set::ChargedSet;
use super::live_set::{LiveSet, Roots};
use crate::error::{CoreError, Result};
use crate::limits::FAMILY_ROOTS_READ_BUDGET_MS;
use crate::namespace::store::NamespaceStore;
use crate::time::{Observation, OperationClock};
use loonfs_objectstore::ObjectStore;
use loonfs_types::NamespaceId;
use std::collections::VecDeque;
use std::sync::Arc;

/// How one pass's walk over its fork family ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FamilyWalkOutcome {
    /// The pass read the roots of every member that the walk found.
    Complete,
    /// [`FAMILY_ROOTS_READ_BUDGET_MS`] passed before the pass read every
    /// member's roots.
    BudgetExceeded,
    /// The roots of a member other than the pass's own namespace couldn't be read.
    UnreadableMember,
}

/// What one pass read of its fork family's roots.
#[derive(Clone, Copy, Debug)]
pub(super) struct FamilyWalkReport {
    pub(super) outcome: FamilyWalkOutcome,
    /// The pass's own namespace, and every fork pin target that the walk
    /// found, including a target whose installation hasn't finished.
    pub(super) members: usize,
    /// The members whose roots the pass read. A fork pin target without a
    /// current manifest has no roots, and it counts as read.
    pub(super) members_read: usize,
}

impl FamilyWalkReport {
    /// Returns the report of a pass that reads only its own namespace's roots.
    pub(super) fn own_roots() -> Self {
        Self {
            outcome: FamilyWalkOutcome::Complete,
            members: 1,
            members_read: 1,
        }
    }
}

impl LiveSet {
    /// Reads the roots of every other member of the fork family of
    /// `family_root`, in the order that the walk over fork pins finds them,
    /// starting from `forks`, the targets of the family root's own fork pins.
    ///
    /// Each fork pin's target is a member, and the walk reads each member's
    /// pin records to find the next generation, until no member is left to
    /// read. The walk reads each member once, one member at a time. It stops
    /// at the first member whose roots can't be read, and before a member that
    /// it would start after [`FAMILY_ROOTS_READ_BUDGET_MS`] has passed since
    /// the pass's clock started. Either way the pass skips the content sweep
    /// and continues.
    pub(super) async fn load_family_roots<S: ObjectStore>(
        &mut self,
        family_root: &NamespaceStore<S>,
        forks: Vec<NamespaceId>,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> FamilyWalkReport {
        let mut report = FamilyWalkReport::own_roots();
        // The queue holds at most the members in `found`, which the read
        // working memory bounds.
        let mut found = ChargedSet::new(&self.memory);
        let mut queue = VecDeque::new();
        let walked = async {
            found.insert(family_root.namespace_id.clone())?;
            enqueue_new_members(&mut found, &mut queue, &mut report, forks)?;
            while let Some(member_id) = queue.pop_front() {
                if clock.elapsed_ms() > FAMILY_ROOTS_READ_BUDGET_MS {
                    return Ok(FamilyWalkOutcome::BudgetExceeded);
                }
                let member = family_root.with_namespace(member_id);
                let forks = self
                    .load_member_roots(&member, grace_window_ms, clock)
                    .await?;
                report.members_read += 1;
                enqueue_new_members(&mut found, &mut queue, &mut report, forks)?;
            }
            // A member read that ends after the budget is as late as one that
            // starts after it.
            Ok::<_, CoreError>(
                if report.members > 1 && clock.elapsed_ms() > FAMILY_ROOTS_READ_BUDGET_MS {
                    FamilyWalkOutcome::BudgetExceeded
                } else {
                    FamilyWalkOutcome::Complete
                },
            )
        }
        .await;
        report.outcome = walked.unwrap_or_else(|error| {
            tracing::warn!(
                namespace_id = %family_root.namespace_id,
                error = %error,
                "a fork family member's roots couldn't be read; skipping the family's content sweep"
            );
            FamilyWalkOutcome::UnreadableMember
        });
        report
    }

    /// Adds the roots of `member`, another member of the fork family, and
    /// returns the targets of its fork pins.
    ///
    /// A fork pin's target has no current manifest until its installation
    /// finishes, and it has no roots and no forks until then.
    async fn load_member_roots<S: ObjectStore>(
        &mut self,
        member: &NamespaceStore<S>,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<Vec<NamespaceId>> {
        let observed = Observation::now(Arc::clone(&member.timer));
        let (manifest, hint) = match member.load_current_manifest_with_hint().await {
            Ok(current) => current,
            Err(CoreError::NamespaceNotFound { .. }) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let anchor = member
            .load_read_anchor_from_manifest(manifest, hint, observed)
            .await?;
        let pins = self
            .load_namespace_roots(member, &anchor, Roots::Member, grace_window_ms, clock)
            .await?;
        Ok(pins.fork_targets)
    }
}

/// Adds each of `members` that `found` doesn't have to `found` and to the
/// back of `queue`, and counts it in `report`.
fn enqueue_new_members(
    found: &mut ChargedSet<NamespaceId>,
    queue: &mut VecDeque<NamespaceId>,
    report: &mut FamilyWalkReport,
    members: impl IntoIterator<Item = NamespaceId>,
) -> Result<()> {
    for member_id in members {
        if !found.contains(&member_id) {
            found.insert(member_id.clone())?;
            queue.push_back(member_id);
            report.members += 1;
        }
    }
    Ok(())
}
