//! The walk over fork pins that finds the other members of a family root's
//! fork family, so that the family root's collection pass reads every
//! member's roots before it sweeps the family's content.

use super::live_set::{LiveSet, RetirementStatus, Roots};
use crate::error::{CoreError, Result};
use crate::limits::FAMILY_ROOTS_READ_BUDGET_MS;
use crate::namespace::store::NamespaceStore;
use crate::time::{Observation, OperationClock};
use loonfs_objectstore::ObjectStore;
use loonfs_types::NamespaceId;
use std::sync::Arc;

/// How far the pass has read one member of its fork family.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberState {
    /// The namespace that the pass collects. The pass reads its roots before
    /// the walk starts, and [`LiveSet::retirement_status`] returns its status.
    Collected,
    /// The member is the target of a fork pin that the walk listed, and the
    /// pass hasn't read its roots yet.
    Found,
    /// The pass has read the member's roots, and its retirement status by the
    /// pass's clock.
    Read { status: RetirementStatus },
    /// The member is the target of a fork pin, and it has no current
    /// manifest, because its installation hasn't finished, or it failed. It
    /// has no roots, and no commit can be published in it.
    Absent,
}

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
    /// starting from the members that the family root's own pin listing
    /// found.
    ///
    /// Each fork pin's target is a member, and the walk reads each member's
    /// pin records to find the next generation, until no member is left to
    /// read. The walk reads each member once, one member at a time. It stops
    /// at the first member whose roots can't be read, and before a member that
    /// it would start after [`FAMILY_ROOTS_READ_BUDGET_MS`] has passed since
    /// the pass's clock started. Either way the pass skips the content sweep
    /// and continues. A charge to the read working memory that fails ends the
    /// pass, as it does while the pass reads its own roots.
    pub(super) async fn load_family_roots<S: ObjectStore>(
        &mut self,
        family_root: &NamespaceStore<S>,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<()> {
        let walked = async {
            while let Some(member_id) = self.member_queue.pop_front() {
                if clock.elapsed_ms() > FAMILY_ROOTS_READ_BUDGET_MS {
                    return Ok(FamilyWalkOutcome::BudgetExceeded);
                }
                let member = family_root.with_namespace(member_id.clone());
                let state = self
                    .load_member_roots(&member, grace_window_ms, clock)
                    .await?;
                self.members.insert(member_id, state)?;
                self.family.members_read += 1;
            }
            // A member read that ends after the budget is as late as one that
            // starts after it.
            Ok::<_, CoreError>(
                if self.family.members > 1 && clock.elapsed_ms() > FAMILY_ROOTS_READ_BUDGET_MS {
                    FamilyWalkOutcome::BudgetExceeded
                } else {
                    FamilyWalkOutcome::Complete
                },
            )
        }
        .await;
        self.family.outcome = match walked {
            Ok(outcome) => outcome,
            Err(error @ CoreError::LiveSetExceedsReadMemory { .. }) => return Err(error),
            Err(error) => {
                tracing::warn!(
                    namespace_id = %family_root.namespace_id,
                    error = %error,
                    "a fork family member's roots couldn't be read; skipping the family's content sweep"
                );
                FamilyWalkOutcome::UnreadableMember
            }
        };
        Ok(())
    }

    /// Adds the roots of `member`, another member of the fork family, adds
    /// the targets of its fork pins to the walk, and returns the member's
    /// state.
    ///
    /// A fork pin's target has no current manifest until its installation
    /// finishes, and it has no roots and no forks until then.
    async fn load_member_roots<S: ObjectStore>(
        &mut self,
        member: &NamespaceStore<S>,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<MemberState> {
        let observed = Observation::now(Arc::clone(&member.timer));
        let (manifest, hint) = match member.load_current_manifest_with_hint().await {
            Ok(current) => current,
            Err(CoreError::NamespaceNotFound { .. }) => return Ok(MemberState::Absent),
            Err(error) => return Err(error),
        };
        let anchor = member
            .load_read_anchor_from_manifest(manifest, hint, observed)
            .await?;
        let pins = self
            .load_namespace_roots(member, &anchor, Roots::Member, grace_window_ms, clock)
            .await?;
        let head = anchor.manifest.payload();
        Ok(MemberState::Read {
            status: RetirementStatus::of(
                head.status.is_deleted().then_some(head),
                pins.any_key,
                self.retirement_grace_window_ms,
                self.now_ms,
            ),
        })
    }

    /// Returns the retirement status of `namespace_id`, the namespace of an
    /// upload session that the pass sweeps, or `None` when the pass can't
    /// decide it.
    ///
    /// The pass can't decide the status of a member whose roots it hasn't
    /// read, or of a namespace that the walk didn't find when the walk
    /// stopped early. A member without a current manifest, and a namespace
    /// that a finished walk didn't find, are eligible, because no commit can
    /// be published in the first, and only retirement deletes the fork pin
    /// that would find the second. That holds for a namespace with a session that
    /// the pass listed before the walk, because a namespace has a current
    /// manifest before it can create a session. A fork installed after the
    /// walk read its source's pins isn't found either, so a session that the
    /// pass didn't list as completed before the walk can't rely on this status
    /// to skip the completed-content grace.
    pub(super) fn find_retirement_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<RetirementStatus> {
        match self.members.get(namespace_id) {
            Some(MemberState::Collected) => Some(self.retirement_status()),
            Some(MemberState::Read { status }) => Some(*status),
            Some(MemberState::Absent) => Some(RetirementStatus::Eligible),
            Some(MemberState::Found) => None,
            None => match self.family.outcome {
                FamilyWalkOutcome::Complete => Some(RetirementStatus::Eligible),
                FamilyWalkOutcome::BudgetExceeded | FamilyWalkOutcome::UnreadableMember => None,
            },
        }
    }

    /// Adds `target_namespace_id`, the target of a fork pin, to the members
    /// that the walk has found and to the back of the walk's queue, unless the
    /// walk has found it already.
    pub(super) fn add_fork_target(&mut self, target_namespace_id: &NamespaceId) -> Result<()> {
        if self.members.get(target_namespace_id).is_some() {
            return Ok(());
        }
        self.members
            .insert(target_namespace_id.clone(), MemberState::Found)?;
        self.member_queue.push_back(target_namespace_id.clone())?;
        self.family.members += 1;
        Ok(())
    }
}
