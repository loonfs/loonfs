//! Work limits for a garbage-collection pass.

/// Counts caller-defined work steps in one invocation. Namespace GC charges
/// a source step for one root, checkpoint (including fork probes and its basis), anchor candidate,
/// or WAL segment. A merge step writes at most 512 entries. A revision step
/// reads one data block, and a sweep step decides one candidate.
///
/// Listing, progress CAS, and mark-table lookups are supporting work rather
/// than separate units. Source decoding is bounded by the existing manifest,
/// WAL, and data-block formats; this is not a literal count of HTTP requests.
/// A budget of one always permits one resumable step.
#[derive(Debug)]
pub struct PassBudget {
    max_steps: Option<u64>,
    spent: u64,
}

impl PassBudget {
    /// Limits a pass to `max_steps` units; an absent limit allows the run to finish.
    pub fn new(max_steps: Option<u64>) -> Self {
        Self {
            max_steps,
            spent: 0,
        }
    }

    /// Returns `true` when no more work may be charged.
    pub fn exhausted(&self) -> bool {
        self.remaining() == 0
    }

    /// Returns the remaining allowance. An unlimited pass returns `u64::MAX`.
    pub(super) fn remaining(&self) -> u64 {
        self.max_steps
            .map_or(u64::MAX, |max_steps| max_steps.saturating_sub(self.spent))
    }

    /// Charges one unit for work already done.
    pub fn charge(&mut self) {
        self.spent = self.spent.saturating_add(1);
    }

    /// Reserves one unit, returning `false` if the budget is exhausted.
    pub(super) fn try_charge(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        self.charge();
        true
    }
}
