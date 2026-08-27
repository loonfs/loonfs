//! Work limits for a garbage-collection pass.

/// Limits object-store work performed by one garbage-collection pass.
///
/// One unit is charged for:
///
/// * the namespace control snapshot: head, metadata root, and retention floor
///   read concurrently;
/// * one checkpoint record read while marking, plus two more for the fork
///   target head and its possible metadata-root probe;
/// * one manifest opened, whether to mark its segments or by the content
///   reference scan;
/// * one page of revision rows read out of an opened manifest;
/// * each request used to read the retained WAL chain, including failed
///   requests;
/// * one enumerated candidate key, decided — the original meaning of
///   `max_objects`.
///
/// Prefix listings are not charged. This is a coarse upper bound on store
/// operations, not a cost model.
#[derive(Debug)]
pub struct PassBudget {
    max_objects: Option<u64>,
    spent: u64,
}

impl PassBudget {
    /// Meters a pass at `max_objects` units, or at nothing when absent.
    pub fn new(max_objects: Option<u64>) -> Self {
        Self {
            max_objects,
            spent: 0,
        }
    }

    /// Returns `true` when no more work may be charged.
    pub fn exhausted(&self) -> bool {
        self.remaining() == 0
    }

    /// Returns the remaining allowance. An unlimited pass returns `u64::MAX`.
    pub(super) fn remaining(&self) -> u64 {
        self.max_objects.map_or(u64::MAX, |max_objects| {
            max_objects.saturating_sub(self.spent)
        })
    }

    /// Charges one unit for work already done.
    pub fn charge(&mut self) {
        self.spent = self.spent.saturating_add(1);
    }

    /// Returns the number of charged units.
    #[cfg(test)]
    pub(super) fn spent(&self) -> u64 {
        self.spent
    }

    /// Reserves one unit, returning `false` if the budget is exhausted.
    pub(super) fn try_charge(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        self.charge();
        true
    }

    /// Charges several units for completed work.
    pub(super) fn charge_block(&mut self, units: u64) {
        self.spent = self.spent.saturating_add(units);
    }
}
