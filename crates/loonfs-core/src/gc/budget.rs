//! What `max_objects` meters, and what a pass does when it runs out.

use super::config::GcConfig;

/// The work one garbage-collection pass is allowed to do, from its first
/// read to its last. Marking is inside the bound, not before it.
///
/// One unit is one object the pass reads or decides on a candidate's
/// behalf:
///
/// * the head and the metadata root together — one unit for the pair,
///   because they are read concurrently and neither is a root without the
///   other;
/// * the retention floor;
/// * one checkpoint record read while marking, plus one more for the fork
///   target head that decides whether that record's target is gone;
/// * one manifest opened, whether to mark its tables or by the content
///   reference scan;
/// * one page of revision rows read out of an opened manifest;
/// * the retained WAL chain, charged as one block of one unit per request
///   the load issued for a segment body — a failed request included, since
///   the store was asked. The pass caps that load at what it has left, so
///   the chain cannot read past the bound either;
/// * one enumerated candidate key, decided — the original meaning of
///   `max_objects`.
///
/// Prefix listing is the one thing not metered: it is how a family finds
/// candidates at all, and the cursor is what resumes it. Everything else a
/// pass touches is charged, so no pass can do work proportional to the
/// namespace while asking for a small budget.
///
/// The rule is deliberately coarse — it is a bound, not a cost model. A
/// page of rows costs more than a manifest header and both count as one.
/// What matters is that every store round trip inside the pass has a
/// charge attached to it, and that running out stops the pass instead of
/// letting it finish "just this one thing" unboundedly.
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

    pub(super) fn of(config: &GcConfig) -> Self {
        Self::new(config.max_objects)
    }

    /// True once nothing further may be charged: the caller stops where it
    /// stands. The sweep returns its cursor; the content reference scan
    /// gives up on collecting and the pass defers that reclamation.
    pub fn exhausted(&self) -> bool {
        self.remaining() == 0
    }

    /// How many units are left to spend. An unmetered pass reports the
    /// largest count there is, so a caller that sizes work by what remains
    /// puts no cap on it.
    pub(super) fn remaining(&self) -> u64 {
        match self.max_objects {
            Some(max_objects) => max_objects.saturating_sub(self.spent),
            None => u64::MAX,
        }
    }

    /// Charges one unit for work already done.
    pub fn charge(&mut self) {
        self.spent = self.spent.saturating_add(1);
    }

    /// What has been charged so far. Tests price a namespace's roots with
    /// this, so a bounded case can ask for a budget in candidates rather
    /// than in a number that has to be kept true by hand.
    #[cfg(test)]
    pub(super) fn spent(&self) -> u64 {
        self.spent
    }

    /// Charges one unit for work about to be done. `false` means the
    /// budget is spent and the caller must not do it.
    pub(super) fn try_charge(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        self.charge();
        true
    }

    /// Charges `units` for one block of work already done — the requests
    /// one chain load issued. The caller sizes that load by what
    /// [`PassBudget::remaining`] reports, so a block never charges more
    /// than the budget had left.
    pub(super) fn charge_block(&mut self, units: u64) {
        self.spent = self.spent.saturating_add(units);
    }
}
