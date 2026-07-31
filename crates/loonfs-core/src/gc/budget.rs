//! What `max_objects` meters, and what a pass does when it runs out.

use super::config::GcConfig;

/// The work one garbage-collection pass is allowed to do.
///
/// One unit is one object the pass reads or decides on a candidate's
/// behalf:
///
/// * one enumerated candidate key, decided — the original meaning of
///   `max_objects`;
/// * one manifest opened by the content reference scan;
/// * one page of revision rows read out of an opened manifest;
/// * one retained WAL segment fetched by the scan.
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
pub(super) struct PassBudget {
    max_objects: Option<u64>,
    spent: u64,
}

impl PassBudget {
    pub(super) fn of(config: &GcConfig) -> Self {
        Self {
            max_objects: config.max_objects,
            spent: 0,
        }
    }

    /// True once nothing further may be charged: the caller stops where it
    /// stands. The sweep returns its cursor; the content reference scan
    /// gives up on collecting and the pass defers that reclamation.
    pub(super) fn exhausted(&self) -> bool {
        self.max_objects
            .is_some_and(|max_objects| self.spent >= max_objects)
    }

    /// Charges one unit for work already done.
    pub(super) fn charge(&mut self) {
        self.spent = self.spent.saturating_add(1);
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
}
