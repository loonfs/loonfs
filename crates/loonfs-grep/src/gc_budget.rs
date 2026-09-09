//! Work limits for grep garbage collection.

pub(crate) const DEFAULT_GC_MAX_STEPS: u64 = 1024;

pub(crate) struct GcBudget {
    remaining: u64,
}

impl GcBudget {
    pub(crate) fn new(max_steps: u64) -> Self {
        Self {
            remaining: max_steps,
        }
    }

    pub(crate) fn exhausted(&self) -> bool {
        self.remaining == 0
    }

    pub(crate) fn charge(&mut self) {
        self.remaining = self.remaining.saturating_sub(1);
    }
}
