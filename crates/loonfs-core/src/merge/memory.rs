//! The memory that planning one merge uses, which the merge's reservation
//! of the merge allowance covers.

use super::MergeAllowance;
use crate::error::Result;
use std::sync::{Arc, Mutex};

/// How much the plan's path caches grow between two reports of their bytes.
const PATH_REPORT_BYTES: usize = 1024 * 1024;

/// The memory that planning one merge uses, as the planner estimates it.
///
/// The plan tracks two numbers: its planned WAL bytes, and its working
/// bytes. The working bytes are the planner's estimate of the set of inodes
/// that the fork's commits touched, the page of the fork's commits that the
/// planner last read, and the paths that the plan's state readers keep.
/// Each report checks the allowance (see [`MergeAllowance::check`]), which
/// grows the merge's reservation to the larger of the estimated peak and the
/// working bytes. The two estimates aren't added, because the peak factor
/// was measured on whole merges, so it already includes the planner's
/// memory for a merge whose changes survive.
///
/// The working bytes that a report checks never fall short of the
/// planner's estimate by more than one page of the fork's commits, which the
/// planner reports after it reads the page, plus 1 MiB of paths, which the
/// path caches report each time they grow by that much. A plan that's
/// abandoned or planned again starts from zero with a new value of this
/// type, while the reservation keeps its size.
pub(super) struct PlanningMemory {
    allowance: MergeAllowance,
    usage: Mutex<Usage>,
}

/// The bytes that one plan reports, and the path bytes at its last report.
#[derive(Default)]
struct Usage {
    planned_bytes: usize,
    touched_bytes: usize,
    page_bytes: usize,
    path_bytes: usize,
    reported_path_bytes: usize,
}

impl Usage {
    fn working_bytes(&self) -> usize {
        self.touched_bytes
            .saturating_add(self.page_bytes)
            .saturating_add(self.path_bytes)
    }
}

impl PlanningMemory {
    /// Returns the memory of a new plan that checks `allowance`.
    pub(super) fn new(allowance: MergeAllowance) -> Arc<Self> {
        Arc::new(Self {
            allowance,
            usage: Mutex::new(Usage::default()),
        })
    }

    /// Sets the plan's planned WAL bytes to `planned_bytes`, and checks the
    /// allowance.
    pub(super) fn check_planned(&self, planned_bytes: usize) -> Result<()> {
        self.report(|usage| {
            usage.planned_bytes = planned_bytes;
            true
        })
    }

    /// Checks the allowance against the bytes reported so far, at the end of
    /// one inode's planning step.
    pub(super) fn check(&self) -> Result<()> {
        self.report(|_| true)
    }

    /// Sets the bytes of the set of touched inodes and of the page of the
    /// fork's commits that the planner just read, and checks the allowance.
    pub(super) fn check_commits(&self, touched_bytes: usize, page_bytes: usize) -> Result<()> {
        self.report(|usage| {
            usage.touched_bytes = touched_bytes;
            usage.page_bytes = page_bytes;
            true
        })
    }

    /// Removes the page of the fork's commits from the working bytes once
    /// the planner has released the page.
    pub(super) fn release_page(&self) {
        self.lock().page_bytes = 0;
    }

    /// Adds the bytes of one path cache entry to the working bytes, without
    /// a check.
    pub(super) fn add_path_bytes(&self, bytes: usize) {
        let mut usage = self.lock();
        usage.path_bytes = usage.path_bytes.saturating_add(bytes);
    }

    /// Checks the allowance when the path caches have grown by at least
    /// [`PATH_REPORT_BYTES`] since the last report.
    pub(super) fn check_paths(&self) -> Result<()> {
        self.report(|usage| usage.path_bytes - usage.reported_path_bytes >= PATH_REPORT_BYTES)
    }

    /// Applies `update` to the usage, and checks the allowance when `update`
    /// returns true. The check grows the merge's reservation through a
    /// trait object, so it runs after the lock is released.
    fn report(&self, update: impl FnOnce(&mut Usage) -> bool) -> Result<()> {
        let (planned_bytes, working_bytes) = {
            let mut usage = self.lock();
            if !update(&mut usage) {
                return Ok(());
            }
            usage.reported_path_bytes = usage.path_bytes;
            (usage.planned_bytes, usage.working_bytes())
        };
        self.allowance.check(planned_bytes, working_bytes)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Usage> {
        self.usage
            .lock()
            .expect("the planning memory lock should not be poisoned")
    }
}
