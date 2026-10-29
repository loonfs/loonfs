//! Checks the options of one namespace collection against the core's limits.

use crate::error::{CoreError, Result};
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
pub use loonfs_types::options::GcOptions;

/// Rejects options that would let a pass delete an object that a publication
/// still in flight wrote, or that would make the pass take no rows per shard.
pub(super) fn validate_gc_options(options: &GcOptions) -> Result<()> {
    // The minimum grace window is derived from the publication budgets and
    // provider deadlines in `limits`. Below it, a publish still in flight
    // could have written objects that already look old enough to delete, so
    // the configuration is rejected outright.
    if options.grace_window_ms < GC_MIN_GRACE_WINDOW_MS {
        return Err(CoreError::InvalidGcOptions(format!(
            "`grace_window_ms` {} is below the derived safety minimum {}",
            options.grace_window_ms, GC_MIN_GRACE_WINDOW_MS
        )));
    }
    if options.content_shard_rows == 0 {
        return Err(CoreError::InvalidGcOptions(
            "`content_shard_rows` must be at least 1".to_owned(),
        ));
    }
    Ok(())
}
