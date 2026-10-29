//! Bounds that the wire schemas and the core's proofs share.

/// Longest gap a reader may leave between the manifest probe it relies on and
/// the successor check that confirms it, in milliseconds.
///
/// It's also the largest maximum staleness that a live read accepts.
/// `loonfs-core` derives the bound from its publication budgets and checks
/// that derivation against this value at compile time.
pub const READ_REVALIDATION_BOUND_MS: u64 = 1_155_000;

/// Default age of an unreachable object before garbage collection may remove
/// it, in milliseconds.
///
/// A retention advance also treats this long before its clock as a cutoff,
/// so the floor doesn't pass a commit dated after that time, and a replaced
/// revision stays readable until the replacing commit is this old (format
/// spec, "Advancing the retention floor"). `loonfs-core` checks at compile
/// time that it isn't below the derived minimum grace window.
pub const GC_DEFAULT_GRACE_WINDOW_MS: u64 = 60 * 60 * 1000;
