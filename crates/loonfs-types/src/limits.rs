//! Bounds that the wire schemas and the core's proofs share.

/// Longest gap a reader may leave between the manifest probe it relies on and
/// the successor check that confirms it, in milliseconds.
///
/// It's also the largest maximum staleness that a live read accepts.
/// `loonfs-core` derives the bound from its publication budgets and checks
/// that derivation against this value at compile time.
pub const READ_REVALIDATION_BOUND_MS: u64 = 1_155_000;
