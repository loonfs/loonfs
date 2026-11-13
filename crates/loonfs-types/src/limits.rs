//! Bounds that the wire schemas and the core's proofs share.

/// The largest maximum staleness that a live read accepts, and the largest
/// interval between the manifest checks of a cached view, in milliseconds.
///
/// An eventual read uses a cached view without a request only while the
/// view's last manifest check is younger than this bound.
pub const READ_REVALIDATION_BOUND_MS: u64 = 1_200_000;

/// How old an unreferenced object must be before a collection deletes it, in
/// milliseconds. Every collection uses this one window.
///
/// A deleted namespace is retired once this window has passed since its
/// deletion, and a retention advance moves the floor only past commits at
/// least this old. `loonfs-core` checks at compile time that the window is
/// longer than a pool's read cap, the clock allowance, an operation
/// snapshot's lifetime, and the provider's request bound together (format
/// spec, "Content roots").
pub const GC_GRACE_WINDOW_MS: u64 = 24 * 60 * 60 * 1000;
