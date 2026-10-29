//! Integer width conversions that the 64-bit target makes lossless.

// LoonFS sizes, offsets, and counts are `u64` in formats and on the wire, and
// they index in-memory buffers as `usize`. Building only for 64-bit targets
// makes that conversion lossless, so one function states it instead of a
// checked conversion at each site.
const _: () = assert!(
    usize::BITS == u64::BITS,
    "LoonFS builds only for targets whose `usize` is 64 bits"
);

/// Converts a `u64` to `usize`. Lossless, because LoonFS builds only for
/// 64-bit targets (a `const` assertion in this module fails the build
/// elsewhere).
#[allow(
    clippy::cast_possible_truncation,
    reason = "the module's const assertion makes usize 64 bits"
)]
pub const fn usize_from_u64(value: u64) -> usize {
    value as usize
}

/// Converts a `usize` to `u64`, which is lossless because LoonFS builds only for 64-bit targets.
pub const fn u64_from_usize(value: usize) -> u64 {
    value as u64
}
