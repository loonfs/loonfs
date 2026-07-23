//! Small validated-value constructors used throughout tests.

use loonfs_api::{EffectiveLimit, NamespaceId};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

/// Parses a namespace id that is expected to be valid test data.
pub fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid namespace id")
}

/// Constructs a nonzero `usize` that is expected to be valid test data.
pub fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test value should be nonzero")
}

/// Constructs a nonzero `u64` that is expected to be valid test data.
pub fn nonzero_u64(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("test value should be nonzero")
}

/// Constructs a nonzero `u32` that is expected to be valid test data.
pub fn nonzero_u32(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("test value should be nonzero")
}

/// Constructs an effective page limit from a test-sized integer.
pub fn page_limit(value: impl TryInto<u32>) -> EffectiveLimit {
    let value = value
        .try_into()
        .ok()
        .expect("test page limit should fit in u32");
    EffectiveLimit::new(nonzero_u32(value))
}
