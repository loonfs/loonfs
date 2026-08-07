//! Small validated-value constructors used throughout tests.

use loonfs_api::{
    AttributeKey, AttributeValue, ContentId, ContentRef, EffectiveLimit, NamespaceId,
};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

/// Parses a namespace id that is expected to be valid test data.
pub fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid namespace id")
}

/// Parses an attribute key that is expected to be valid test data.
pub fn attribute_key(value: &str) -> AttributeKey {
    AttributeKey::parse(value).expect("valid attribute key")
}

/// Builds a one-string attribute value.
pub fn attribute_text(value: &str) -> AttributeValue {
    AttributeValue::String {
        value: value.to_owned(),
    }
}

/// Parses a content id that is expected to be valid test data.
pub fn content_id(value: &str) -> ContentId {
    ContentId::parse(value).expect("valid content id")
}

/// Builds a reference to a fresh content object holding `bytes`, the way
/// every production write path does.
///
/// Each call mints a new identity, so two calls with the same bytes give two
/// distinct references. A test that needs one object referenced twice should
/// clone the reference.
pub fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef::blob_v1(ContentId::generate(), bytes)
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
