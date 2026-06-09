use crate::digest::sha256_digest;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Kind of content reference.
///
/// Serializes as a plain string (`"whole_file_v0"`). Kinds unknown to this
/// build decode as [`ContentRefKind::Unsupported`] carrying the original
/// string, and re-serialize to that same string — so a reader that merely
/// relays or rewrites rows it does not fully understand can never destroy a
/// newer kind. Writers must not *create* references with an unsupported
/// kind; commit validation rejects them (spec 050 §1.3).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContentRefKind {
    /// Whole-file v0 content addressed by SHA-256.
    WholeFileV0,
    /// A content kind unknown to this build, preserved verbatim.
    Unsupported(String),
}

impl ContentRefKind {
    const WHOLE_FILE_V0: &'static str = "whole_file_v0";

    pub fn as_str(&self) -> &str {
        match self {
            Self::WholeFileV0 => Self::WHOLE_FILE_V0,
            Self::Unsupported(other) => other,
        }
    }
}

impl fmt::Display for ContentRefKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for ContentRefKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ContentRefKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            Self::WHOLE_FILE_V0 => Self::WholeFileV0,
            _ => Self::Unsupported(value),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Pointer to immutable file content.
///
/// A `ContentRef` is safe to publish only after the referenced bytes are
/// durable in the namespace's content store.
pub struct ContentRef {
    /// Content strategy used by the referenced object.
    pub kind: ContentRefKind,
    /// Digest string, currently `sha256:<64 lowercase hex>`.
    pub digest: String,
    /// Complete byte length of the referenced file content.
    pub size_bytes: u64,
}

impl ContentRef {
    /// Builds a whole-file v0 reference for these bytes.
    pub fn whole_file_v0(bytes: &[u8]) -> Self {
        Self {
            kind: ContentRefKind::WholeFileV0,
            digest: sha256_digest(bytes),
            size_bytes: bytes.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ContentRefKind;

    #[test]
    fn known_kind_round_trips_as_snake_case_string() {
        let encoded = serde_json::to_string(&ContentRefKind::WholeFileV0).expect("encode");
        assert_eq!(encoded, "\"whole_file_v0\"");
        let decoded: ContentRefKind = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, ContentRefKind::WholeFileV0);
    }

    #[test]
    fn unknown_kind_is_preserved_verbatim_through_a_round_trip() {
        let decoded: ContentRefKind =
            serde_json::from_str("\"sparse_file_v9\"").expect("decode unknown kind");
        assert_eq!(
            decoded,
            ContentRefKind::Unsupported("sparse_file_v9".to_owned())
        );
        let reencoded = serde_json::to_string(&decoded).expect("encode unknown kind");
        assert_eq!(reencoded, "\"sparse_file_v9\"");
    }
}
