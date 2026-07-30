//! [`ContentRef`]: the durable reference a file revision points at, naming
//! one immutable content object and carrying the integrity evidence for it.

use crate::hex::hex_encode_bytes;
use crate::ids::ContentId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;

/// Kind of content reference.
///
/// Serializes as a plain string (`"blob_v1"`). Kinds unknown to this build
/// decode as [`ContentRefKind::Unsupported`] carrying the original string,
/// and re-serialize to that same string — so a reader that merely relays or
/// rewrites rows it does not fully understand can never destroy a newer
/// kind. Writers must not *create* references with an unsupported kind;
/// commit validation rejects them (format spec, "Validation and logical commits").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContentRefKind {
    /// One immutable content object, addressed by its random content id.
    BlobV1,
    /// A content kind unknown to this build, preserved verbatim.
    Unsupported(String),
}

impl ContentRefKind {
    const BLOB_V1: &'static str = "blob_v1";

    /// Returns the frozen wire spelling, including an unknown spelling preserved by a reader.
    pub fn as_str(&self) -> &str {
        match self {
            Self::BlobV1 => Self::BLOB_V1,
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
            Self::BLOB_V1 => Self::BlobV1,
            _ => Self::Unsupported(value),
        })
    }
}

/// Algorithm of a stored full-object checksum.
///
/// Every algorithm here covers the complete object. There is deliberately no
/// checksum-*type* field: full-object coverage is an invariant of this
/// format, established when the object is written, never read back from a
/// provider. (Cloudflare R2 never reports `x-amz-checksum-type` at all, so a
/// type read back would be unavailable exactly where it would matter.)
///
/// Only [`ChecksumAlgorithm::Sha256`] has producers today. `Crc64nvme` and
/// `Crc32c` decode and round-trip so that direct multipart, which mandates
/// provider-computed full-object CRC-64/NVME, needs no format change to
/// start writing them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ChecksumAlgorithm {
    /// SHA-256 over the complete object.
    Sha256,
    /// CRC-64/NVME over the complete object.
    Crc64nvme,
    /// CRC-32C over the complete object.
    Crc32c,
}

impl ChecksumAlgorithm {
    /// Returns the frozen wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Crc64nvme => "crc64nvme",
            Self::Crc32c => "crc32c",
        }
    }

    /// Returns the raw checksum width in bytes.
    pub fn value_bytes(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Crc64nvme => 8,
            Self::Crc32c => 4,
        }
    }
}

impl fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A checksum computed over the complete bytes of one content object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StorageChecksum {
    /// Algorithm that produced `value`.
    pub algorithm: ChecksumAlgorithm,
    /// Lowercase hex of the raw checksum bytes.
    ///
    /// The algorithm is its own field, so the value carries no prefix.
    /// Provider APIs that report base64 are converted at the adapter.
    pub value: String,
}

impl StorageChecksum {
    /// Builds the SHA-256 storage checksum for these complete bytes.
    pub fn sha256(bytes: &[u8]) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Sha256,
            value: hex_encode_bytes(&Sha256::digest(bytes)),
        }
    }
}

/// Describes why a content reference cannot be part of a durable commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ContentRefValidationError {
    /// The reference names a content strategy this build cannot write.
    #[error("unsupported content ref kind `{kind}`")]
    UnsupportedKind {
        /// Kind spelling carried by the rejected reference.
        kind: String,
    },
    /// A checksum value was not the algorithm's width in lowercase hex.
    #[error("invalid {field} for algorithm `{algorithm}`: {reason}")]
    InvalidChecksum {
        /// Reference field that carried the rejected value.
        field: String,
        /// Algorithm whose width and alphabet the value violated.
        algorithm: ChecksumAlgorithm,
        /// Specific rule the value broke.
        reason: String,
    },
}

/// Pointer to one immutable content object.
///
/// `content_id` is identity — *which* object — and the checksums are
/// evidence about its bytes. Separating the two is what lets the final
/// object key exist before the first byte is read.
///
/// A `ContentRef` is safe to publish only after the referenced bytes are
/// durable in the namespace's content store.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ContentRef {
    /// Content strategy used by the referenced object.
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub kind: ContentRefKind,
    /// Immutable identity of the referenced object.
    pub content_id: ContentId,
    /// Complete byte length of the referenced content.
    pub size_bytes: u64,
    /// Mandatory checksum over the complete object, used to verify the
    /// stored bytes against this reference without downloading them.
    pub storage_checksum: StorageChecksum,
    /// SHA-256 over the complete payload, lowercase hex, when a trusted
    /// party computed it.
    ///
    /// Present means the LoonFS write path hashed the whole stream itself,
    /// or a provider validated a signed whole-object SHA-256 on the write.
    /// There are no client-claimed digests: absent means nobody trustworthy
    /// hashed these bytes, never "the client did not tell us".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whole_file_sha256: Option<String>,
}

impl ContentRef {
    /// Builds a reference to a freshly minted content object holding these bytes.
    ///
    /// Every caller of this constructor moves the bytes through the LoonFS
    /// write path, so the whole-file SHA-256 is trusted by construction.
    pub fn blob_v1(content_id: ContentId, bytes: &[u8]) -> Self {
        let storage_checksum = StorageChecksum::sha256(bytes);
        Self {
            kind: ContentRefKind::BlobV1,
            content_id,
            size_bytes: bytes.len() as u64,
            whole_file_sha256: Some(storage_checksum.value.clone()),
            storage_checksum,
        }
    }

    /// Reports whether the reference is well formed enough to publish.
    ///
    /// This is a shape check on the reference itself; proving that the
    /// object exists and matches is the storage layer's job.
    pub fn validate(&self) -> Result<(), ContentRefValidationError> {
        if self.kind != ContentRefKind::BlobV1 {
            return Err(ContentRefValidationError::UnsupportedKind {
                kind: self.kind.as_str().to_owned(),
            });
        }
        validate_checksum_value(
            "storage_checksum",
            self.storage_checksum.algorithm,
            &self.storage_checksum.value,
        )?;
        if let Some(whole_file_sha256) = &self.whole_file_sha256 {
            validate_checksum_value(
                "whole_file_sha256",
                ChecksumAlgorithm::Sha256,
                whole_file_sha256,
            )?;
        }
        Ok(())
    }
}

fn validate_checksum_value(
    field: &str,
    algorithm: ChecksumAlgorithm,
    value: &str,
) -> Result<(), ContentRefValidationError> {
    let expected_len = algorithm.value_bytes() * 2;
    if value.len() != expected_len {
        return Err(ContentRefValidationError::InvalidChecksum {
            field: field.to_owned(),
            algorithm,
            reason: format!("must be {expected_len} hex characters, got {}", value.len()),
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ContentRefValidationError::InvalidChecksum {
            field: field.to_owned(),
            algorithm,
            reason: "must be lowercase hex".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ChecksumAlgorithm, ContentRef, ContentRefKind, ContentRefValidationError, StorageChecksum,
    };
    use crate::ids::ContentId;

    fn content_id() -> ContentId {
        ContentId::parse("cnt_0123456789abcdef0123456789abcdef").expect("valid content id")
    }

    #[test]
    fn known_kind_round_trips_as_snake_case_string() {
        let encoded = serde_json::to_string(&ContentRefKind::BlobV1).expect("encode");
        assert_eq!(encoded, "\"blob_v1\"");
        let decoded: ContentRefKind = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, ContentRefKind::BlobV1);
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

    #[test]
    fn every_checksum_algorithm_round_trips() {
        for (algorithm, wire) in [
            (ChecksumAlgorithm::Sha256, "\"sha256\""),
            (ChecksumAlgorithm::Crc64nvme, "\"crc64nvme\""),
            (ChecksumAlgorithm::Crc32c, "\"crc32c\""),
        ] {
            let encoded = serde_json::to_string(&algorithm).expect("encode algorithm");
            assert_eq!(encoded, wire);
            let decoded: ChecksumAlgorithm =
                serde_json::from_str(&encoded).expect("decode algorithm");
            assert_eq!(decoded, algorithm);
        }
    }

    #[test]
    fn a_content_ref_rejects_unknown_fields() {
        let json = r#"{
            "kind": "blob_v1",
            "content_id": "cnt_0123456789abcdef0123456789abcdef",
            "size_bytes": 5,
            "storage_checksum": {"algorithm": "sha256", "value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
            "checksum_type": "full_object"
        }"#;
        assert!(serde_json::from_str::<ContentRef>(json).is_err());
    }

    #[test]
    fn a_produced_reference_carries_a_trusted_whole_file_sha256() {
        let content_ref = ContentRef::blob_v1(content_id(), b"hello");

        assert_eq!(content_ref.kind, ContentRefKind::BlobV1);
        assert_eq!(content_ref.size_bytes, 5);
        assert_eq!(
            content_ref.storage_checksum.algorithm,
            ChecksumAlgorithm::Sha256
        );
        assert_eq!(
            content_ref.whole_file_sha256.as_deref(),
            Some(content_ref.storage_checksum.value.as_str())
        );
        content_ref.validate().expect("produced refs validate");
    }

    #[test]
    fn validation_rejects_unsupported_kinds_and_malformed_checksums() {
        let mut content_ref = ContentRef::blob_v1(content_id(), b"hello");
        content_ref.kind = ContentRefKind::Unsupported("sparse_file_v9".to_owned());
        assert!(matches!(
            content_ref.validate(),
            Err(ContentRefValidationError::UnsupportedKind { .. })
        ));

        let mut content_ref = ContentRef::blob_v1(content_id(), b"hello");
        content_ref.storage_checksum = StorageChecksum {
            algorithm: ChecksumAlgorithm::Crc64nvme,
            value: content_ref.storage_checksum.value.clone(),
        };
        assert!(matches!(
            content_ref.validate(),
            Err(ContentRefValidationError::InvalidChecksum { .. })
        ));

        let mut content_ref = ContentRef::blob_v1(content_id(), b"hello");
        content_ref.whole_file_sha256 = Some(content_ref.storage_checksum.value.to_uppercase());
        assert!(matches!(
            content_ref.validate(),
            Err(ContentRefValidationError::InvalidChecksum { .. })
        ));
    }

    #[test]
    fn a_crc_only_reference_round_trips_without_a_whole_file_sha256() {
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: content_id(),
            size_bytes: 11_534_336,
            storage_checksum: StorageChecksum {
                algorithm: ChecksumAlgorithm::Crc64nvme,
                value: "bbb7305bdf118bcb".to_owned(),
            },
            whole_file_sha256: None,
        };
        content_ref.validate().expect("crc-only refs are valid");

        let encoded = serde_json::to_string(&content_ref).expect("encode");
        assert!(!encoded.contains("whole_file_sha256"));
        let decoded: ContentRef = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, content_ref);
    }
}
