//! Immutable content references and their checksums.

use crate::hex::{hex_encode_bytes, is_lower_hex_byte};
use crate::ids::{ContentId, NamespaceId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256 as Sha2Sha256};
use std::fmt;
use thiserror::Error;

/// A supported content reference kind serialized as a string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ContentRefKind {
    /// One immutable content object, addressed by its random content id.
    BlobV1,
}

impl ContentRefKind {
    /// Returns the frozen wire spelling.
    pub fn as_str(&self) -> &str {
        match self {
            Self::BlobV1 => "blob_v1",
        }
    }
}

impl fmt::Display for ContentRefKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A supported checksum algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ChecksumAlgorithm {
    /// SHA-256.
    Sha256,
    /// CRC-64/NVME.
    Crc64nvme,
    /// CRC-32C.
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

/// A checksum algorithm and its canonical lowercase hexadecimal value.
// This type also appears in request bodies, so it rejects unknown fields in
// every context. Add new algorithms instead of new fields. This is not
// rustdoc because it describes storage behavior, not the public API.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Checksum {
    /// Algorithm that produced `value`.
    pub algorithm: ChecksumAlgorithm,
    /// The canonical lowercase hexadecimal checksum without a prefix.
    pub value: String,
}

impl Checksum {
    /// Builds the `algorithm` checksum for these complete bytes.
    ///
    /// The one-shot forms below are this with the algorithm spelled out, so
    /// a payload held whole and one delivered in pieces cannot drift: both
    /// close the same digest.
    pub fn compute(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Self {
        let mut digest = StreamingChecksum::for_algorithm(algorithm);
        digest.update(bytes);
        digest.finish()
    }

    /// Builds the SHA-256 checksum for these bytes.
    pub fn sha256(bytes: &[u8]) -> Self {
        Self::compute(ChecksumAlgorithm::Sha256, bytes)
    }

    /// Builds the CRC-64/NVME checksum for these bytes.
    pub fn crc64nvme(bytes: &[u8]) -> Self {
        Self::compute(ChecksumAlgorithm::Crc64nvme, bytes)
    }

    /// Builds the CRC-32C checksum for these bytes.
    pub fn crc32c(bytes: &[u8]) -> Self {
        Self::compute(ChecksumAlgorithm::Crc32c, bytes)
    }

    /// Reports whether these bytes produce this exact checksum.
    pub fn matches(&self, bytes: &[u8]) -> bool {
        Self::compute(self.algorithm, bytes).value == self.value
    }

    /// Validates the exact width and lowercase-hex alphabet for `algorithm`.
    pub fn validate(&self) -> Result<(), ChecksumValidationError> {
        let expected_len = self.algorithm.value_bytes() * 2;
        if self.value.len() != expected_len {
            return Err(ChecksumValidationError::InvalidWidth {
                algorithm: self.algorithm,
                expected_len,
                actual_len: self.value.len(),
            });
        }
        if !self.value.bytes().all(is_lower_hex_byte) {
            return Err(ChecksumValidationError::InvalidAlphabet {
                algorithm: self.algorithm,
            });
        }
        Ok(())
    }
}

/// Describes why a checksum is not in its canonical wire form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ChecksumValidationError {
    /// The checksum value does not have the exact width for its algorithm.
    #[error(
        "checksum for algorithm `{algorithm}` must be {expected_len} hex characters, got {actual_len}"
    )]
    InvalidWidth {
        /// Algorithm whose checksum width was required.
        algorithm: ChecksumAlgorithm,
        /// Required number of hexadecimal characters.
        expected_len: usize,
        /// Number of characters supplied.
        actual_len: usize,
    },
    /// The checksum value contains a character outside lowercase hexadecimal.
    #[error("checksum for algorithm `{algorithm}` must be lowercase hex")]
    InvalidAlphabet {
        /// Algorithm whose checksum value was rejected.
        algorithm: ChecksumAlgorithm,
    },
}

/// An incremental checksum for streamed reads and writes.
#[derive(Debug)]
pub enum StreamingChecksum {
    /// SHA-256 folded over the payload.
    Sha256(Sha256),
    /// CRC-64/NVME folded over the payload.
    Crc64nvme(Crc64Nvme),
    /// CRC-32C folded over the payload.
    Crc32c(Crc32c),
}

impl StreamingChecksum {
    /// Starts an empty digest for `algorithm`.
    pub fn for_algorithm(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
            ChecksumAlgorithm::Crc64nvme => Self::Crc64nvme(Crc64Nvme::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(Crc32c::new()),
        }
    }

    /// Folds the next piece of the payload in, in order.
    pub fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha256(digest) => digest.update(bytes),
            Self::Crc64nvme(digest) => digest.update(bytes),
            Self::Crc32c(digest) => digest.update(bytes),
        }
    }

    /// Closes the digest over everything fed so far.
    pub fn finish(self) -> Checksum {
        match self {
            Self::Sha256(digest) => digest.finish(),
            Self::Crc64nvme(digest) => digest.finish(),
            Self::Crc32c(digest) => digest.finish(),
        }
    }
}

/// An incremental CRC-64/NVME checksum.
#[derive(Default)]
pub struct Crc64Nvme {
    digest: crc64fast_nvme::Digest,
}

impl Crc64Nvme {
    /// Starts an empty digest.
    pub fn new() -> Self {
        Self {
            digest: crc64fast_nvme::Digest::new(),
        }
    }

    /// Folds the next piece of the payload in, in order.
    pub fn update(&mut self, bytes: &[u8]) {
        self.digest.write(bytes);
    }

    /// Closes the digest over everything fed so far.
    ///
    /// The value is the big-endian spelling of the 64-bit result, which is
    /// what the raw checksum bytes are on the wire and therefore what the
    /// hex here has to be.
    pub fn finish(self) -> Checksum {
        Checksum {
            algorithm: ChecksumAlgorithm::Crc64nvme,
            value: hex_encode_bytes(&self.digest.sum64().to_be_bytes()),
        }
    }
}

impl fmt::Debug for Crc64Nvme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Crc64Nvme").finish_non_exhaustive()
    }
}

/// An incremental CRC-32C checksum.
#[derive(Default)]
pub struct Crc32c {
    crc: u32,
}

impl Crc32c {
    /// Starts an empty digest.
    pub fn new() -> Self {
        Self { crc: 0 }
    }

    /// Folds the next piece of the payload in, in order.
    pub fn update(&mut self, bytes: &[u8]) {
        self.crc = crc32c::crc32c_append(self.crc, bytes);
    }

    /// Closes the digest over everything fed so far.
    ///
    /// The value is the big-endian spelling of the 32-bit result, which is
    /// what the raw checksum bytes are on the wire and therefore what the
    /// hex here has to be.
    pub fn finish(self) -> Checksum {
        Checksum {
            algorithm: ChecksumAlgorithm::Crc32c,
            value: hex_encode_bytes(&self.crc.to_be_bytes()),
        }
    }
}

impl fmt::Debug for Crc32c {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Crc32c").finish_non_exhaustive()
    }
}

/// An incremental SHA-256 checksum.
#[derive(Default)]
pub struct Sha256 {
    digest: Sha2Sha256,
}

impl Sha256 {
    /// Starts an empty digest.
    pub fn new() -> Self {
        Self {
            digest: Sha2Sha256::new(),
        }
    }

    /// Folds the next piece of the payload in, in order.
    pub fn update(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    /// Closes the digest over everything fed so far.
    pub fn finish(self) -> Checksum {
        Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: hex_encode_bytes(&self.digest.finalize()),
        }
    }
}

impl fmt::Debug for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sha256").finish_non_exhaustive()
    }
}

/// Describes why a content reference cannot be part of a durable commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ContentRefValidationError {
    /// The checksum is not in the algorithm's canonical form.
    #[error("invalid content ref checksum: {0}")]
    InvalidChecksum(ChecksumValidationError),
}

/// A reference to one immutable content object.
///
/// The object must be durable before the reference is published.
// Request bodies and durable records share this type, so it rejects unknown
// fields in every context. After release, new content kinds, not new fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ContentRef {
    /// Content strategy used by the referenced object.
    pub kind: ContentRefKind,
    /// Namespace that originally wrote the bytes.
    pub owner_namespace_id: NamespaceId,
    /// Immutable identity of the referenced object.
    pub content_id: ContentId,
    /// Complete byte length of the referenced content.
    pub size_bytes: u64,
    /// Mandatory checksum over the complete object.
    pub checksum: Checksum,
}

impl ContentRef {
    /// Builds a reference to a freshly minted content object holding these bytes.
    ///
    /// Every caller of this constructor moves the bytes through the LoonFS
    /// write path, so the checksum is trusted by construction.
    pub fn blob_v1(owner_namespace_id: NamespaceId, content_id: ContentId, bytes: &[u8]) -> Self {
        Self {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id,
            content_id,
            size_bytes: bytes.len() as u64,
            checksum: Checksum::sha256(bytes),
        }
    }

    /// Builds a content reference from a SHA-256 computed while streaming the
    /// payload.
    ///
    /// Accepting the digest object, rather than an arbitrary checksum string,
    /// ensures that the checksum came from the LoonFS write path.
    pub fn blob_v1_streamed(
        owner_namespace_id: NamespaceId,
        content_id: ContentId,
        size_bytes: u64,
        digest: Sha256,
    ) -> Self {
        Self {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id,
            content_id,
            size_bytes,
            checksum: digest.finish(),
        }
    }

    /// Reports whether the reference is well formed enough to publish.
    ///
    /// This is a shape check on the reference itself; proving that the
    /// object exists and matches is the storage layer's job.
    pub fn validate(&self) -> Result<(), ContentRefValidationError> {
        self.checksum
            .validate()
            .map_err(ContentRefValidationError::InvalidChecksum)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Checksum, ChecksumAlgorithm, ChecksumValidationError, ContentRef, ContentRefKind,
        ContentRefValidationError, StreamingChecksum,
    };
    use crate::ids::ContentId;

    fn content_id() -> ContentId {
        ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("valid content id")
    }

    #[test]
    fn known_kind_round_trips_as_snake_case_string() {
        let encoded = serde_json::to_string(&ContentRefKind::BlobV1).expect("encode");
        assert_eq!(encoded, "\"blob_v1\"");
        let decoded: ContentRefKind = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, ContentRefKind::BlobV1);
    }

    #[test]
    fn unknown_kind_fails_to_decode() {
        let error = serde_json::from_str::<ContentRefKind>("\"sparse_file_v9\"")
            .expect_err("unknown content kind must be rejected");
        assert_eq!(
            error.to_string(),
            "unknown variant `sparse_file_v9`, expected `blob_v1` at line 1 column 16"
        );
    }

    #[test]
    fn every_checksum_algorithm_round_trips() {
        for (algorithm, wire) in [
            (ChecksumAlgorithm::Sha256, "sha256"),
            (ChecksumAlgorithm::Crc64nvme, "crc64nvme"),
            (ChecksumAlgorithm::Crc32c, "crc32c"),
        ] {
            let encoded = serde_json::to_string(&algorithm).expect("encode algorithm");
            assert_eq!(encoded, format!("\"{wire}\""));
            assert_eq!(
                algorithm.as_str(),
                wire,
                "the hand-written spelling must match the serde tag"
            );
            let decoded: ChecksumAlgorithm =
                serde_json::from_str(&encoded).expect("decode algorithm");
            assert_eq!(decoded, algorithm);
        }
    }

    #[test]
    fn an_unknown_checksum_algorithm_fails_to_decode() {
        assert!(serde_json::from_str::<ChecksumAlgorithm>("\"md5\"").is_err());

        let json = r#"{
            "kind": "blob_v1",
            "owner_namespace_id": "demo",
            "content_id": "con_0123456789abcdef0123456789abcdef",
            "size_bytes": 5,
            "checksum": {"algorithm": "md5", "value": "00000000000000000000000000000000"}
        }"#;
        assert!(serde_json::from_str::<ContentRef>(json).is_err());
    }

    #[test]
    fn a_content_ref_requires_an_owner_and_one_checksum() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            content_id(),
            b"hello",
        );

        assert_eq!(content_ref.kind, ContentRefKind::BlobV1);
        assert_eq!(content_ref.size_bytes, 5);
        assert_eq!(content_ref.checksum.algorithm, ChecksumAlgorithm::Sha256);
        content_ref.validate().expect("produced refs validate");

        let document = serde_json::to_value(&content_ref).expect("encode content ref");
        let object = document.as_object().expect("content ref object");
        assert_eq!(object.len(), 5);
        assert_eq!(object["owner_namespace_id"], "demo");
        let mut missing_owner = document.clone();
        missing_owner
            .as_object_mut()
            .expect("reference")
            .remove("owner_namespace_id");
        assert!(serde_json::from_value::<ContentRef>(missing_owner).is_err());
        assert!(object.contains_key("checksum"));
        assert!(!object.contains_key("storage_checksum"));
        assert!(!object.contains_key("whole_file_sha256"));
    }

    #[test]
    fn validation_rejects_a_malformed_checksum() {
        let mut content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            content_id(),
            b"hello",
        );
        content_ref.checksum = Checksum {
            algorithm: ChecksumAlgorithm::Crc64nvme,
            value: content_ref.checksum.value.clone(),
        };
        assert!(matches!(
            content_ref.validate(),
            Err(ContentRefValidationError::InvalidChecksum(
                ChecksumValidationError::InvalidWidth { .. }
            ))
        ));
    }

    #[test]
    fn checksum_validation_enforces_exact_widths_and_lowercase_hex() {
        for (algorithm, width) in [
            (ChecksumAlgorithm::Sha256, 64),
            (ChecksumAlgorithm::Crc64nvme, 16),
            (ChecksumAlgorithm::Crc32c, 8),
        ] {
            Checksum {
                algorithm,
                value: "a".repeat(width),
            }
            .validate()
            .expect("exact lowercase width");

            assert!(matches!(
                Checksum {
                    algorithm,
                    value: "a".repeat(width - 1),
                }
                .validate(),
                Err(ChecksumValidationError::InvalidWidth { .. })
            ));
            assert!(matches!(
                Checksum {
                    algorithm,
                    value: "a".repeat(width + 1),
                }
                .validate(),
                Err(ChecksumValidationError::InvalidWidth { .. })
            ));
            assert!(matches!(
                Checksum {
                    algorithm,
                    value: "A".repeat(width),
                }
                .validate(),
                Err(ChecksumValidationError::InvalidAlphabet { .. })
            ));
        }
    }

    #[test]
    fn crc64nvme_matches_its_catalog_check_value() {
        assert_eq!(Checksum::crc64nvme(b"123456789").value, "ae8b14860a799888");
        assert_eq!(
            Checksum::crc64nvme(b"").value,
            "0000000000000000",
            "the empty payload is the identity"
        );
    }

    #[test]
    fn crc32c_matches_its_catalog_check_value() {
        assert_eq!(Checksum::crc32c(b"123456789").value, "e3069283");
        assert_eq!(
            Checksum::crc32c(b"").value,
            "00000000",
            "the empty payload is the identity"
        );
    }

    #[test]
    fn a_streamed_checksum_agrees_with_the_whole_payload_at_once() {
        let payload: Vec<u8> = (0..4096u32).map(|byte| byte as u8).collect();
        for expected in [
            Checksum::sha256(&payload),
            Checksum::crc64nvme(&payload),
            Checksum::crc32c(&payload),
        ] {
            let mut streaming = StreamingChecksum::for_algorithm(expected.algorithm);
            for chunk in payload.chunks(97) {
                streaming.update(chunk);
            }
            assert_eq!(streaming.finish(), expected);
        }
    }

    #[test]
    fn every_algorithm_compares_bytes_against_the_checksum_they_produce() {
        for algorithm in [
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Crc64nvme,
            ChecksumAlgorithm::Crc32c,
        ] {
            let expected = Checksum::compute(algorithm, b"hello");
            assert_eq!(expected.algorithm, algorithm);
            assert!(expected.matches(b"hello"));
            assert!(!expected.matches(b"other"));
        }
    }
}
