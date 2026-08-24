//! [`ContentRef`]: the durable reference a file revision points at, naming
//! one immutable content object and carrying the integrity evidence for it.

use crate::hex::hex_encode_bytes;
use crate::ids::ContentId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256 as Sha2Sha256};
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

/// Supported checksum algorithms.
///
/// The enclosing value defines which bytes a checksum covers. Unknown
/// algorithms fail to decode because every in-memory `ChecksumAlgorithm` must
/// be recomputable by this build. Adding a variant also requires adding its
/// implementation.
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

/// An algorithm and its canonical lowercase-hex checksum value.
///
/// The enclosing value defines which bytes the checksum covers.
// This type also appears in request bodies, so it rejects unknown fields in
// every context. Add new algorithms instead of new fields. This is not
// rustdoc because it describes storage behavior, not the public API.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Checksum {
    /// Algorithm that produced `value`.
    pub algorithm: ChecksumAlgorithm,
    /// Lowercase hex of the raw checksum bytes.
    ///
    /// The algorithm is its own field, so the value carries no prefix.
    /// Provider APIs that report base64 are converted at the adapter.
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
        if !self
            .value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
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

/// Incremental checksum for streamed reads and writes.
///
/// It supports every [`ChecksumAlgorithm`] used by buffered verification, so
/// buffered and streaming paths differ only in how bytes are supplied.
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

/// CRC-64/NVME over a payload delivered in pieces.
///
/// A direct multipart upload needs this digest twice over the same bytes:
/// once per part, for the header the provider enforces on the way in, and
/// once over the whole stream, for the reference completion verifies. Parts
/// fed in order produce both without the object ever being held whole.
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

/// Incremental CRC-32C (Castagnoli) checksum.
///
/// Google Cloud Storage reports this checksum for direct transfers. Resumed
/// reads first add the retained prefix and then the fetched remainder, which
/// produces the same full-object checksum as an uninterrupted read.
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

/// SHA-256 over a payload delivered in pieces.
///
/// The proxied write path folds this over the request body as it forwards
/// it to object storage, so a reference's full-object checksum exists without
/// the payload ever being held whole. Pieces must be fed in order.
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
    /// The reference names a content strategy this build cannot write.
    #[error("unsupported content ref kind `{kind}`")]
    UnsupportedKind {
        /// Kind spelling carried by the rejected reference.
        kind: String,
    },
    /// The checksum is not in the algorithm's canonical form.
    #[error("invalid content ref checksum: {0}")]
    InvalidChecksum(ChecksumValidationError),
}

/// Pointer to one immutable content object.
///
/// `content_id` is identity — *which* object — and the checksum is
/// evidence about its bytes. Separating the two is what lets the final
/// object key exist before the first byte is read.
///
/// A `ContentRef` is safe to publish only after the referenced bytes are
/// durable in the namespace's content store.
// This type also appears in request bodies, so it rejects unknown fields in
// every context. Add new content kinds instead of new fields. This is not
// rustdoc because it describes storage behavior, not the public API.
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
    /// Mandatory checksum over the complete object.
    pub checksum: Checksum,
}

/// Available proof that a payload matches a committed content reference.
#[derive(Debug, Clone, Copy)]
pub enum ContentEvidence<'a> {
    /// Bytes that can be hashed with the committed reference's algorithm.
    Bytes(&'a [u8]),
    /// A reference carrying checksum evidence about its payload.
    ContentRef(&'a ContentRef),
}

impl ContentRef {
    /// Builds a reference to a freshly minted content object holding these bytes.
    ///
    /// Every caller of this constructor moves the bytes through the LoonFS
    /// write path, so the checksum is trusted by construction.
    pub fn blob_v1(content_id: ContentId, bytes: &[u8]) -> Self {
        Self {
            kind: ContentRefKind::BlobV1,
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
    pub fn blob_v1_streamed(content_id: ContentId, size_bytes: u64, digest: Sha256) -> Self {
        Self {
            kind: ContentRefKind::BlobV1,
            content_id,
            size_bytes,
            checksum: digest.finish(),
        }
    }

    /// Whether `evidence` proves that a payload has the same bytes as this
    /// reference.
    ///
    /// Reference evidence returns `false` when the size or checksum algorithm
    /// differs. A checksum that was never computed is not evidence of a match.
    pub fn matches_evidence(&self, evidence: ContentEvidence<'_>) -> bool {
        match evidence {
            ContentEvidence::Bytes(bytes) => {
                self.size_bytes == bytes.len() as u64 && self.checksum.matches(bytes)
            }
            ContentEvidence::ContentRef(reference) => {
                self.size_bytes == reference.size_bytes && self.checksum == reference.checksum
            }
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
        self.checksum
            .validate()
            .map_err(ContentRefValidationError::InvalidChecksum)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Checksum, ChecksumAlgorithm, ChecksumValidationError, ContentEvidence, ContentRef,
        ContentRefKind, ContentRefValidationError, StreamingChecksum,
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
            "content_id": "con_0123456789abcdef0123456789abcdef",
            "size_bytes": 5,
            "checksum": {"algorithm": "md5", "value": "00000000000000000000000000000000"}
        }"#;
        assert!(serde_json::from_str::<ContentRef>(json).is_err());
    }

    #[test]
    fn a_content_ref_uses_only_the_checksum_shape() {
        let content_ref = ContentRef::blob_v1(content_id(), b"hello");

        assert_eq!(content_ref.kind, ContentRefKind::BlobV1);
        assert_eq!(content_ref.size_bytes, 5);
        assert_eq!(content_ref.checksum.algorithm, ChecksumAlgorithm::Sha256);
        content_ref.validate().expect("produced refs validate");

        let document = serde_json::to_value(&content_ref).expect("encode content ref");
        let object = document.as_object().expect("content ref object");
        assert_eq!(object.len(), 4);
        assert!(object.contains_key("checksum"));
        assert!(!object.contains_key("storage_checksum"));
        assert!(!object.contains_key("whole_file_sha256"));
    }

    #[test]
    fn validation_rejects_an_unsupported_kind_and_a_malformed_checksum() {
        let mut content_ref = ContentRef::blob_v1(content_id(), b"hello");
        content_ref.kind = ContentRefKind::Unsupported("sparse_file_v9".to_owned());
        assert!(matches!(
            content_ref.validate(),
            Err(ContentRefValidationError::UnsupportedKind { .. })
        ));

        let mut content_ref = ContentRef::blob_v1(content_id(), b"hello");
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

    #[test]
    fn a_reference_compares_bytes_using_its_checksum_and_size() {
        let bytes = b"retried payload";
        let reference = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: content_id(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc32c(bytes),
        };

        assert!(reference.matches_evidence(ContentEvidence::Bytes(bytes)));
        assert!(!reference.matches_evidence(ContentEvidence::Bytes(b"different payload")));
        let mut wrong_size = reference.clone();
        wrong_size.size_bytes += 1;
        assert!(!wrong_size.matches_evidence(ContentEvidence::Bytes(bytes)));
    }

    #[test]
    fn a_reference_requires_the_other_reference_to_carry_its_checksum_algorithm() {
        let bytes = b"retried payload";
        let crc_reference = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: content_id(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc32c(bytes),
        };
        let sha_reference = ContentRef::blob_v1(content_id(), bytes);
        let matching_crc_reference = ContentRef {
            content_id: content_id(),
            ..crc_reference.clone()
        };
        let different_size = ContentRef {
            size_bytes: crc_reference.size_bytes + 1,
            ..crc_reference.clone()
        };

        assert!(!crc_reference.matches_evidence(ContentEvidence::ContentRef(&sha_reference)));
        assert!(
            crc_reference.matches_evidence(ContentEvidence::ContentRef(&matching_crc_reference))
        );
        assert!(!crc_reference.matches_evidence(ContentEvidence::ContentRef(&different_size)));
        assert!(sha_reference.matches_evidence(ContentEvidence::ContentRef(&sha_reference)));
    }
}
