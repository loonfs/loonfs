//! Immutable content references and their checksums.

use crate::hex::{hex_encode_bytes, is_lower_hex_byte, HexDecodeError};
use crate::ids::{string_id, validation_error, ContentId, NamespaceId};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// A supported kind of content reference, serialized as a string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ContentRefKind {
    /// The reference covers a prefix of one chain, which is addressed by its random content ID.
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
    /// The CRC-64/NVME algorithm.
    Crc64nvme,
    /// The CRC-32C algorithm.
    Crc32c,
}

impl ChecksumAlgorithm {
    /// Returns the frozen wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crc64nvme => "crc64nvme",
            Self::Crc32c => "crc32c",
        }
    }

    /// Returns the raw checksum width in bytes.
    pub fn value_bytes(self) -> usize {
        match self {
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

validation_error!(
    ChecksumHexValidationError,
    "invalid checksum {value:?}: {reason}"
);

string_id! {
    /// A checksum's bytes spelled in lowercase hexadecimal, two characters per
    /// byte.
    ChecksumHex,
    error = ChecksumHexValidationError,
    validate = validate_checksum_hex,
}

impl ChecksumHex {
    /// Spells checksum bytes in lowercase hexadecimal.
    pub fn encode(bytes: &[u8]) -> Self {
        Self(hex_encode_bytes(bytes))
    }
}

/// Checks the lowercase hexadecimal spelling without decoding it, so the check
/// allocates nothing.
fn validate_checksum_hex(value: &str) -> Result<(), ChecksumHexValidationError> {
    let error = if !value.len().is_multiple_of(2) {
        HexDecodeError::OddLength {
            length: value.len(),
        }
    } else if let Some(byte) = value.bytes().find(|&byte| !is_lower_hex_byte(byte)) {
        HexDecodeError::InvalidByte { byte }
    } else {
        return Ok(());
    };
    Err(ChecksumHexValidationError::new(value, error.to_string()))
}

/// A checksum algorithm and the checksum's canonical lowercase hexadecimal value.
// This type also appears in request bodies, so it rejects unknown fields in
// every context. Add new algorithms instead of new fields. This is not
// rustdoc because it describes storage behavior, not the public API.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Checksum {
    /// The algorithm that produced `value`.
    pub algorithm: ChecksumAlgorithm,
    /// The checksum in canonical lowercase hexadecimal, without a prefix.
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub value: ChecksumHex,
}

impl Checksum {
    /// Builds the `algorithm` checksum for these complete bytes.
    ///
    /// The one-shot forms below are this with the algorithm spelled out, so
    /// a payload held whole and one delivered in pieces cannot drift: both
    /// close the same digest.
    pub fn compute(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Self {
        let mut digest = ChecksumDigest::new(algorithm);
        digest.update(bytes);
        digest.finish()
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

    /// Combines equal CRC algorithms without reading either payload.
    pub fn crc_combine(&self, next: &Checksum, next_len: u64) -> Option<Checksum> {
        if self.algorithm != next.algorithm || self.validate().is_err() || next.validate().is_err()
        {
            return None;
        }
        let combined = crc_fast::checksum_combine(
            crc_fast_algorithm(self.algorithm),
            u64::from_str_radix(self.value.as_str(), 16).ok()?,
            u64::from_str_radix(next.value.as_str(), 16).ok()?,
            next_len,
        );
        let bytes = combined.to_be_bytes();
        Some(Checksum {
            algorithm: self.algorithm,
            value: ChecksumHex::encode(&bytes[8 - self.algorithm.value_bytes()..]),
        })
    }

    /// Validates the exact width for `algorithm`.
    pub fn validate(&self) -> Result<(), ChecksumValidationError> {
        let expected_len = self.algorithm.value_bytes() * 2;
        let actual_len = self.value.as_str().len();
        if actual_len != expected_len {
            return Err(ChecksumValidationError::InvalidWidth {
                algorithm: self.algorithm,
                expected_len,
                actual_len,
            });
        }
        Ok(())
    }
}

/// Describes why a checksum is not in its canonical wire form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[non_exhaustive]
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
}

/// An incremental checksum for streamed reads and writes.
pub struct ChecksumDigest {
    algorithm: ChecksumAlgorithm,
    // Boxed because a `crc_fast::Digest` is about 300 bytes, and every enum
    // that holds a digest would otherwise be that large.
    digest: Box<crc_fast::Digest>,
}

impl ChecksumDigest {
    /// Starts an empty digest for `algorithm`.
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            digest: Box::new(crc_fast::Digest::new(crc_fast_algorithm(algorithm))),
        }
    }

    /// Folds the next piece of the payload in, in order.
    pub fn update(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    /// Closes the digest over everything fed so far.
    ///
    /// The value is the big-endian spelling of the 64-bit CRC-64/NVME or the
    /// 32-bit CRC-32C result
    /// ([Appendix A.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a3-content-references-and-checksums)).
    pub fn finish(self) -> Checksum {
        let result = self.digest.finalize();
        let value = match self.algorithm {
            ChecksumAlgorithm::Crc64nvme => ChecksumHex::encode(&result.to_be_bytes()),
            ChecksumAlgorithm::Crc32c => ChecksumHex::encode(&crc32_from_u64(result).to_be_bytes()),
        };
        Checksum {
            algorithm: self.algorithm,
            value,
        }
    }
}

impl fmt::Debug for ChecksumDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChecksumDigest")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

/// Names the `crc-fast` algorithm that computes `algorithm`.
fn crc_fast_algorithm(algorithm: ChecksumAlgorithm) -> crc_fast::CrcAlgorithm {
    match algorithm {
        ChecksumAlgorithm::Crc64nvme => crc_fast::CrcAlgorithm::Crc64Nvme,
        ChecksumAlgorithm::Crc32c => crc_fast::CrcAlgorithm::Crc32Iscsi,
    }
}

/// A streamed read's check against its content reference.
///
/// A read that starts at a nonzero offset first folds in the bytes below that
/// offset, which the caller already holds, so the checksum covers the whole
/// content. Each fetched chunk is folded in after that. At the end,
/// [`Self::finish`] compares the byte count and the checksum with the
/// reference, and every later call returns the same result.
#[derive(Debug)]
pub struct ContentVerifier {
    expected_bytes: u64,
    expected: Checksum,
    start_offset: u64,
    prefix_bytes: u64,
    fetched_bytes: u64,
    status: ContentVerificationStatus,
}

#[derive(Debug)]
enum ContentVerificationStatus {
    Streaming(ChecksumDigest),
    Verified,
    Failed(ContentMismatchError),
}

impl ContentVerifier {
    /// Starts checking a read of `content_ref` that fetches from `start_offset`.
    pub fn new(content_ref: &ContentRef, start_offset: u64) -> Self {
        Self {
            expected_bytes: content_ref.size_bytes,
            expected: content_ref.checksum.clone(),
            start_offset,
            prefix_bytes: 0,
            fetched_bytes: 0,
            status: ContentVerificationStatus::Streaming(ChecksumDigest::new(
                content_ref.checksum.algorithm,
            )),
        }
    }

    /// Folds in held bytes from below the start offset, in order.
    ///
    /// A prefix that runs past the start offset, or that arrives after the
    /// first fetched chunk or after [`Self::finish`], is refused, and none of
    /// it is folded.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) -> Result<(), ResumedPrefixError> {
        if self.fetched_bytes > 0 {
            return Err(ResumedPrefixError::AfterFetch);
        }
        let prefix_bytes = self.prefix_bytes.saturating_add(bytes.len() as u64);
        let start_offset = self.start_offset;
        let Some(digest) = self.digest() else {
            return Err(ResumedPrefixError::AfterEnd);
        };
        if prefix_bytes > start_offset {
            return Err(ResumedPrefixError::PastStartOffset {
                prefix_bytes,
                start_offset,
            });
        }
        digest.update(bytes);
        self.prefix_bytes = prefix_bytes;
        Ok(())
    }

    /// Returns an error until every byte below the start offset is folded in.
    pub fn ensure_resumed_prefix_complete(&self) -> Result<(), ResumedPrefixIncompleteError> {
        if self.prefix_bytes == self.start_offset {
            return Ok(());
        }
        Err(ResumedPrefixIncompleteError {
            start_offset: self.start_offset,
            prefix_bytes: self.prefix_bytes,
        })
    }

    /// Folds in the next fetched chunk, in order.
    ///
    /// A chunk that arrives after [`Self::finish`] is refused.
    pub fn update(&mut self, chunk: &[u8]) -> Result<(), ChunkAfterEndError> {
        self.digest().ok_or(ChunkAfterEndError)?.update(chunk);
        self.fetched_bytes += chunk.len() as u64;
        Ok(())
    }

    /// Compares everything folded in with the reference the first time it's
    /// called, and returns the same result on every later call.
    pub fn finish(&mut self) -> Result<(), ContentMismatchError> {
        let verdict = match std::mem::replace(&mut self.status, ContentVerificationStatus::Verified)
        {
            ContentVerificationStatus::Streaming(digest) => self.compare(digest.finish()),
            ContentVerificationStatus::Verified => Ok(()),
            ContentVerificationStatus::Failed(mismatch) => Err(mismatch),
        };
        self.status = match &verdict {
            Ok(()) => ContentVerificationStatus::Verified,
            Err(mismatch) => ContentVerificationStatus::Failed(mismatch.clone()),
        };
        verdict
    }

    fn digest(&mut self) -> Option<&mut ChecksumDigest> {
        match &mut self.status {
            ContentVerificationStatus::Streaming(digest) => Some(digest),
            ContentVerificationStatus::Verified | ContentVerificationStatus::Failed(_) => None,
        }
    }

    fn compare(&self, actual: Checksum) -> Result<(), ContentMismatchError> {
        let actual_bytes = self.prefix_bytes + self.fetched_bytes;
        if actual_bytes == self.expected_bytes && actual == self.expected {
            return Ok(());
        }
        Err(ContentMismatchError {
            expected_bytes: self.expected_bytes,
            actual_bytes,
            expected: self.expected.clone(),
            actual,
        })
    }
}

/// Describes streamed content that doesn't match its reference.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "content of {actual_bytes} bytes with checksum `{}:{}` does not match the {expected_bytes} \
     bytes with checksum `{}:{}` that its reference names",
    .actual.algorithm, .actual.value, .expected.algorithm, .expected.value
)]
pub struct ContentMismatchError {
    /// Byte length the reference names.
    pub expected_bytes: u64,
    /// Bytes folded in: the resumed prefix and every fetched chunk.
    pub actual_bytes: u64,
    /// Checksum the reference names.
    pub expected: Checksum,
    /// Checksum of the bytes folded in.
    pub actual: Checksum,
}

/// Why a resumed prefix was refused.
///
/// None of the refused bytes are folded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ResumedPrefixError {
    /// The prefix would run past the offset the read resumes at.
    #[error("a resumed prefix of {prefix_bytes} bytes runs past the resume offset {start_offset}")]
    PastStartOffset {
        /// Prefix length the refused bytes would have reached.
        prefix_bytes: u64,
        /// Offset the read fetches from.
        start_offset: u64,
    },
    /// The prefix arrived after the read fetched content.
    #[error("a resumed prefix must arrive before the read fetches content")]
    AfterFetch,
    /// The prefix arrived after the read reached its end.
    #[error("a resumed prefix must arrive before the read reaches its end")]
    AfterEnd,
}

/// Describes a fetched chunk that arrived after the read reached its end.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("a fetched chunk arrived after the read reached its end")]
pub struct ChunkAfterEndError;

/// Describes a resumed read asked for content before its whole prefix was
/// folded in.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "a read resumed at offset {start_offset} was given {prefix_bytes} bytes of what it skipped; \
     verification covers the whole content, so all of them are needed first"
)]
pub struct ResumedPrefixIncompleteError {
    /// Offset the read fetches from.
    pub start_offset: u64,
    /// Prefix bytes folded in so far.
    pub prefix_bytes: u64,
}

/// Computes the CRC-32C of `bytes`.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32_from_u64(crc_fast::checksum(
        crc_fast::CrcAlgorithm::Crc32Iscsi,
        bytes,
    ))
}

/// Narrows a CRC-32 value, which `crc-fast` returns as a `u64`.
fn crc32_from_u64(value: u64) -> u32 {
    u32::try_from(value).expect("a CRC-32 value should fit in 32 bits")
}

/// Describes why a content reference cannot be part of a durable commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[non_exhaustive]
pub enum ContentRefValidationError {
    /// The checksum is not in the algorithm's canonical form.
    #[error("invalid content ref checksum: {0}")]
    InvalidChecksum(#[from] ChecksumValidationError),
}

/// A reference to a prefix of one content chain, by its owner, content ID, size, and checksum.
// Request bodies and durable records share this type, so it rejects unknown
// fields in every context. After release, new content kinds, not new fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ContentRef {
    /// The kind of content reference. The format defines one kind, `blob_v1`.
    pub kind: ContentRefKind,
    /// The namespace that originally wrote the bytes.
    pub owner_namespace_id: NamespaceId,
    /// The immutable identity of the content. The content ID and the owner together determine the
    /// object key.
    pub content_id: ContentId,
    /// The length of the referenced bytes, which are a prefix of the content object.
    pub size_bytes: u64,
    /// The required checksum of the referenced bytes.
    pub checksum: Checksum,
}

impl ContentRef {
    /// Builds a reference to a freshly minted content object holding these bytes.
    ///
    /// Every caller of this constructor moves the bytes through the LoonFS
    /// write path, so the checksum is trusted by construction.
    pub fn blob_v1(
        owner_namespace_id: NamespaceId,
        content_id: ContentId,
        bytes: &[u8],
        algorithm: ChecksumAlgorithm,
    ) -> Self {
        Self {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id,
            content_id,
            size_bytes: bytes.len() as u64,
            checksum: Checksum::compute(algorithm, bytes),
        }
    }

    /// Reports whether the reference is well formed enough to publish.
    ///
    /// This is a shape check on the reference itself; proving that the
    /// object exists and matches is the storage layer's job.
    pub fn validate(&self) -> Result<(), ContentRefValidationError> {
        self.checksum.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Checksum, ChecksumAlgorithm, ChecksumDigest, ChecksumHex, ChecksumValidationError,
        ContentRef, ContentRefKind, ContentRefValidationError,
    };
    use crate::ids::ContentId;
    use std::assert_matches;

    fn content_id() -> ContentId {
        ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("valid content id")
    }

    #[test]
    fn combining_provider_crcs_matches_hashing_the_joined_bytes() {
        for algorithm in [ChecksumAlgorithm::Crc32c, ChecksumAlgorithm::Crc64nvme] {
            let first = vec![42; 1001];
            let second = vec![71; 8193];
            let actual = Checksum::compute(algorithm, &first)
                .crc_combine(&Checksum::compute(algorithm, &second), second.len() as u64);
            assert_eq!(
                actual,
                Some(Checksum::compute(algorithm, &[first, second].concat()))
            );
        }
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
    fn an_unknown_checksum_algorithm_fails_to_decode() {
        serde_json::from_str::<ChecksumAlgorithm>("\"md5\"").expect_err("`md5` should be rejected");
        serde_json::from_str::<ChecksumAlgorithm>("\"sha256\"")
            .expect_err("`sha256` should be rejected");

        let json = r#"{
            "kind": "blob_v1",
            "owner_namespace_id": "demo",
            "content_id": "con_0123456789abcdef0123456789abcdef",
            "size_bytes": 5,
            "checksum": {"algorithm": "md5", "value": "00000000000000000000000000000000"}
        }"#;
        serde_json::from_str::<ContentRef>(json)
            .expect_err("a content ref with an unknown algorithm should be rejected");
    }

    #[test]
    fn a_content_ref_requires_an_owner_and_one_checksum() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            content_id(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );

        assert_eq!(content_ref.kind, ContentRefKind::BlobV1);
        assert_eq!(content_ref.size_bytes, 5);
        assert_eq!(content_ref.checksum.algorithm, ChecksumAlgorithm::Crc64nvme);
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
        serde_json::from_value::<ContentRef>(missing_owner)
            .expect_err("a content ref without an owner should be rejected");
        assert!(object.contains_key("checksum"));
    }

    #[test]
    fn validation_rejects_a_malformed_checksum() {
        let mut content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            content_id(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );
        content_ref.checksum = Checksum {
            algorithm: ChecksumAlgorithm::Crc64nvme,
            value: ChecksumHex::parse("0".repeat(64)).expect("a lowercase hex checksum"),
        };
        assert_matches!(
            content_ref.validate(),
            Err(ContentRefValidationError::InvalidChecksum(
                ChecksumValidationError::InvalidWidth { .. }
            ))
        );
    }

    #[test]
    fn checksum_validation_enforces_exact_widths() {
        for (algorithm, width) in [
            (ChecksumAlgorithm::Crc64nvme, 16),
            (ChecksumAlgorithm::Crc32c, 8),
        ] {
            let checksum = |len: usize| Checksum {
                algorithm,
                value: ChecksumHex::parse("a".repeat(len)).expect("a lowercase hex checksum"),
            };
            checksum(width).validate().expect("exact width");
            assert_matches!(
                checksum(width - 2).validate(),
                Err(ChecksumValidationError::InvalidWidth { .. })
            );
            assert_matches!(
                checksum(width + 2).validate(),
                Err(ChecksumValidationError::InvalidWidth { .. })
            );
        }
    }

    #[test]
    fn a_checksum_value_outside_lowercase_hex_pairs_fails_to_decode() {
        for value in ["A".repeat(16), "a".repeat(15), "g".repeat(16)] {
            let document = serde_json::json!({ "algorithm": "crc64nvme", "value": value });
            let error = serde_json::from_value::<Checksum>(document).expect_err("not hex pairs");
            assert!(error.to_string().contains("invalid checksum"), "{error}");
        }
    }

    #[test]
    fn crc64nvme_matches_its_catalog_check_value() {
        assert_eq!(
            Checksum::crc64nvme(b"123456789").value.as_str(),
            "ae8b14860a799888"
        );
        assert_eq!(
            Checksum::crc64nvme(b"").value.as_str(),
            "0000000000000000",
            "the empty payload is the identity"
        );
    }

    #[test]
    fn crc32c_matches_its_catalog_check_value() {
        assert_eq!(Checksum::crc32c(b"123456789").value.as_str(), "e3069283");
        assert_eq!(
            Checksum::crc32c(b"").value.as_str(),
            "00000000",
            "the empty payload is the identity"
        );
    }

    #[test]
    fn a_streamed_checksum_agrees_with_the_whole_payload_at_once() {
        let payload: Vec<u8> = (0..=u8::MAX).cycle().take(4096).collect();
        for expected in [Checksum::crc64nvme(&payload), Checksum::crc32c(&payload)] {
            let mut streaming = ChecksumDigest::new(expected.algorithm);
            for chunk in payload.chunks(97) {
                streaming.update(chunk);
            }
            assert_eq!(streaming.finish(), expected);
        }
    }

    #[test]
    fn every_algorithm_compares_bytes_against_the_checksum_they_produce() {
        for algorithm in [ChecksumAlgorithm::Crc64nvme, ChecksumAlgorithm::Crc32c] {
            let expected = Checksum::compute(algorithm, b"hello");
            assert_eq!(expected.algorithm, algorithm);
            assert!(expected.matches(b"hello"));
            assert!(!expected.matches(b"other"));
        }
    }
}
