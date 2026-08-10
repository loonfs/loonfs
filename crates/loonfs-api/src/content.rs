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

/// Supported algorithms for full-object checksums.
///
/// Full-object coverage is part of the LoonFS format, so no separate checksum
/// type is stored. Unknown algorithms fail to decode because every in-memory
/// `ChecksumAlgorithm` must be recomputable by this build. Adding a variant
/// also requires adding its implementation.
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

    /// Builds the SHA-256 storage checksum for these complete bytes.
    pub fn sha256(bytes: &[u8]) -> Self {
        Self::compute(ChecksumAlgorithm::Sha256, bytes)
    }

    /// Builds the CRC-64/NVME storage checksum for these complete bytes.
    pub fn crc64nvme(bytes: &[u8]) -> Self {
        Self::compute(ChecksumAlgorithm::Crc64nvme, bytes)
    }

    /// Builds the CRC-32C storage checksum for these complete bytes.
    pub fn crc32c(bytes: &[u8]) -> Self {
        Self::compute(ChecksumAlgorithm::Crc32c, bytes)
    }

    /// Reports whether these bytes produce this exact checksum.
    pub fn matches(&self, bytes: &[u8]) -> bool {
        Self::compute(self.algorithm, bytes).value == self.value
    }
}

/// Incremental full-object checksum for streamed reads and writes.
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
    pub fn finish(self) -> StorageChecksum {
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
    pub fn finish(self) -> StorageChecksum {
        StorageChecksum {
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
    pub fn finish(self) -> StorageChecksum {
        StorageChecksum {
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
/// it to object storage, so a reference's trusted whole-file digest exists
/// without the payload ever being held whole. Pieces must be fed in order.
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
    pub fn finish(self) -> StorageChecksum {
        StorageChecksum {
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

/// Available proof that a payload matches a committed content reference.
#[derive(Debug, Clone, Copy)]
pub enum ContentEvidence<'a> {
    /// Bytes that can be hashed with the committed reference's algorithm.
    Bytes(&'a [u8]),
    /// A reference that carries only the checksums computed while its payload
    /// was available.
    ContentRef(&'a ContentRef),
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

    /// Builds a content reference from a SHA-256 computed while streaming the
    /// payload.
    ///
    /// Accepting the digest object, rather than an arbitrary checksum string,
    /// ensures that `whole_file_sha256` came from the LoonFS write path.
    pub fn blob_v1_streamed(content_id: ContentId, size_bytes: u64, digest: Sha256) -> Self {
        let storage_checksum = digest.finish();
        Self {
            kind: ContentRefKind::BlobV1,
            content_id,
            size_bytes,
            whole_file_sha256: Some(storage_checksum.value.clone()),
            storage_checksum,
        }
    }

    /// Returns the checksum used to verify this content reference.
    ///
    /// A trusted whole-file SHA-256 is preferred when present. Otherwise the
    /// provider's full-object storage checksum is used. All verification paths
    /// call this method so they use the same evidence.
    pub fn verifiable_checksum(&self) -> StorageChecksum {
        match &self.whole_file_sha256 {
            Some(digest) => StorageChecksum {
                algorithm: ChecksumAlgorithm::Sha256,
                value: digest.clone(),
            },
            None => self.storage_checksum.clone(),
        }
    }

    /// The digest this reference carries under `algorithm`, when it carries
    /// one at all.
    ///
    /// A reference describes its object with the checksum whoever wrote it
    /// produced, plus a whole-file SHA-256 when the write path hashed the
    /// bytes itself. Asking for anything else answers `None`: a digest
    /// nobody computed over these bytes cannot be conjured from the ones
    /// that were, and a retry comparing against it must refuse rather than
    /// guess.
    pub fn digest_under(&self, algorithm: ChecksumAlgorithm) -> Option<&str> {
        if self.storage_checksum.algorithm == algorithm {
            return Some(&self.storage_checksum.value);
        }
        match algorithm {
            ChecksumAlgorithm::Sha256 => self.whole_file_sha256.as_deref(),
            ChecksumAlgorithm::Crc64nvme | ChecksumAlgorithm::Crc32c => None,
        }
    }

    /// Whether `evidence` proves that a payload has the same bytes as this
    /// reference.
    ///
    /// Reference evidence returns `false` when it does not carry this
    /// reference's verification algorithm. A checksum that was never
    /// computed is not evidence of a match.
    pub fn matches_evidence(&self, evidence: ContentEvidence<'_>) -> bool {
        let expected = self.verifiable_checksum();
        match evidence {
            ContentEvidence::Bytes(bytes) => expected.matches(bytes),
            ContentEvidence::ContentRef(reference) => {
                reference.digest_under(expected.algorithm) == Some(expected.value.as_str())
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
        ChecksumAlgorithm, ContentEvidence, ContentRef, ContentRefKind, ContentRefValidationError,
        Crc32c, Crc64Nvme, StorageChecksum, StreamingChecksum,
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

    /// Unknown checksum algorithms are rejected during decoding.
    ///
    /// Content kinds may be preserved by readers without interpretation, but a
    /// checksum is accepted only when this build can verify it. This guarantees
    /// that every decoded `ChecksumAlgorithm` is supported by buffered, streamed,
    /// and resumed verification.
    #[test]
    fn an_unknown_checksum_algorithm_fails_to_decode() {
        assert!(serde_json::from_str::<ChecksumAlgorithm>("\"md5\"").is_err());

        let json = r#"{
            "kind": "blob_v1",
            "content_id": "con_0123456789abcdef0123456789abcdef",
            "size_bytes": 5,
            "storage_checksum": {"algorithm": "md5", "value": "00000000000000000000000000000000"}
        }"#;
        assert!(serde_json::from_str::<ContentRef>(json).is_err());
    }

    #[test]
    fn a_content_ref_rejects_unknown_fields() {
        let json = r#"{
            "kind": "blob_v1",
            "content_id": "con_0123456789abcdef0123456789abcdef",
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

    /// The catalog check value for CRC-64/NVME. This is the one thing that
    /// has to agree with the provider bit for bit: a completion compares our
    /// value against the one S3 computed over the assembled object, so a
    /// wrong polynomial or byte order would fail every multipart upload.
    #[test]
    fn crc64nvme_matches_its_catalog_check_value() {
        assert_eq!(
            StorageChecksum::crc64nvme(b"123456789").value,
            "ae8b14860a799888"
        );
        assert_eq!(
            StorageChecksum::crc64nvme(b"").value,
            "0000000000000000",
            "the empty payload is the identity"
        );
    }

    /// The catalog check value for CRC-32C (Castagnoli), the one the RFC
    /// 3720 iSCSI CRC and Google Cloud Storage both mean by "crc32c". Like
    /// the CRC-64/NVME vector above it is an external anchor: it fixes the
    /// polynomial and the big-endian byte order of the canonical hex against
    /// a published value rather than against whatever this build happens to
    /// compute, so a GCS-minted reference and one produced here agree.
    #[test]
    fn crc32c_matches_its_catalog_check_value() {
        assert_eq!(StorageChecksum::crc32c(b"123456789").value, "e3069283");
        assert_eq!(
            StorageChecksum::crc32c(b"").value,
            "00000000",
            "the empty payload is the identity"
        );
    }

    /// The streaming form exists so parts can be hashed on the way past
    /// without the whole object ever being held, so it must agree with the
    /// one-shot form over the same bytes.
    #[test]
    fn a_streamed_crc64nvme_equals_the_whole_payload_at_once() {
        let payload: Vec<u8> = (0..4096u32).map(|byte| byte as u8).collect();
        let mut streamed = Crc64Nvme::new();
        for chunk in payload.chunks(97) {
            streamed.update(chunk);
        }

        assert_eq!(streamed.finish(), StorageChecksum::crc64nvme(&payload));
    }

    /// A resumed read folds a prefix it already holds and then the bytes it
    /// fetches, so a CRC has to close over the two halves exactly as it
    /// closes over the whole.
    #[test]
    fn a_crc32c_folded_over_a_prefix_and_the_rest_equals_the_whole_payload() {
        let payload: Vec<u8> = (0..4096u32).map(|byte| byte as u8).collect();
        let mut streamed = Crc32c::new();
        streamed.update(&payload[..1500]);
        streamed.update(&payload[1500..]);

        assert_eq!(streamed.finish(), StorageChecksum::crc32c(&payload));
    }

    /// A verifying reader folds the same checksum the one-shot check
    /// computes, for every algorithm the vocabulary has — a reader that
    /// disagreed with [`StorageChecksum::matches`] would accept or reject
    /// objects the buffered read would not.
    #[test]
    fn a_streamed_checksum_agrees_with_the_whole_payload_at_once() {
        let payload: Vec<u8> = (0..4096u32).map(|byte| byte as u8).collect();
        for expected in [
            StorageChecksum::sha256(&payload),
            StorageChecksum::crc64nvme(&payload),
            StorageChecksum::crc32c(&payload),
        ] {
            let mut streaming = StreamingChecksum::for_algorithm(expected.algorithm);
            for chunk in payload.chunks(97) {
                streaming.update(chunk);
            }
            assert_eq!(streaming.finish(), expected);
        }
    }

    /// Every algorithm in the vocabulary is comparable, so a checksum in
    /// hand is always a question with an answer.
    #[test]
    fn every_algorithm_compares_bytes_against_the_checksum_they_produce() {
        for algorithm in [
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Crc64nvme,
            ChecksumAlgorithm::Crc32c,
        ] {
            let expected = StorageChecksum::compute(algorithm, b"hello");
            assert_eq!(expected.algorithm, algorithm);
            assert!(expected.matches(b"hello"));
            assert!(!expected.matches(b"other"));
        }
    }

    /// The evidence a read holds an object to is one answer, whichever
    /// surface asks: the trusted whole-file digest when there is one, and
    /// the reference's own checksum when it is all there is.
    #[test]
    fn a_reference_names_the_whole_file_digest_as_its_evidence_when_it_has_one() {
        let hashed = ContentRef::blob_v1(content_id(), b"hello");
        assert_eq!(
            hashed.verifiable_checksum(),
            StorageChecksum::sha256(b"hello")
        );

        let crc_only = ContentRef {
            storage_checksum: StorageChecksum::crc32c(b"hello"),
            whole_file_sha256: None,
            ..hashed
        };
        assert_eq!(
            crc_only.verifiable_checksum(),
            StorageChecksum::crc32c(b"hello")
        );
    }

    #[test]
    fn a_reference_compares_bytes_using_its_verifiable_checksum() {
        let bytes = b"retried payload";
        let reference = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: content_id(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc32c(bytes),
            whole_file_sha256: None,
        };

        assert!(reference.matches_evidence(ContentEvidence::Bytes(bytes)));
        assert!(!reference.matches_evidence(ContentEvidence::Bytes(b"different payload")));
    }

    #[test]
    fn a_reference_requires_the_other_reference_to_carry_its_checksum_algorithm() {
        let bytes = b"retried payload";
        let crc_reference = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: content_id(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc32c(bytes),
            whole_file_sha256: None,
        };
        let sha_reference = ContentRef::blob_v1(content_id(), bytes);
        let matching_crc_reference = ContentRef {
            content_id: content_id(),
            ..crc_reference.clone()
        };

        assert!(!crc_reference.matches_evidence(ContentEvidence::ContentRef(&sha_reference)));
        assert!(
            crc_reference.matches_evidence(ContentEvidence::ContentRef(&matching_crc_reference))
        );
        assert!(sha_reference.matches_evidence(ContentEvidence::ContentRef(&sha_reference)));
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
