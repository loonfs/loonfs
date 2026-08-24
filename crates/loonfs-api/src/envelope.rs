//! Shared encoding and validation for durable envelopes.
//!
//! Durable objects declare `kind`, `format_version`, and `payload_checksum`
//! before their payload. Checksums cover the stored payload bytes rather than
//! a re-encoding. JSON control objects and CBOR WAL segments use the same
//! validation rules and errors.

use crate::digest::sha256_digest;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;

/// Identifying prefix of a durable envelope document.
///
/// Readers decode this probe before the full document so that objects written
/// with an unknown kind or an unsupported format version fail with a precise
/// error instead of a generic decode error. `kind` is deliberately a string
/// (not an enum) so future kinds remain reportable.
#[derive(Debug, Deserialize)]
pub struct EnvelopeProbe {
    /// Durable family declared by the stored object.
    pub kind: String,
    /// Family-independent version gate declared by the stored object.
    pub format_version: u32,
}

/// Failure vocabulary shared by every envelope codec. Messages are
/// envelope-generic; the wrapping error names the object (and its key) the
/// bytes came from.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EnvelopeCodecError {
    /// Reports a payload that cannot be serialized into its family's durable encoding.
    #[error("failed to encode envelope payload: {0}")]
    PayloadEncode(String),
    /// Reports an envelope document that cannot be serialized around an encoded payload.
    #[error("failed to encode envelope document: {0}")]
    EnvelopeEncode(String),
    /// Reports bytes that do not decode as the shared durable envelope layout.
    #[error("failed to decode envelope document: {0}")]
    EnvelopeDecode(String),
    /// Reports a verified envelope whose opaque payload does not decode as its declared family.
    #[error("failed to decode envelope payload: {0}")]
    PayloadDecode(String),
    /// Reports an envelope that the configured transport codec could not compress.
    #[error("failed to compress envelope: {0}")]
    Compress(String),
    /// Reports stored bytes that the configured transport codec could not decompress.
    #[error("failed to decompress envelope: {0}")]
    Decompress(String),
    /// Reports an unrecognized durable-family discriminator found during the envelope probe.
    #[error("unknown envelope kind `{found}`")]
    UnknownKind {
        /// Untrusted `kind` spelling decoded from the stored object.
        found: String,
    },
    /// Reports a valid discriminator that does not belong to the decoder the caller selected.
    #[error("envelope kind mismatch: expected `{expected}`, found `{found}`")]
    KindMismatch {
        /// Durable family the selected decoder accepts.
        expected: String,
        /// Durable family declared by the stored object.
        found: String,
    },
    /// Reports a known durable family whose format version this build cannot read.
    #[error(
        "unsupported `{kind}` envelope format version `{found}`: \
         this build supports `{supported}`"
    )]
    UnsupportedFormatVersion {
        /// Durable family whose independent version gate rejected the object.
        kind: String,
        /// Version declared by the stored object.
        found: u32,
        /// Sole version this build reads and writes for `kind`.
        supported: u32,
    },
    /// Reports stored payload bytes that do not match the checksum recorded beside them.
    #[error("envelope payload checksum mismatch: expected `{expected}`, actual `{actual}`")]
    ChecksumMismatch {
        /// Digest recorded in the durable envelope.
        expected: String,
        /// Digest recomputed over the exact stored payload bytes.
        actual: String,
    },
    /// Reports an in-memory payload changed without rebuilding its envelope checksum.
    #[error(
        "envelope checksum `{checksum}` does not match its payload `{actual}`: \
         rebuild the envelope from its payload"
    )]
    StalePayloadChecksum {
        /// Digest retained by the stale in-memory envelope.
        checksum: String,
        /// Digest recomputed from the payload about to be encoded.
        actual: String,
    },
}

/// Requires the probed kind to be exactly `expected`.
pub fn verify_kind(expected: &str, found: &str) -> Result<(), EnvelopeCodecError> {
    if found != expected {
        return Err(EnvelopeCodecError::KindMismatch {
            expected: expected.to_owned(),
            found: found.to_owned(),
        });
    }
    Ok(())
}

/// Requires the probed format version to be exactly what this build writes
/// for `kind` — no envelope family tolerates version skew.
pub fn verify_version(kind: &str, found: u32, supported: u32) -> Result<(), EnvelopeCodecError> {
    if found != supported {
        return Err(EnvelopeCodecError::UnsupportedFormatVersion {
            kind: kind.to_owned(),
            found,
            supported,
        });
    }
    Ok(())
}

/// Requires the stored checksum to match the payload bytes as stored.
pub fn verify_payload_checksum(
    expected: &str,
    payload_bytes: &[u8],
) -> Result<(), EnvelopeCodecError> {
    let actual = sha256_digest(payload_bytes);
    if actual != expected {
        return Err(EnvelopeCodecError::ChecksumMismatch {
            expected: expected.to_owned(),
            actual,
        });
    }
    Ok(())
}

/// Requires an in-memory envelope's recorded checksum to still match its
/// payload before encoding — a stale checksum means the caller mutated the
/// payload without rebuilding the envelope.
pub fn verify_checksum_fresh(
    checksum: &str,
    payload_bytes: &[u8],
) -> Result<(), EnvelopeCodecError> {
    let actual = sha256_digest(payload_bytes);
    if actual != checksum {
        return Err(EnvelopeCodecError::StalePayloadChecksum {
            checksum: checksum.to_owned(),
            actual,
        });
    }
    Ok(())
}

/// Durable layout of a JSON-bodied envelope: the shared fields plus the
/// payload as a raw JSON fragment, kept inline so the object remains
/// directly readable JSON while `payload_checksum` covers the exact
/// fragment bytes as stored.
#[derive(Serialize, Deserialize)]
struct JsonEnvelopeDocument {
    kind: String,
    format_version: u32,
    payload_checksum: String,
    payload: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictJsonEnvelopeDocument {
    kind: String,
    format_version: u32,
    payload_checksum: String,
    payload: Box<RawValue>,
}

impl From<StrictJsonEnvelopeDocument> for JsonEnvelopeDocument {
    fn from(document: StrictJsonEnvelopeDocument) -> Self {
        Self {
            kind: document.kind,
            format_version: document.format_version,
            payload_checksum: document.payload_checksum,
            payload: document.payload,
        }
    }
}

/// The `sha256:<hex>` checksum a JSON payload will carry, computed over its
/// canonical serialization.
pub fn json_payload_checksum<T: Serialize>(payload: &T) -> Result<String, EnvelopeCodecError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    Ok(sha256_digest(&bytes))
}

/// Encodes one JSON-bodied envelope, validating that the recorded version is
/// what this build writes for `kind` and that the recorded checksum still
/// matches the payload.
pub fn encode_json_envelope<T: Serialize>(
    kind: &str,
    format_version: u32,
    supported_version: u32,
    payload_checksum: &str,
    payload: &T,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    verify_version(kind, format_version, supported_version)?;
    let payload_json = serde_json::to_string(payload)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    verify_checksum_fresh(payload_checksum, payload_json.as_bytes())?;
    let document = JsonEnvelopeDocument {
        kind: kind.to_owned(),
        format_version,
        payload_checksum: payload_checksum.to_owned(),
        payload: RawValue::from_string(payload_json)
            .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?,
    };
    serde_json::to_vec(&document).map_err(|err| EnvelopeCodecError::EnvelopeEncode(err.to_string()))
}

/// A decoded JSON-bodied envelope's shared fields plus its parsed payload.
pub struct DecodedJsonEnvelope<T> {
    /// Version gate the stored object declared and this build accepted.
    pub format_version: u32,
    /// Digest verified against the payload fragment exactly as stored.
    pub payload_checksum: String,
    /// The family payload decoded from that verified fragment.
    pub payload: T,
}

/// Decodes one JSON-bodied envelope: probe first (kind through
/// `classify_kind`, then version), then the checksum over the stored
/// payload fragment, then the payload itself.
///
/// `classify_kind` lets a family with a kind registry report unknown kinds
/// distinctly from mismatched ones; families with one kind pass
/// [`verify_kind`] directly.
pub fn decode_json_envelope<T: DeserializeOwned>(
    bytes: &[u8],
    supported_version: u32,
    classify_kind: impl FnOnce(&str) -> Result<(), EnvelopeCodecError>,
) -> Result<DecodedJsonEnvelope<T>, EnvelopeCodecError> {
    decode_json_envelope_probe(bytes, supported_version, classify_kind)?;
    let document: JsonEnvelopeDocument = serde_json::from_slice(bytes)
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    decode_json_envelope_payload(document)
}

/// Immutable envelope families tolerate unknown fields, while mutable control-object envelopes
/// reject them. A tolerant read followed by rewrite would erase fields the current binary does
/// not understand.
pub fn decode_strict_json_envelope<T: DeserializeOwned>(
    bytes: &[u8],
    supported_version: u32,
    classify_kind: impl FnOnce(&str) -> Result<(), EnvelopeCodecError>,
) -> Result<DecodedJsonEnvelope<T>, EnvelopeCodecError> {
    decode_json_envelope_probe(bytes, supported_version, classify_kind)?;
    let document: StrictJsonEnvelopeDocument = serde_json::from_slice(bytes)
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    decode_json_envelope_payload(document.into())
}

fn decode_json_envelope_probe(
    bytes: &[u8],
    supported_version: u32,
    classify_kind: impl FnOnce(&str) -> Result<(), EnvelopeCodecError>,
) -> Result<(), EnvelopeCodecError> {
    let probe: EnvelopeProbe = serde_json::from_slice(bytes)
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    classify_kind(&probe.kind)?;
    verify_version(&probe.kind, probe.format_version, supported_version)?;
    Ok(())
}

fn decode_json_envelope_payload<T: DeserializeOwned>(
    document: JsonEnvelopeDocument,
) -> Result<DecodedJsonEnvelope<T>, EnvelopeCodecError> {
    verify_payload_checksum(
        &document.payload_checksum,
        document.payload.get().as_bytes(),
    )?;
    let payload: T = serde_json::from_str(document.payload.get())
        .map_err(|err| EnvelopeCodecError::PayloadDecode(err.to_string()))?;

    Ok(DecodedJsonEnvelope {
        format_version: document.format_version,
        payload_checksum: document.payload_checksum,
        payload,
    })
}
