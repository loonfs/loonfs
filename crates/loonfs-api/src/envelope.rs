//! The durable envelope layout, decided once.
//!
//! Every durable LoonFS object is an envelope document with the same leading
//! fields — `kind`, `format_version`, `writer_version`, `payload_checksum` —
//! followed by the payload as an opaque sub-document. This module owns the
//! whole contract: the probe, the one error vocabulary, the kind/version/
//! checksum validation rules, and the generic JSON codec that control
//! objects and namespace manifests parameterize. The WAL segment codec keeps
//! its CBOR-plus-zstd transport but validates through the same rules and
//! reports through the same errors, so no envelope family can drift from
//! the others.
//!
//! `payload_checksum` is always computed over the exact payload bytes as
//! stored, never over a re-encoding, so checksum failures mean corruption
//! and version skew surfaces as a version error.

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
pub(crate) struct EnvelopeProbe {
    pub(crate) kind: String,
    pub(crate) format_version: u32,
}

/// Failure vocabulary shared by every envelope codec. Messages are
/// envelope-generic; the wrapping error names the object (and its key) the
/// bytes came from.
#[derive(Debug, Error)]
pub enum EnvelopeCodecError {
    #[error("failed to encode envelope payload: {0}")]
    PayloadEncode(String),
    #[error("failed to encode envelope document: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode envelope document: {0}")]
    EnvelopeDecode(String),
    #[error("failed to decode envelope payload: {0}")]
    PayloadDecode(String),
    #[error("failed to compress envelope: {0}")]
    Compress(String),
    #[error("failed to decompress envelope: {0}")]
    Decompress(String),
    #[error("unknown envelope kind `{found}`")]
    UnknownKind { found: String },
    #[error("envelope kind mismatch: expected `{expected}`, found `{found}`")]
    KindMismatch { expected: String, found: String },
    #[error(
        "unsupported `{kind}` envelope format version `{found}`: \
         this build supports `{supported}`"
    )]
    UnsupportedFormatVersion {
        kind: String,
        found: u32,
        supported: u32,
    },
    #[error("envelope payload checksum mismatch: expected `{expected}`, actual `{actual}`")]
    ChecksumMismatch { expected: String, actual: String },
    #[error(
        "envelope checksum `{checksum}` does not match its payload `{actual}`: \
         rebuild the envelope from its payload"
    )]
    StalePayloadChecksum { checksum: String, actual: String },
}

/// Requires the probed kind to be exactly `expected`.
pub(crate) fn verify_kind(expected: &str, found: &str) -> Result<(), EnvelopeCodecError> {
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
pub(crate) fn verify_version(
    kind: &str,
    found: u32,
    supported: u32,
) -> Result<(), EnvelopeCodecError> {
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
pub(crate) fn verify_payload_checksum(
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
pub(crate) fn verify_checksum_fresh(
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
    writer_version: String,
    payload_checksum: String,
    payload: Box<RawValue>,
}

/// The `sha256:<hex>` checksum a JSON payload will carry, computed over its
/// canonical serialization.
pub(crate) fn json_payload_checksum<T: Serialize>(
    payload: &T,
) -> Result<String, EnvelopeCodecError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    Ok(sha256_digest(&bytes))
}

/// Encodes one JSON-bodied envelope, validating that the recorded version is
/// what this build writes for `kind` and that the recorded checksum still
/// matches the payload.
pub(crate) fn encode_json_envelope<T: Serialize>(
    kind: &str,
    format_version: u32,
    supported_version: u32,
    writer_version: &str,
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
        writer_version: writer_version.to_owned(),
        payload_checksum: payload_checksum.to_owned(),
        payload: RawValue::from_string(payload_json)
            .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?,
    };
    serde_json::to_vec(&document).map_err(|err| EnvelopeCodecError::EnvelopeEncode(err.to_string()))
}

/// A decoded JSON-bodied envelope's shared fields plus its parsed payload.
pub(crate) struct DecodedJsonEnvelope<T> {
    pub(crate) format_version: u32,
    pub(crate) writer_version: String,
    pub(crate) payload_checksum: String,
    pub(crate) payload: T,
}

/// Decodes one JSON-bodied envelope: probe first (kind through
/// `classify_kind`, then version), then the checksum over the stored
/// payload fragment, then the payload itself.
///
/// `classify_kind` lets a family with a kind registry report unknown kinds
/// distinctly from mismatched ones; families with one kind pass
/// [`verify_kind`] directly.
pub(crate) fn decode_json_envelope<T: DeserializeOwned>(
    bytes: &[u8],
    supported_version: u32,
    classify_kind: impl FnOnce(&str) -> Result<(), EnvelopeCodecError>,
) -> Result<DecodedJsonEnvelope<T>, EnvelopeCodecError> {
    let probe: EnvelopeProbe = serde_json::from_slice(bytes)
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    classify_kind(&probe.kind)?;
    verify_version(&probe.kind, probe.format_version, supported_version)?;

    let document: JsonEnvelopeDocument = serde_json::from_slice(bytes)
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    verify_payload_checksum(
        &document.payload_checksum,
        document.payload.get().as_bytes(),
    )?;
    let payload: T = serde_json::from_str(document.payload.get())
        .map_err(|err| EnvelopeCodecError::PayloadDecode(err.to_string()))?;

    Ok(DecodedJsonEnvelope {
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: document.payload_checksum,
        payload,
    })
}
