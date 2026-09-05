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

/// Durable layout of a JSON-bodied envelope: the shared fields plus the
/// payload as a raw JSON fragment, kept inline so the object remains
/// directly readable JSON while `payload_checksum` covers the exact
/// fragment bytes as stored.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonEnvelopeDocument {
    kind: String,
    format_version: u32,
    payload_checksum: String,
    payload: Box<RawValue>,
}

/// An envelope whose framing was verified on read or derived on write.
///
/// Payload access is read-only: changing a payload requires a new encoding.
/// Family codecs apply their own kind, version, and payload validation rules.
/// Kind and version are checked at the codec boundary, not retained as caller state.
/// This type has no serde decoder; durable reads must use a checked codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedEnvelope<T> {
    pub(crate) payload_checksum: String,
    pub(crate) payload: T,
}

impl<T> VerifiedEnvelope<T> {
    /// Checksum of the exact stored payload bytes.
    pub fn payload_checksum(&self) -> &str {
        &self.payload_checksum
    }
    /// Payload protected by this framing. Clone it to prepare a changed successor.
    pub fn payload(&self) -> &T {
        &self.payload
    }
    /// Takes the payload out of its verified framing for reuse or modification.
    pub fn into_payload(self) -> T {
        self.payload
    }
}

/// One encoding and the envelope derived from those exact bytes.
///
/// Only codecs construct this pair. Neither half can be changed in place.
/// Readers retain just [`VerifiedEnvelope`], without a second copy of the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedEnvelope<T> {
    pub(crate) envelope: VerifiedEnvelope<T>,
    pub(crate) bytes: Vec<u8>,
}

impl<T> EncodedEnvelope<T> {
    /// Envelope derived while encoding, without decoding or serializing again.
    pub fn envelope(&self) -> &VerifiedEnvelope<T> {
        &self.envelope
    }
    /// Complete durable document, ready to write.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Takes the complete durable document.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
    /// Discards the encoding and retains its verified envelope.
    pub fn into_envelope(self) -> VerifiedEnvelope<T> {
        self.envelope
    }
    /// Separates the immutable envelope and its bytes for storage publication.
    pub fn into_parts(self) -> (VerifiedEnvelope<T>, Vec<u8>) {
        (self.envelope, self.bytes)
    }
}

/// Serializes the payload once and derives framing from those same bytes.
pub fn encode_json_envelope<T: Serialize>(
    kind: &str,
    format_version: u32,
    payload: T,
) -> Result<EncodedEnvelope<T>, EnvelopeCodecError> {
    let payload_json = serde_json::to_string(&payload)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    let payload_checksum = sha256_digest(payload_json.as_bytes());
    let document = JsonEnvelopeDocument {
        kind: kind.to_owned(),
        format_version,
        payload_checksum: payload_checksum.clone(),
        payload: RawValue::from_string(payload_json)
            .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?,
    };
    let bytes = serde_json::to_vec(&document)
        .map_err(|err| EnvelopeCodecError::EnvelopeEncode(err.to_string()))?;
    Ok(EncodedEnvelope {
        envelope: VerifiedEnvelope {
            payload_checksum,
            payload,
        },
        bytes,
    })
}

/// Decodes one JSON-bodied envelope, then checks its kind, version, checksum,
/// and payload. Unknown envelope fields are rejected for every durable family.
///
/// `classify_kind` lets a family with a kind registry report unknown kinds
/// distinctly from mismatched ones; families with one kind pass
/// [`verify_kind`] directly.
pub fn decode_json_envelope<T: DeserializeOwned>(
    bytes: &[u8],
    supported_version: u32,
    classify_kind: impl FnOnce(&str) -> Result<(), EnvelopeCodecError>,
) -> Result<VerifiedEnvelope<T>, EnvelopeCodecError> {
    let probe: EnvelopeProbe = serde_json::from_slice(bytes)
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    classify_kind(&probe.kind)?;
    verify_version(&probe.kind, probe.format_version, supported_version)?;
    let document: JsonEnvelopeDocument = serde_json::from_slice(bytes)
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    decode_json_envelope_payload(document)
}

fn decode_json_envelope_payload<T: DeserializeOwned>(
    document: JsonEnvelopeDocument,
) -> Result<VerifiedEnvelope<T>, EnvelopeCodecError> {
    verify_payload_checksum(
        &document.payload_checksum,
        document.payload.get().as_bytes(),
    )?;
    let payload: T = serde_json::from_str(document.payload.get())
        .map_err(|err| EnvelopeCodecError::PayloadDecode(err.to_string()))?;

    Ok(VerifiedEnvelope {
        payload_checksum: document.payload_checksum,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn encoding_serializes_the_payload_once_and_checksums_those_bytes() {
        struct CountedPayload<'a>(&'a Cell<usize>);
        impl Serialize for CountedPayload<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                serializer.serialize_u64(42)
            }
        }
        let calls = Cell::new(0);
        let encoded = encode_json_envelope("test", 1, CountedPayload(&calls)).expect("encode");
        let document: JsonEnvelopeDocument =
            serde_json::from_slice(encoded.as_bytes()).expect("document");
        assert_eq!(calls.get(), 1);
        assert_eq!(document.payload.get(), "42");
        assert_eq!(encoded.envelope().payload_checksum(), sha256_digest(b"42"));
        assert_eq!(
            document.payload_checksum,
            encoded.envelope().payload_checksum()
        );
    }

    #[test]
    fn decoding_checks_the_stored_payload_including_noncanonical_whitespace() {
        let payload = r#"{ "value" : 42 }"#;
        let checksum = sha256_digest(payload.as_bytes());
        let bytes = format!(
            r#"{{"kind":"test","format_version":1,"payload_checksum":"{checksum}","payload":{payload}}}"#
        );
        let decoded: VerifiedEnvelope<serde_json::Value> =
            decode_json_envelope(bytes.as_bytes(), 1, |kind| verify_kind("test", kind))
                .expect("decode exact stored bytes");
        assert_eq!(decoded.payload_checksum(), checksum);
        let successor =
            encode_json_envelope("test", 1, decoded.into_payload()).expect("canonical encoding");
        assert_ne!(successor.envelope().payload_checksum(), checksum);
        let reread: VerifiedEnvelope<serde_json::Value> =
            decode_json_envelope(successor.as_bytes(), 1, |kind| verify_kind("test", kind))
                .expect("decode canonical bytes");
        assert_eq!(&reread, successor.envelope());
    }
}
