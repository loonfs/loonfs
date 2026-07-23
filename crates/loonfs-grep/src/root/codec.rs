//! Versioned JSON envelope codecs for grep root pointers and manifests.

use super::error::GrepRootCodecError;
use super::state::{GrepManifestId, GrepRootPointer, GrepRootState};
use loonfs_api::sha256_digest;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Durable kind string for a grep root-pointer envelope.
pub const GREP_ROOT_KIND: &str = "grep_root";
/// Durable kind string for a grep manifest envelope.
pub const GREP_MANIFEST_KIND: &str = "grep_manifest";
/// Durable v1 root-pointer format string. Unknown strings are rejected.
pub const GREP_ROOT_FORMAT_VERSION: &str = "v1";
/// Durable v1 manifest format string. Unknown strings are rejected.
pub const GREP_MANIFEST_FORMAT_VERSION: &str = "v1";

/// Verified in-memory representation of one grep root-pointer envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepRootEnvelope {
    format_version: String,
    writer_version: String,
    payload_checksum: String,
    pointer: GrepRootPointer,
}

impl GrepRootEnvelope {
    /// Builds a fresh root-pointer envelope over its canonical payload bytes.
    pub fn from_pointer(
        writer_version: impl Into<String>,
        pointer: GrepRootPointer,
    ) -> Result<Self, GrepRootCodecError> {
        let payload = payload_bytes(&pointer)?;
        Ok(Self {
            format_version: GREP_ROOT_FORMAT_VERSION.to_owned(),
            writer_version: writer_version.into(),
            payload_checksum: sha256_digest(&payload),
            pointer,
        })
    }

    pub fn format_version(&self) -> &str {
        &self.format_version
    }

    pub fn writer_version(&self) -> &str {
        &self.writer_version
    }

    pub fn payload_checksum(&self) -> &str {
        &self.payload_checksum
    }

    pub fn pointer(&self) -> &GrepRootPointer {
        &self.pointer
    }
}

/// Verified in-memory representation of one immutable grep manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepManifestEnvelope {
    format_version: String,
    writer_version: String,
    payload_checksum: String,
    manifest_id: GrepManifestId,
    state: GrepRootState,
}

impl GrepManifestEnvelope {
    /// Builds a manifest whose id is the SHA-256 of its canonical payload.
    pub fn from_state(
        writer_version: impl Into<String>,
        state: GrepRootState,
    ) -> Result<Self, GrepRootCodecError> {
        state.validate()?;
        let payload = payload_bytes(&state)?;
        let manifest_id = GrepManifestId::for_payload(&payload);
        Ok(Self {
            format_version: GREP_MANIFEST_FORMAT_VERSION.to_owned(),
            writer_version: writer_version.into(),
            payload_checksum: manifest_id.payload_checksum(),
            manifest_id,
            state,
        })
    }

    pub fn format_version(&self) -> &str {
        &self.format_version
    }

    pub fn writer_version(&self) -> &str {
        &self.writer_version
    }

    pub fn payload_checksum(&self) -> &str {
        &self.payload_checksum
    }

    pub fn manifest_id(&self) -> &GrepManifestId {
        &self.manifest_id
    }

    pub fn state(&self) -> &GrepRootState {
        &self.state
    }
}

#[derive(Debug, Deserialize)]
struct EnvelopeProbe {
    kind: String,
    format_version: String,
}

#[derive(Serialize, Deserialize)]
struct EnvelopeDocument {
    kind: String,
    format_version: String,
    writer_version: String,
    payload_checksum: String,
    payload: Box<RawValue>,
}

/// Encodes a root pointer after rechecking its version and checksum.
pub fn encode_grep_root(envelope: &GrepRootEnvelope) -> Result<Vec<u8>, GrepRootCodecError> {
    encode_envelope(
        GREP_ROOT_KIND,
        GREP_ROOT_FORMAT_VERSION,
        &envelope.format_version,
        &envelope.writer_version,
        &envelope.payload_checksum,
        &envelope.pointer,
    )
}

/// Decodes only the current root-pointer format and verifies exact payload bytes.
pub fn decode_grep_root(bytes: &[u8]) -> Result<GrepRootEnvelope, GrepRootCodecError> {
    let document = decode_document(bytes, GREP_ROOT_KIND, GREP_ROOT_FORMAT_VERSION)?;
    let pointer: GrepRootPointer = decode_payload(&document)?;
    Ok(GrepRootEnvelope {
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: document.payload_checksum,
        pointer,
    })
}

/// Encodes an immutable manifest after rechecking every boundary invariant.
pub fn encode_grep_manifest(
    envelope: &GrepManifestEnvelope,
) -> Result<Vec<u8>, GrepRootCodecError> {
    envelope.state.validate()?;
    let bytes = encode_envelope(
        GREP_MANIFEST_KIND,
        GREP_MANIFEST_FORMAT_VERSION,
        &envelope.format_version,
        &envelope.writer_version,
        &envelope.payload_checksum,
        &envelope.state,
    )?;
    let payload = payload_bytes(&envelope.state)?;
    let actual_id = GrepManifestId::for_payload(&payload);
    if actual_id != envelope.manifest_id {
        return Err(GrepRootCodecError::StalePayloadChecksum {
            checksum: envelope.manifest_id.payload_checksum(),
            actual: actual_id.payload_checksum(),
        });
    }
    Ok(bytes)
}

/// Decodes only the current manifest format, verifies it, and derives its id.
pub fn decode_grep_manifest(bytes: &[u8]) -> Result<GrepManifestEnvelope, GrepRootCodecError> {
    let document = decode_document(bytes, GREP_MANIFEST_KIND, GREP_MANIFEST_FORMAT_VERSION)?;
    let state: GrepRootState = decode_payload(&document)?;
    state.validate()?;
    let manifest_id = GrepManifestId::for_payload(document.payload.get().as_bytes());
    if manifest_id.payload_checksum() != document.payload_checksum {
        return Err(GrepRootCodecError::ChecksumMismatch {
            expected: document.payload_checksum,
            actual: manifest_id.payload_checksum(),
        });
    }
    Ok(GrepManifestEnvelope {
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: manifest_id.payload_checksum(),
        manifest_id,
        state,
    })
}

fn encode_envelope<T: Serialize>(
    kind: &str,
    supported_version: &str,
    format_version: &str,
    writer_version: &str,
    payload_checksum: &str,
    payload: &T,
) -> Result<Vec<u8>, GrepRootCodecError> {
    verify_version(format_version, supported_version)?;
    let payload =
        serde_json::to_string(payload).map_err(|error| GrepRootCodecError::PayloadEncode {
            message: error.to_string(),
        })?;
    let actual = sha256_digest(payload.as_bytes());
    if actual != payload_checksum {
        return Err(GrepRootCodecError::StalePayloadChecksum {
            checksum: payload_checksum.to_owned(),
            actual,
        });
    }
    let document = EnvelopeDocument {
        kind: kind.to_owned(),
        format_version: format_version.to_owned(),
        writer_version: writer_version.to_owned(),
        payload_checksum: payload_checksum.to_owned(),
        payload: RawValue::from_string(payload).map_err(|error| {
            GrepRootCodecError::PayloadEncode {
                message: error.to_string(),
            }
        })?,
    };
    serde_json::to_vec(&document).map_err(|error| GrepRootCodecError::EnvelopeEncode {
        message: error.to_string(),
    })
}

fn decode_document(
    bytes: &[u8],
    expected_kind: &str,
    supported_version: &str,
) -> Result<EnvelopeDocument, GrepRootCodecError> {
    let probe: EnvelopeProbe =
        serde_json::from_slice(bytes).map_err(|error| GrepRootCodecError::EnvelopeDecode {
            message: error.to_string(),
        })?;
    if probe.kind != expected_kind {
        return Err(GrepRootCodecError::KindMismatch {
            expected: expected_kind.to_owned(),
            found: probe.kind,
        });
    }
    verify_version(&probe.format_version, supported_version)?;

    let document: EnvelopeDocument =
        serde_json::from_slice(bytes).map_err(|error| GrepRootCodecError::EnvelopeDecode {
            message: error.to_string(),
        })?;
    let actual = sha256_digest(document.payload.get().as_bytes());
    if actual != document.payload_checksum {
        return Err(GrepRootCodecError::ChecksumMismatch {
            expected: document.payload_checksum,
            actual,
        });
    }
    Ok(document)
}

fn decode_payload<T: DeserializeOwned>(
    document: &EnvelopeDocument,
) -> Result<T, GrepRootCodecError> {
    serde_json::from_str(document.payload.get()).map_err(|error| {
        GrepRootCodecError::PayloadDecode {
            message: error.to_string(),
        }
    })
}

fn payload_bytes<T: Serialize>(payload: &T) -> Result<Vec<u8>, GrepRootCodecError> {
    serde_json::to_vec(payload).map_err(|error| GrepRootCodecError::PayloadEncode {
        message: error.to_string(),
    })
}

fn verify_version(found: &str, supported: &str) -> Result<(), GrepRootCodecError> {
    if found != supported {
        return Err(GrepRootCodecError::UnsupportedFormatVersion {
            found: found.to_owned(),
            supported: supported.to_owned(),
        });
    }
    Ok(())
}
