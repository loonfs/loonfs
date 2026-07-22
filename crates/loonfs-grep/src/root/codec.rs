//! Versioned JSON envelope codec for the grep root.

use super::error::GrepRootCodecError;
use super::state::GrepRootState;
use loonfs_api::sha256_digest;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Durable kind string for a grep root envelope.
pub const GREP_ROOT_KIND: &str = "grep_root";
/// Durable v0 format string. Unknown strings are rejected without fallback.
pub const GREP_ROOT_FORMAT_VERSION: &str = "v0";

/// Verified in-memory representation of one grep root envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepRootEnvelope {
    format_version: String,
    writer_version: String,
    payload_checksum: String,
    state: GrepRootState,
}

impl GrepRootEnvelope {
    /// Builds a fresh envelope and checksums the canonical payload bytes.
    pub fn from_state(
        writer_version: impl Into<String>,
        state: GrepRootState,
    ) -> Result<Self, GrepRootCodecError> {
        state.validate()?;
        let payload =
            serde_json::to_vec(&state).map_err(|error| GrepRootCodecError::PayloadEncode {
                message: error.to_string(),
            })?;
        Ok(Self {
            format_version: GREP_ROOT_FORMAT_VERSION.to_owned(),
            writer_version: writer_version.into(),
            payload_checksum: sha256_digest(&payload),
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

/// Encodes an envelope after rechecking its version, state, and checksum.
pub fn encode_grep_root(envelope: &GrepRootEnvelope) -> Result<Vec<u8>, GrepRootCodecError> {
    verify_version(&envelope.format_version)?;
    envelope.state.validate()?;
    let payload = serde_json::to_string(&envelope.state).map_err(|error| {
        GrepRootCodecError::PayloadEncode {
            message: error.to_string(),
        }
    })?;
    let actual = sha256_digest(payload.as_bytes());
    if actual != envelope.payload_checksum {
        return Err(GrepRootCodecError::StalePayloadChecksum {
            checksum: envelope.payload_checksum.clone(),
            actual,
        });
    }
    let document = EnvelopeDocument {
        kind: GREP_ROOT_KIND.to_owned(),
        format_version: envelope.format_version.clone(),
        writer_version: envelope.writer_version.clone(),
        payload_checksum: envelope.payload_checksum.clone(),
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

/// Decodes only v0, verifies the exact stored payload bytes, and validates
/// the decoded root's cross-field invariants.
pub fn decode_grep_root(bytes: &[u8]) -> Result<GrepRootEnvelope, GrepRootCodecError> {
    let probe: EnvelopeProbe =
        serde_json::from_slice(bytes).map_err(|error| GrepRootCodecError::EnvelopeDecode {
            message: error.to_string(),
        })?;
    if probe.kind != GREP_ROOT_KIND {
        return Err(GrepRootCodecError::KindMismatch {
            expected: GREP_ROOT_KIND.to_owned(),
            found: probe.kind,
        });
    }
    verify_version(&probe.format_version)?;

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
    let state: GrepRootState = serde_json::from_str(document.payload.get()).map_err(|error| {
        GrepRootCodecError::PayloadDecode {
            message: error.to_string(),
        }
    })?;
    state.validate()?;
    Ok(GrepRootEnvelope {
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: document.payload_checksum,
        state,
    })
}

fn verify_version(found: &str) -> Result<(), GrepRootCodecError> {
    if found != GREP_ROOT_FORMAT_VERSION {
        return Err(GrepRootCodecError::UnsupportedFormatVersion {
            found: found.to_owned(),
            supported: GREP_ROOT_FORMAT_VERSION.to_owned(),
        });
    }
    Ok(())
}
