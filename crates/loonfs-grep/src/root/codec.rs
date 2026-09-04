//! Grep's two durable families, parameterized over the shared envelope codec.
//!
//! Grep owns its kinds, its versions, and its payload invariants; the probe,
//! the checksum rules, and the failure vocabulary are the ones every other
//! LoonFS durable family reads and writes through.

use super::error::GrepEnvelopeCodecError;
use super::state::{GrepManifestState, GrepRootPointer};
use loonfs_api::wire::envelope::{
    decode_json_envelope, encode_json_envelope, json_payload_checksum, verify_kind,
    DecodedJsonEnvelope,
};

/// Durable kind string for a grep root-pointer envelope.
pub const GREP_ROOT_KIND: &str = "grep_root";
/// Durable kind string for a grep manifest envelope.
pub const GREP_MANIFEST_KIND: &str = "grep_manifest";
/// Sole root-pointer format version this build reads and writes.
pub const GREP_ROOT_FORMAT_VERSION: u32 = 1;
/// Sole manifest format version this build reads and writes.
pub const GREP_MANIFEST_FORMAT_VERSION: u32 = 1;

/// Verified in-memory representation of one grep root-pointer envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepRootEnvelope {
    format_version: u32,
    payload_checksum: String,
    pointer: GrepRootPointer,
}

impl GrepRootEnvelope {
    /// Builds a fresh root-pointer envelope over its canonical payload bytes.
    pub fn from_pointer(pointer: GrepRootPointer) -> Result<Self, GrepEnvelopeCodecError> {
        Ok(Self {
            format_version: GREP_ROOT_FORMAT_VERSION,
            payload_checksum: json_payload_checksum(&pointer)?,
            pointer,
        })
    }

    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn payload_checksum(&self) -> &str {
        &self.payload_checksum
    }

    pub fn pointer(&self) -> &GrepRootPointer {
        &self.pointer
    }
}

/// Verified in-memory representation of one immutable grep manifest.
///
/// The envelope describes the bytes and nothing about which object holds
/// them: a manifest's identity is the key it was written under, which the
/// root pointer names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepManifestEnvelope {
    format_version: u32,
    payload_checksum: String,
    state: GrepManifestState,
}

impl GrepManifestEnvelope {
    /// Builds a manifest envelope over its canonical payload bytes.
    pub fn from_state(state: GrepManifestState) -> Result<Self, GrepEnvelopeCodecError> {
        state.validate()?;
        Ok(Self {
            format_version: GREP_MANIFEST_FORMAT_VERSION,
            payload_checksum: json_payload_checksum(&state)?,
            state,
        })
    }

    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn payload_checksum(&self) -> &str {
        &self.payload_checksum
    }

    pub fn manifest_state(&self) -> &GrepManifestState {
        &self.state
    }
}

/// Encodes a root pointer after rechecking its version and checksum.
pub fn encode_grep_root(envelope: &GrepRootEnvelope) -> Result<Vec<u8>, GrepEnvelopeCodecError> {
    Ok(encode_json_envelope(
        GREP_ROOT_KIND,
        envelope.format_version,
        GREP_ROOT_FORMAT_VERSION,
        &envelope.payload_checksum,
        &envelope.pointer,
    )?)
}

/// Decodes only the current root-pointer format and verifies exact payload bytes.
///
/// The pointer is a mutable control object, so unknown envelope and payload
/// fields are corruption: a tolerant read followed by the publication rewrite
/// would erase what this binary does not understand.
pub fn decode_grep_root(bytes: &[u8]) -> Result<GrepRootEnvelope, GrepEnvelopeCodecError> {
    let decoded: DecodedJsonEnvelope<GrepRootPointer> =
        decode_json_envelope(bytes, GREP_ROOT_FORMAT_VERSION, |found| {
            verify_kind(GREP_ROOT_KIND, found)
        })?;
    Ok(GrepRootEnvelope {
        format_version: decoded.format_version,
        payload_checksum: decoded.payload_checksum,
        pointer: decoded.payload,
    })
}

/// Encodes an immutable manifest after rechecking every boundary invariant.
pub fn encode_grep_manifest(
    envelope: &GrepManifestEnvelope,
) -> Result<Vec<u8>, GrepEnvelopeCodecError> {
    envelope.state.validate()?;
    Ok(encode_json_envelope(
        GREP_MANIFEST_KIND,
        envelope.format_version,
        GREP_MANIFEST_FORMAT_VERSION,
        &envelope.payload_checksum,
        &envelope.state,
    )?)
}

/// Decodes only the current manifest format and verifies it.
///
/// Unknown fields are rejected because indexing and compaction write successor manifests.
pub fn decode_grep_manifest(bytes: &[u8]) -> Result<GrepManifestEnvelope, GrepEnvelopeCodecError> {
    let decoded: DecodedJsonEnvelope<GrepManifestState> =
        decode_json_envelope(bytes, GREP_MANIFEST_FORMAT_VERSION, |found| {
            verify_kind(GREP_MANIFEST_KIND, found)
        })?;
    decoded.payload.validate()?;
    Ok(GrepManifestEnvelope {
        format_version: decoded.format_version,
        payload_checksum: decoded.payload_checksum,
        state: decoded.payload,
    })
}
