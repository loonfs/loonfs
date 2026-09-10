//! Grep's durable families share framing and checksum rules with the filesystem.

use super::error::GrepEnvelopeCodecError;
use super::state::{GrepHint, GrepManifestState};
use loonfs_api::wire::envelope::{
    decode_json_envelope, encode_json_envelope, verify_kind, EncodedEnvelope, VerifiedEnvelope,
};

/// Durable kind string for a grep hint envelope.
pub const GREP_HINT_KIND: &str = "grep_hint";
/// Durable kind string for a grep manifest envelope.
pub const GREP_MANIFEST_KIND: &str = "grep_manifest";
/// Sole hint format version this build reads and writes.
pub const GREP_HINT_FORMAT_VERSION: u32 = 1;
/// Sole manifest format version this build reads and writes.
pub const GREP_MANIFEST_FORMAT_VERSION: u32 = 1;

/// Verified in-memory representation of one grep hint envelope.
pub type GrepHintEnvelope = VerifiedEnvelope<GrepHint>;
/// Verified in-memory representation of one immutable grep manifest.
pub type GrepManifestEnvelope = VerifiedEnvelope<GrepManifestState>;

/// Encodes a hint and derives its framing from the exact payload bytes.
pub fn encode_grep_hint(
    hint: GrepHint,
) -> Result<EncodedEnvelope<GrepHint>, GrepEnvelopeCodecError> {
    Ok(encode_json_envelope(
        GREP_HINT_KIND,
        GREP_HINT_FORMAT_VERSION,
        hint,
    )?)
}

/// Decodes only the current hint format and verifies exact payload bytes.
/// Unknown fields are rejected because publication writes successor hints.
pub fn decode_grep_hint(bytes: &[u8]) -> Result<GrepHintEnvelope, GrepEnvelopeCodecError> {
    Ok(decode_json_envelope(
        bytes,
        GREP_HINT_FORMAT_VERSION,
        |found| verify_kind(GREP_HINT_KIND, found),
    )?)
}

/// Validates a manifest and derives framing from one payload encoding.
pub fn encode_grep_manifest(
    state: GrepManifestState,
) -> Result<EncodedEnvelope<GrepManifestState>, GrepEnvelopeCodecError> {
    state.validate()?;
    Ok(encode_json_envelope(
        GREP_MANIFEST_KIND,
        GREP_MANIFEST_FORMAT_VERSION,
        state,
    )?)
}

/// Decodes only the current manifest format and verifies it.
/// Unknown fields are rejected because indexing and compaction write successors.
pub fn decode_grep_manifest(bytes: &[u8]) -> Result<GrepManifestEnvelope, GrepEnvelopeCodecError> {
    let decoded: GrepManifestEnvelope =
        decode_json_envelope(bytes, GREP_MANIFEST_FORMAT_VERSION, |found| {
            verify_kind(GREP_MANIFEST_KIND, found)
        })?;
    decoded.payload().validate()?;
    Ok(decoded)
}
