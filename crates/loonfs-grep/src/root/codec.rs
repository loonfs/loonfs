//! Grep's durable families share framing and checksum rules with the filesystem.

use super::error::GrepEnvelopeCodecError;
use super::state::{GrepManifestState, GrepRootPointer};
use loonfs_api::wire::envelope::{
    decode_json_envelope, encode_json_envelope, verify_kind, EncodedEnvelope, VerifiedEnvelope,
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
pub type GrepRootEnvelope = VerifiedEnvelope<GrepRootPointer>;
/// Verified in-memory representation of one immutable grep manifest.
/// Its identity is the key named by the root pointer.
pub type GrepManifestEnvelope = VerifiedEnvelope<GrepManifestState>;

/// Encodes a root pointer and derives its framing from the exact payload bytes.
pub fn encode_grep_root(
    pointer: GrepRootPointer,
) -> Result<EncodedEnvelope<GrepRootPointer>, GrepEnvelopeCodecError> {
    Ok(encode_json_envelope(
        GREP_ROOT_KIND,
        GREP_ROOT_FORMAT_VERSION,
        pointer,
    )?)
}

/// Decodes only the current root-pointer format and verifies exact payload bytes.
/// Unknown fields are rejected because publication writes successor pointers.
pub fn decode_grep_root(bytes: &[u8]) -> Result<GrepRootEnvelope, GrepEnvelopeCodecError> {
    Ok(decode_json_envelope(
        bytes,
        GREP_ROOT_FORMAT_VERSION,
        |found| verify_kind(GREP_ROOT_KIND, found),
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
