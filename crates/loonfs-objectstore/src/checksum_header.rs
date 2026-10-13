//! The base64 checksum spelling that provider headers and request bodies
//! carry. The wire [`Checksum`] spells the same bytes in lowercase hex.

use base64::Engine as _;
use loonfs_types::format::hex::hex_decode_bytes;
use loonfs_types::{Checksum, ChecksumAlgorithm, ChecksumHex};

/// Encodes a checksum's bytes in the base64 spelling providers use.
pub(crate) fn encode_base64_checksum(checksum: &Checksum) -> String {
    let raw = hex_decode_bytes(checksum.value.as_str())
        .expect("a checksum spelled in lowercase hex should decode");
    base64::engine::general_purpose::STANDARD.encode(raw)
}

/// Decodes a provider's base64 checksum, or returns `None` when the value
/// is not base64 of `algorithm`'s width.
pub(crate) fn decode_base64_checksum(
    algorithm: ChecksumAlgorithm,
    value: &str,
) -> Option<Checksum> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    (raw.len() == algorithm.value_bytes()).then(|| Checksum {
        algorithm,
        value: ChecksumHex::encode(&raw),
    })
}
