//! The base64 checksum spelling that provider headers and request bodies
//! include. The wire [`Checksum`] spells the same bytes in lowercase hex.

use base64::Engine as _;
use loonfs_types::format::hex::hex_decode_bytes;
use loonfs_types::{Checksum, ChecksumAlgorithm, ChecksumHex};

/// Encodes a checksum's bytes in the base64 spelling providers use.
pub(crate) fn encode_base64_checksum(checksum: &Checksum) -> String {
    let raw = hex_decode_bytes(checksum.value().as_str())
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
    Checksum::new(algorithm, ChecksumHex::encode(&raw)).ok()
}

/// Encodes a checksum's bytes least significant first, as Azure spells a
/// CRC-64 in `x-ms-content-crc64`.
pub(crate) fn encode_base64_checksum_little_endian(checksum: &Checksum) -> String {
    let mut raw = hex_decode_bytes(checksum.value().as_str())
        .expect("a checksum spelled in lowercase hex should decode");
    raw.reverse();
    base64::engine::general_purpose::STANDARD.encode(raw)
}

/// Decodes a base64 checksum whose bytes are least significant first, or
/// returns `None` when the value is not base64 of `algorithm`'s width.
pub(crate) fn decode_base64_checksum_little_endian(
    algorithm: ChecksumAlgorithm,
    value: &str,
) -> Option<Checksum> {
    let mut raw = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    raw.reverse();
    Checksum::new(algorithm, ChecksumHex::encode(&raw)).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Microsoft documents the CRC-64 of the byte `0x11` as the bytes
    /// `d0616757b45f54d2`, which are the CRC-64/NVME of that byte spelled
    /// least significant first.
    #[test]
    fn azure_spells_a_crc64nvme_least_significant_byte_first() {
        let checksum = Checksum::crc64nvme(&[0x11]);
        assert_eq!(checksum.value().as_str(), "d2545fb4576761d0");
        assert_eq!(encode_base64_checksum(&checksum), "0lRftFdnYdA=");
        assert_eq!(
            encode_base64_checksum_little_endian(&checksum),
            "0GFnV7RfVNI="
        );
        assert_eq!(
            decode_base64_checksum_little_endian(ChecksumAlgorithm::Crc64nvme, "0GFnV7RfVNI="),
            Some(checksum)
        );
        assert_eq!(
            decode_base64_checksum_little_endian(ChecksumAlgorithm::Crc64nvme, "short"),
            None
        );
    }
}
