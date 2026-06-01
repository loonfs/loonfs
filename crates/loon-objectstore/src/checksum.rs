use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

pub(crate) fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex_bytes(Sha256::digest(bytes)))
}

pub(crate) fn sha256_base64(bytes: &[u8]) -> String {
    STANDARD.encode(Sha256::digest(bytes))
}

pub(crate) fn sha256_digest_from_base64(encoded: &str) -> Result<String, String> {
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|err| format!("invalid base64 SHA-256 checksum: {err}"))?;
    if bytes.len() != 32 {
        return Err(format!(
            "invalid SHA-256 checksum length: expected 32 bytes, got {}",
            bytes.len()
        ));
    }
    Ok(format!("sha256:{}", sha256_hex_bytes(bytes)))
}

fn sha256_hex_bytes(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should not fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{sha256_base64, sha256_digest, sha256_digest_from_base64};

    #[test]
    fn sha256_base64_round_trips_to_normalized_digest() {
        let bytes = b"checksum bytes";
        let encoded = sha256_base64(bytes);
        assert_eq!(
            sha256_digest_from_base64(&encoded).expect("decode checksum"),
            sha256_digest(bytes)
        );
    }

    #[test]
    fn sha256_digest_from_base64_rejects_wrong_length() {
        assert!(sha256_digest_from_base64("aGVsbG8=").is_err());
    }
}
