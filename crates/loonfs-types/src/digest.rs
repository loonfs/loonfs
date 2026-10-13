//! The `sha256:<64hex>` digest form durable formats use.

use sha2::{Digest, Sha256};

/// Computes the durable `sha256:` digest spelling used by envelope payloads
/// and local compare tokens.
pub fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

/// Computes the lowercase hexadecimal SHA-256 digest of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    crate::hex::hex_encode_bytes(&Sha256::digest(bytes))
}
