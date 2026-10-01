//! The `sha256:<64hex>` digest form durable formats use.

use sha2::{Digest, Sha256};

/// Computes the durable `sha256:` digest spelling used by envelope payloads
/// and local compare tokens.
pub fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    crate::hex::hex_encode_bytes(&Sha256::digest(bytes))
}
