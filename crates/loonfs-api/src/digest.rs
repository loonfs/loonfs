//! The `sha256:<64hex>` digest form durable formats use.

use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Computes the durable `sha256:` digest spelling used by content and envelope references.
pub fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);

    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should not fail");
    }

    encoded
}
