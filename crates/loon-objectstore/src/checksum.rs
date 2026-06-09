use sha2::{Digest, Sha256};
use std::fmt::Write as _;

pub(crate) fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex_bytes(Sha256::digest(bytes)))
}

fn sha256_hex_bytes(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should not fail");
    }
    encoded
}
