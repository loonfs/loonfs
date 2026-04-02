use sha2::{Digest, Sha256};
use std::fmt::Write as _;

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);

    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should not fail");
    }

    encoded
}
