//! Cryptographic primitives shared by object-store signing and callers that
//! already depend on this crate.

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Computes the HMAC-SHA256 signature used by content tokens.
pub fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(key).expect("HMAC-SHA256 should accept a key of any length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

/// Compares token bytes without short-circuiting on the first mismatch, so
/// response timing does not narrow down the token byte by byte.
///
/// Length is still observable; token values are not.
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::hmac_sha256;

    #[test]
    fn hmac_sha256_matches_rfc_4231_vectors() {
        let case_one = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            loonfs_types::format::hex::hex_encode_bytes(&case_one),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        let case_two = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            loonfs_types::format::hex::hex_encode_bytes(&case_two),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
