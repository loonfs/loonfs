//! The `sha256:<64hex>` digest form durable formats use.

use crate::hex::is_lower_hex_byte;
use crate::id_macros::{string_id, validation_error};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Length of a [`sha256_digest`] string in bytes.
pub const SHA256_DIGEST_BYTES: usize = "sha256:".len() + 64;

validation_error!(
    PayloadChecksumValidationError,
    "invalid payload_checksum {value:?}: {reason}"
);

string_id! {
    /// The SHA-256 digest of a durable envelope's payload bytes, spelled
    /// `sha256:` and 64 lowercase hex characters.
    PayloadChecksum,
    error = PayloadChecksumValidationError,
    validate = validate_payload_checksum,
}

fn validate_payload_checksum(value: &str) -> Result<(), PayloadChecksumValidationError> {
    if value.strip_prefix("sha256:").is_some_and(is_sha256_hex) {
        return Ok(());
    }
    // A longer value can't be a digest, and echoing it would make the error
    // as large as the bad input.
    let echoed = if value.len() <= SHA256_DIGEST_BYTES {
        value
    } else {
        ""
    };
    Err(PayloadChecksumValidationError::new(
        echoed,
        "must be `sha256:` followed by 64 lowercase hex characters",
    ))
}

/// Reports whether `text` is a SHA-256 digest in lowercase hexadecimal.
pub(crate) fn is_sha256_hex(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(is_lower_hex_byte)
}

/// Computes the `sha256:` digest of payload bytes that durable envelopes
/// record.
pub fn sha256_digest(bytes: &[u8]) -> PayloadChecksum {
    PayloadChecksum(format!("sha256:{}", sha256_hex(bytes)))
}

/// Computes the lowercase hexadecimal SHA-256 digest of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    crate::hex::hex_encode_bytes(&Sha256::digest(bytes))
}
