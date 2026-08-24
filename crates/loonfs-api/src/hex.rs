//! Lowercase hexadecimal encoding shared by wire and durable codecs.

use thiserror::Error;

/// Describes why bytes cannot be decoded from the durable lowercase hexadecimal form.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HexDecodeError {
    /// Reports an input that ends after the high nibble of its final byte.
    #[error("odd hex length {length}")]
    OddLength {
        /// Number of ASCII bytes in the rejected input.
        length: usize,
    },
    /// Reports an input byte outside `0`–`9` and `a`–`f`.
    #[error("invalid hex byte {byte:#04x}")]
    InvalidByte {
        /// First byte that violated the lowercase hexadecimal alphabet.
        byte: u8,
    },
}

/// Encodes bytes using the lowercase hexadecimal alphabet expected by durable codecs.
pub fn hex_encode_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    encoded
}

fn nibble(byte: u8) -> Result<u8, HexDecodeError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(HexDecodeError::InvalidByte { byte }),
    }
}

/// Reports whether `byte` is in the lowercase hexadecimal alphabet: it is
/// exactly the set of bytes this module can decode.
pub(crate) fn is_lower_hex_byte(byte: u8) -> bool {
    nibble(byte).is_ok()
}

/// Decodes lowercase hex. Errors carry no input bytes; callers name the
/// field they were decoding.
pub fn hex_decode_bytes(encoded: &str) -> Result<Vec<u8>, HexDecodeError> {
    let bytes = encoded.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(HexDecodeError::OddLength {
            length: bytes.len(),
        });
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        decoded.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Ok(decoded)
}
