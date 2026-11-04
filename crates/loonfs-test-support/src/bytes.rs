//! Generated test content.

/// Returns `len` bytes that zstd can't compress, generated from `seed`, so a
/// WAL object stores about as many bytes as the inline content it carries.
pub fn incompressible_bytes(seed: u64, len: usize) -> Vec<u8> {
    // Setting the low bit keeps the xorshift state nonzero, because xorshift
    // never leaves zero.
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut bytes = Vec::with_capacity(len + 8);
    while bytes.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}
