//! How a display name folds into the name key directory lookups compare on.
//!
//! The fixed data versions and evolution rule are in `docs/specs/format.md`,
//! section 1.4. Admission and lookup share this implementation.

use icu_casemap::CaseMapper;
use icu_normalizer::ComposingNormalizer;

/// Derives the canonical lookup key for a display name.
pub fn name_key_for_display_name(display_name: &str) -> String {
    let normalizer = ComposingNormalizer::new_nfc();
    let normalized = normalizer.normalize(display_name);
    let folded = CaseMapper::new().fold_string(&normalized);
    normalizer.normalize(&folded).into_owned()
}

#[cfg(test)]
mod tests {
    use super::name_key_for_display_name;

    #[test]
    fn every_scalar_value_expands_at_most_threefold() {
        // Normalization and case folding act on one code point at a time and
        // composition only shortens, so the worst code point bounds every
        // name, and `ids.rs` asserts at compile time that three times
        // `MAX_DISPLAY_NAME_BYTES` fits `MAX_NAME_KEY_BYTES`.
        let mut name = String::new();
        for scalar in char::MIN..=char::MAX {
            name.clear();
            name.push(scalar);
            let key_bytes = name_key_for_display_name(&name).len();
            assert!(
                key_bytes <= 3 * name.len(),
                "U+{:04X} has a {key_bytes}-byte key from {} bytes",
                u32::from(scalar),
                name.len()
            );
        }
    }
}
