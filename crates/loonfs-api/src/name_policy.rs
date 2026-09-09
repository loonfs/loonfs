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
    fn nfc_casefold_v0_normalizes_and_casefolds() {
        let decomposed = "Cafe\u{301}.TXT";
        let composed = "CAFÉ.txt";

        let left = name_key_for_display_name(decomposed);
        let right = name_key_for_display_name(composed);

        assert_eq!(left, right);
        assert_eq!(left, "café.txt");
    }
}
