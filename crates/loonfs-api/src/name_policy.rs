//! How a display name folds into the name key directory lookups compare on.
//!
//! v0 has exactly one rule and it is part of the format, not a per-namespace
//! choice: normalize to NFC, apply Unicode default case folding, then
//! normalize to NFC again. Both the write and read paths derive keys through
//! this one function, so a namespace cannot disagree with itself about what
//! two names mean.
//!
//! If a second supported rule ever ships, the head gains a policy field that
//! selects between them; until then there is nothing to select.

use unicode_casefold::UnicodeCaseFold;
use unicode_normalization::UnicodeNormalization;

/// Derives the canonical lookup key for a display name.
pub fn name_key_for_display_name(display_name: &str) -> String {
    display_name
        .nfc()
        .collect::<String>()
        .case_fold()
        .nfc()
        .collect()
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
