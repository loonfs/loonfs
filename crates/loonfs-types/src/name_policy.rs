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
    fn keys_are_normalized_then_fully_case_folded_then_normalized_again() {
        let cases: &[(&[&str], &str)] = &[
            // A decomposed and a precomposed spelling share one key.
            (&["Cafe\u{301}.TXT", "CAFÉ.txt", "café.txt"], "café.txt"),
            (&["A\u{30a}ngstro\u{308}m", "Ångström"], "ångström"),
            // Full folding expands one code point into several.
            (&["Straße", "STRASSE"], "strasse"),
            (&["ẞ", "ß", "SS"], "ss"),
            (&["ﬃ", "FFI"], "ffi"),
            // Every case form of a letter, including final sigma, shares one
            // key.
            (&["Σ", "σ", "ς"], "σ"),
            // Folding can leave a decomposed sequence that composes, so the
            // second normalization composes it.
            (&["Ϊ\u{301}", "ΐ"], "ΐ"),
            // The default folding, not the Turkic variant: dotless i keeps
            // its own key, and dotted capital I folds to i and a combining
            // dot above.
            (&["I", "i"], "i"),
            (&["ı"], "ı"),
            (&["İ", "i\u{307}"], "i\u{307}"),
        ];
        for (display_names, key) in cases {
            for display_name in *display_names {
                assert_eq!(
                    name_key_for_display_name(display_name),
                    *key,
                    "{display_name:?}"
                );
            }
        }
    }

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
