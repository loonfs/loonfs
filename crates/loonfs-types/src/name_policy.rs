//! How a display name folds into the name key directory lookups compare on.
//!
//! The fixed data versions and evolution rule are in `docs/specs/format.md`,
//! section 1.4. Admission and lookup share this implementation.

use icu_casemap::CaseMapper;
use icu_normalizer::ComposingNormalizer;
use serde::{Deserialize, Serialize};

/// How a namespace compares sibling names, fixed at creation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum NamespaceNaming {
    /// Names that differ only in letter case or Unicode normalization are
    /// the same name.
    #[default]
    CaseInsensitive,
    /// Names that differ only in Unicode normalization are the same name.
    CaseSensitive,
}

impl NamespaceNaming {
    /// Returns the serialized value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CaseInsensitive => "case_insensitive",
            Self::CaseSensitive => "case_sensitive",
        }
    }
}

/// Derives the canonical lookup key for a display name under `naming`.
pub fn name_key_for_display_name(naming: NamespaceNaming, display_name: &str) -> String {
    let normalizer = ComposingNormalizer::new_nfc();
    let normalized = normalizer.normalize(display_name);
    match naming {
        NamespaceNaming::CaseInsensitive => {
            let folded = CaseMapper::new().fold_string(&normalized);
            normalizer.normalize(&folded).into_owned()
        }
        NamespaceNaming::CaseSensitive => normalized.into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{name_key_for_display_name, NamespaceNaming};

    #[test]
    fn each_mode_normalizes_and_only_case_insensitive_folds() {
        let decomposed = "Cafe\u{301}.TXT";
        let composed = "CAFÉ.txt";

        let insensitive = NamespaceNaming::CaseInsensitive;
        assert_eq!(
            name_key_for_display_name(insensitive, decomposed),
            name_key_for_display_name(insensitive, composed)
        );
        assert_eq!(name_key_for_display_name(insensitive, composed), "café.txt");

        let sensitive = NamespaceNaming::CaseSensitive;
        assert_eq!(name_key_for_display_name(sensitive, decomposed), "Café.TXT");
        assert_ne!(
            name_key_for_display_name(sensitive, decomposed),
            name_key_for_display_name(sensitive, composed)
        );
    }

    #[test]
    fn naming_serializes_as_its_wire_string() {
        for naming in [
            NamespaceNaming::CaseInsensitive,
            NamespaceNaming::CaseSensitive,
        ] {
            assert_eq!(
                serde_json::to_value(naming).expect("serialize naming"),
                serde_json::Value::from(naming.as_str())
            );
        }
        assert!(serde_json::from_str::<NamespaceNaming>("\"labels\"").is_err());
    }

    #[test]
    fn every_scalar_value_expands_at_most_threefold_in_either_mode() {
        // Normalization and case folding act on one code point at a time and
        // composition only shortens, so the worst code point bounds every
        // name, and `ids.rs` asserts at compile time that three times
        // `MAX_DISPLAY_NAME_BYTES` fits `MAX_NAME_KEY_BYTES`.
        let mut name = String::new();
        for scalar in char::MIN..=char::MAX {
            name.clear();
            name.push(scalar);
            for naming in [
                NamespaceNaming::CaseInsensitive,
                NamespaceNaming::CaseSensitive,
            ] {
                let key_bytes = name_key_for_display_name(naming, &name).len();
                assert!(
                    key_bytes <= 3 * name.len(),
                    "U+{:04X} has a {key_bytes}-byte key from {} bytes ({naming:?})",
                    u32::from(scalar),
                    name.len()
                );
            }
        }
    }
}
