//! [`NamePolicy`]: how a display name folds into the name key directory
//! lookups compare on. Immutable per namespace.

use serde::{Deserialize, Serialize};
use unicode_casefold::UnicodeCaseFold;
use unicode_normalization::UnicodeNormalization;

/// Selects the immutable rule a namespace uses to derive canonical directory lookup keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NamePolicy {
    /// Normalizes to NFC, applies Unicode default case folding, then normalizes again.
    #[default]
    NfcCasefoldV0,
}

/// Derives the canonical lookup key for a display name under `policy`.
pub fn name_key_for_display_name(policy: NamePolicy, display_name: &str) -> String {
    match policy {
        NamePolicy::NfcCasefoldV0 => display_name
            .nfc()
            .collect::<String>()
            .case_fold()
            .nfc()
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::{name_key_for_display_name, NamePolicy};

    #[test]
    fn nfc_casefold_v0_normalizes_and_casefolds() {
        let decomposed = "Cafe\u{301}.TXT";
        let composed = "CAFÉ.txt";

        let left = name_key_for_display_name(NamePolicy::NfcCasefoldV0, decomposed);
        let right = name_key_for_display_name(NamePolicy::NfcCasefoldV0, composed);

        assert_eq!(left, right);
        assert_eq!(left, "café.txt");
    }
}
