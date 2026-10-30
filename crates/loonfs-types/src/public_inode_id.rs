//! Converts inode IDs to and from their public API strings.
//!
//! A public ID is `ino_` followed by the 26-character body that every
//! generated ID uses, such as `ino_1z5ae0x2rx99qswz2v78egz3kc`.

use crate::id_macros::validation_error;
use crate::InodeId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Prefix used by every public inode ID.
pub(crate) const PREFIX: &str = "ino_";

/// OpenAPI pattern for public inode IDs.
pub const PATTERN: &str = r"^ino_[0-7][0-9a-hjkmnp-tv-z]{25}$";

/// Example public inode ID used in OpenAPI.
pub const EXAMPLE: &str = "ino_1z5ae0x2rx99qswz2v78egz3kc";

/// OpenAPI description for public inode IDs.
pub const DESCRIPTION: &str =
    "The stable ID of one inode within a namespace. An inode's ID doesn't change.";

const INVALID_REASON: &str =
    "must use `ino_` followed by 26 lowercase Crockford base 32 characters, \
                              the first from `0` through `7`";

validation_error!(PublicInodeIdError, "invalid inode id {value:?}: {reason}");

/// Converts an inode ID to its public API string.
pub fn encode(id: InodeId) -> String {
    id.to_string()
}

/// Parses and validates a public inode ID.
pub fn decode(value: &str) -> Result<InodeId, PublicInodeIdError> {
    value
        .strip_prefix(PREFIX)
        .and_then(InodeId::parse_body)
        .ok_or_else(|| PublicInodeIdError::new(value, INVALID_REASON))
}

/// Serializes an inode ID as a public API string.
pub fn serialize<S>(id: &InodeId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    encode(*id).serialize(serializer)
}

/// Deserializes an inode ID from its public API string.
pub fn deserialize<'de, D>(deserializer: D) -> Result<InodeId, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    decode(&value).map_err(serde::de::Error::custom)
}

/// Serde support for optional public inode ID fields.
pub mod option {
    use super::{decode, encode};
    use crate::InodeId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Serializes an optional inode ID as a public API string.
    pub fn serialize<S>(id: &Option<InodeId>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        id.map(encode).serialize(serializer)
    }

    /// Deserializes an optional inode ID from its public API string.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<InodeId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|value| decode(&value))
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_rejected(value: &str) {
        let error = decode(value).expect_err("invalid public inode id");

        assert_eq!(error.value(), value);
        assert_eq!(error.reason(), INVALID_REASON);
        assert_eq!(
            error.to_string(),
            format!("invalid inode id {value:?}: {INVALID_REASON}")
        );
    }

    const SAMPLE: &str = "ino_1z5ae0x2rx99qswz2v78egz3kc";
    const SAMPLE_ID: InodeId = InodeId(0x3f2a_9c0e_8b1d_4a6f_9e7c_5b3a_1d0f_8e6c);

    #[test]
    fn decode_accepts_public_inode_id_grammar() {
        for (value, expected) in [
            ("ino_00000000000000000000000000", crate::ROOT_INODE_ID),
            (SAMPLE, SAMPLE_ID),
            ("ino_7zzzzzzzzzzzzzzzzzzzzzzzzz", InodeId(u128::MAX)),
        ] {
            assert_eq!(decode(value).expect("valid public inode id"), expected);
            assert_eq!(encode(expected), value);
        }
        assert_eq!(EXAMPLE, SAMPLE);
        assert_eq!(decode(EXAMPLE).expect("the example parses"), SAMPLE_ID);
    }

    macro_rules! rejection_test {
        ($name:ident, $value:literal) => {
            #[test]
            fn $name() {
                assert_rejected($value);
            }
        };
    }

    rejection_test!(decode_rejects_a_decimal_suffix, "ino_42");
    rejection_test!(
        decode_rejects_a_short_suffix,
        "ino_1z5ae0x2rx99qswz2v78egz3k"
    );
    rejection_test!(
        decode_rejects_a_long_suffix,
        "ino_1z5ae0x2rx99qswz2v78egz3kc0"
    );
    rejection_test!(
        decode_rejects_a_hex_suffix,
        "ino_3f2a9c0e8b1d4a6f9e7c5b3a1d0f8e6c"
    );
    rejection_test!(
        decode_rejects_an_uppercase_suffix,
        "ino_1Z5AE0X2RX99QSWZ2V78EGZ3KC"
    );
    rejection_test!(
        decode_rejects_a_first_character_above_7,
        "ino_8z5ae0x2rx99qswz2v78egz3kc"
    );
    rejection_test!(
        decode_rejects_a_letter_outside_the_alphabet,
        "ino_1z5ae0x2rx99qswz2v78egz3ki"
    );
    rejection_test!(
        decode_rejects_uppercase_prefix,
        "INO_1z5ae0x2rx99qswz2v78egz3kc"
    );
    rejection_test!(
        decode_rejects_mixed_case_prefix,
        "Ino_1z5ae0x2rx99qswz2v78egz3kc"
    );
    rejection_test!(decode_rejects_missing_prefix, "1z5ae0x2rx99qswz2v78egz3kc");
    rejection_test!(decode_rejects_empty_suffix, "ino_");
    rejection_test!(
        decode_rejects_leading_whitespace,
        " ino_1z5ae0x2rx99qswz2v78egz3kc"
    );
    rejection_test!(
        decode_rejects_trailing_whitespace,
        "ino_1z5ae0x2rx99qswz2v78egz3kc "
    );
    rejection_test!(decode_rejects_empty_string, "");

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct RequiredField {
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct OptionalField {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        inode_id: Option<InodeId>,
    }

    #[test]
    fn serde_adapter_uses_only_the_string_form() {
        let value = RequiredField {
            inode_id: SAMPLE_ID,
        };
        let json = format!(r#"{{"inode_id":"{SAMPLE}"}}"#);

        assert_eq!(
            serde_json::to_string(&value).expect("serialize public inode id"),
            json
        );
        assert_eq!(
            serde_json::from_str::<RequiredField>(&json).expect("deserialize public inode id"),
            value
        );
        serde_json::from_str::<RequiredField>(r#"{"inode_id":42}"#)
            .expect_err("a numeric inode ID should be rejected");
        serde_json::from_str::<RequiredField>(r#"{"inode_id":"1z5ae0x2rx99qswz2v78egz3kc"}"#)
            .expect_err("an inode ID without the prefix should be rejected");
    }

    #[test]
    fn optional_serde_adapter_composes_with_skip_serializing_if() {
        let present = OptionalField {
            inode_id: Some(SAMPLE_ID),
        };
        let absent = OptionalField { inode_id: None };
        let json = format!(r#"{{"inode_id":"{SAMPLE}"}}"#);

        assert_eq!(
            serde_json::to_string(&present).expect("serialize present public inode id"),
            json
        );
        assert_eq!(
            serde_json::to_string(&absent).expect("serialize absent public inode id"),
            "{}"
        );
        assert_eq!(
            serde_json::from_str::<OptionalField>(&json)
                .expect("deserialize present public inode id"),
            present
        );
        assert_eq!(
            serde_json::from_str::<OptionalField>(r#"{"inode_id":null}"#)
                .expect("deserialize null public inode id"),
            absent
        );
        assert_eq!(
            serde_json::from_str::<OptionalField>("{}").expect("deserialize missing inode id"),
            absent
        );
        serde_json::from_str::<OptionalField>(r#"{"inode_id":42}"#)
            .expect_err("a numeric inode ID should be rejected");
    }
}
