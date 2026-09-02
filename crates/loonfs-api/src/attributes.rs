//! Validated key-value attributes stored on inodes.

use crate::ids::{numeric_id, string_id, validation_error};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;

/// Maximum attribute key length in UTF-8 bytes.
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 128;
/// Maximum length of one attribute value in UTF-8 bytes.
pub const MAX_ATTRIBUTE_VALUE_BYTES: usize = 4096;
/// Maximum number of entries in one attribute map.
pub const MAX_ATTRIBUTE_ENTRIES: usize = 100;
/// Maximum total size of one attribute map in logical UTF-8 bytes. The total
/// counts every key's bytes plus every value's bytes. It excludes encoder
/// framing, so the limit does not move when the map is written as JSON instead
/// of CBOR.
pub const MAX_ATTRIBUTES_TOTAL_BYTES: usize = 65_536;

/// Key prefix reserved for system-owned attributes.
pub const RESERVED_ATTRIBUTE_KEY_PREFIX: &str = "loonfs.";

validation_error!(
    AttributeKeyValidationError,
    "invalid attribute key {value:?}: {reason}"
);

string_id! {
    /// A validated inode attribute name from 1 to 128 UTF-8 bytes without control characters.
    ///
    /// Names are byte-exact; writes reject the reserved `loonfs.` prefix even though
    /// the type accepts it.
    AttributeKey,
    error = AttributeKeyValidationError,
    validate = validate_attribute_key,
    schema(example = "owner")
}

impl AttributeKey {
    /// Reports whether this key names a system-owned attribute.
    pub fn is_reserved(&self) -> bool {
        self.as_str().starts_with(RESERVED_ATTRIBUTE_KEY_PREFIX)
    }
}

fn validate_attribute_key(value: &str) -> Result<(), AttributeKeyValidationError> {
    if value.is_empty() {
        return Err(attribute_key_error(value, "must not be empty"));
    }
    if value.len() > MAX_ATTRIBUTE_KEY_BYTES {
        // An oversized or hostile key must not ride along in error payloads
        // that serialize onto the wire. The length check runs before the
        // character check so that no oversized key reaches an error that does
        // echo its input.
        return Err(attribute_key_error(
            "",
            format!("exceeds the maximum attribute key length of {MAX_ATTRIBUTE_KEY_BYTES} bytes"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(attribute_key_error(
            value,
            "must not contain control characters",
        ));
    }
    Ok(())
}

fn attribute_key_error(value: &str, reason: impl Into<String>) -> AttributeKeyValidationError {
    AttributeKeyValidationError {
        value: value.to_owned(),
        reason: reason.into(),
    }
}

validation_error!(
    AttributeValueValidationError,
    "invalid attribute value: {reason}"
);

string_id! {
    /// A validated inode attribute value of at most [`MAX_ATTRIBUTE_VALUE_BYTES`] UTF-8 bytes.
    ///
    /// Empty strings and control characters are valid, and only an explicit remove
    /// operation deletes an attribute.
    AttributeValue,
    error = AttributeValueValidationError,
    validate = validate_attribute_value,
    schema(example = "platform")
}

fn validate_attribute_value(value: &str) -> Result<(), AttributeValueValidationError> {
    if value.len() > MAX_ATTRIBUTE_VALUE_BYTES {
        return Err(AttributeValueValidationError {
            value: String::new(),
            reason: format!(
                "exceeds the maximum attribute value length of {MAX_ATTRIBUTE_VALUE_BYTES} bytes"
            ),
        });
    }
    Ok(())
}

impl AttributeValue {
    /// Returns this value's logical size in UTF-8 bytes.
    ///
    /// The value counts its own bytes and no encoder framing.
    pub fn logical_bytes(&self) -> usize {
        self.as_str().len()
    }
}

/// A validated attribute map limited to [`MAX_ATTRIBUTE_ENTRIES`] entries and
/// [`MAX_ATTRIBUTES_TOTAL_BYTES`] total key and value UTF-8 bytes.
///
/// Construction and decoding reject values over these limits; an empty map
/// represents cleared attributes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "openapi",
    schema(value_type = std::collections::BTreeMap<String, AttributeValue>)
)]
#[serde(transparent)]
pub struct Attributes(BTreeMap<AttributeKey, AttributeValue>);

impl Attributes {
    /// Creates a map after checking every limit.
    pub fn new(entries: BTreeMap<AttributeKey, AttributeValue>) -> Result<Self, AttributesError> {
        if entries.len() > MAX_ATTRIBUTE_ENTRIES {
            return Err(AttributesError::TooManyEntries {
                entries: entries.len(),
            });
        }
        let total_bytes = logical_bytes_of(&entries);
        if total_bytes > MAX_ATTRIBUTES_TOTAL_BYTES {
            return Err(AttributesError::TooLarge { total_bytes });
        }
        Ok(Self(entries))
    }

    /// Returns the value stored under a key.
    pub fn get(&self, key: &AttributeKey) -> Option<&AttributeValue> {
        self.0.get(key)
    }

    /// Iterates the entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&AttributeKey, &AttributeValue)> {
        self.0.iter()
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Reports whether the map holds no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the entries.
    pub fn as_map(&self) -> &BTreeMap<AttributeKey, AttributeValue> {
        &self.0
    }

    /// Returns the map's total logical size in UTF-8 bytes.
    pub fn logical_bytes(&self) -> usize {
        logical_bytes_of(&self.0)
    }
}

impl TryFrom<BTreeMap<AttributeKey, AttributeValue>> for Attributes {
    type Error = AttributesError;

    fn try_from(entries: BTreeMap<AttributeKey, AttributeValue>) -> Result<Self, Self::Error> {
        Self::new(entries)
    }
}

impl From<Attributes> for BTreeMap<AttributeKey, AttributeValue> {
    fn from(attributes: Attributes) -> Self {
        attributes.0
    }
}

impl<'de> Deserialize<'de> for Attributes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let entries = BTreeMap::<AttributeKey, AttributeValue>::deserialize(deserializer)?;
        Self::new(entries).map_err(serde::de::Error::custom)
    }
}

fn logical_bytes_of(entries: &BTreeMap<AttributeKey, AttributeValue>) -> usize {
    entries
        .iter()
        .map(|(key, value)| key.as_str().len() + value.logical_bytes())
        .sum()
}

/// Describes which attribute-map limit an input broke.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AttributesError {
    /// The map holds more entries than the limit allows.
    #[error("attribute map holds {entries} entries, which exceeds the maximum of {MAX_ATTRIBUTE_ENTRIES}")]
    TooManyEntries {
        /// Number of entries the rejected map held.
        entries: usize,
    },
    /// The whole map is larger than the limit allows.
    #[error("attribute map holds {total_bytes} logical bytes, which exceeds the maximum of {MAX_ATTRIBUTES_TOTAL_BYTES} bytes")]
    TooLarge {
        /// Total logical size in UTF-8 bytes of the rejected map.
        total_bytes: usize,
    },
}

numeric_id! {
    /// The revision number for an inode's attribute map.
    ///
    /// It starts at zero and increases when the map changes; clients can guard
    /// updates but cannot query earlier maps.
    AttributeRevisionNo,
    public_ordinal,
    schema_description = "Revision number for an inode's attributes. It starts at 0 and increases whenever the attribute map changes."
}

#[cfg(test)]
mod tests {
    use super::{
        AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, AttributesError,
        MAX_ATTRIBUTES_TOTAL_BYTES, MAX_ATTRIBUTE_ENTRIES, MAX_ATTRIBUTE_KEY_BYTES,
        MAX_ATTRIBUTE_VALUE_BYTES,
    };
    use crate::RevisionNo;
    use std::collections::BTreeMap;

    fn key(value: &str) -> AttributeKey {
        AttributeKey::parse(value).expect("valid attribute key")
    }

    fn string_value(bytes: usize) -> AttributeValue {
        AttributeValue::parse("v".repeat(bytes)).expect("valid attribute value")
    }

    fn value(value: &str) -> AttributeValue {
        AttributeValue::parse(value).expect("valid attribute value")
    }

    fn map(entries: impl IntoIterator<Item = (AttributeKey, AttributeValue)>) -> Attributes {
        Attributes::new(entries.into_iter().collect()).expect("valid attribute map")
    }

    fn map_error(
        entries: impl IntoIterator<Item = (AttributeKey, AttributeValue)>,
    ) -> AttributesError {
        Attributes::new(entries.into_iter().collect()).expect_err("invalid attribute map")
    }

    #[test]
    fn attribute_key_accepts_the_allowed_grammar() {
        for value in [
            "a",
            &"k".repeat(MAX_ATTRIBUTE_KEY_BYTES),
            "user.tag",
            "Case.Sensitive",
        ] {
            assert_eq!(key(value).as_str(), value);
        }
    }

    #[test]
    fn attribute_key_counts_length_in_utf8_bytes() {
        // Four bytes per character, so 32 characters is exactly the cap and
        // 33 is over it.
        let at_cap = "🐧".repeat(MAX_ATTRIBUTE_KEY_BYTES / 4);
        assert_eq!(at_cap.len(), MAX_ATTRIBUTE_KEY_BYTES);
        assert_eq!(key(&at_cap).as_str(), at_cap);
        assert!(AttributeKey::parse(format!("{at_cap}🐧")).is_err());
    }

    #[test]
    fn attribute_key_rejects_invalid_values() {
        assert_eq!(
            AttributeKey::parse("").expect_err("empty").reason(),
            "must not be empty"
        );
        assert_eq!(
            AttributeKey::parse("a\u{0}b").expect_err("nul").reason(),
            "must not contain control characters"
        );
        assert_eq!(
            AttributeKey::parse("a\u{7}b")
                .expect_err("control")
                .reason(),
            "must not contain control characters"
        );
    }

    #[test]
    fn attribute_key_over_length_error_does_not_echo_the_key() {
        let oversized = "k".repeat(MAX_ATTRIBUTE_KEY_BYTES + 1);
        let error = AttributeKey::parse(&oversized).expect_err("over cap");

        assert_eq!(error.value(), "");
        assert_eq!(
            error.reason(),
            "exceeds the maximum attribute key length of 128 bytes"
        );
        assert!(!error.to_string().contains(&oversized));
    }

    #[test]
    fn attribute_key_accepts_the_reserved_prefix() {
        // The durable format has to carry system attributes, so the type
        // admits them. Rejecting a caller's write of one is the write
        // operation's job.
        let reserved = key("loonfs.kind");

        assert!(reserved.is_reserved());
        assert!(!key("loonfs").is_reserved());
        assert!(!key("user.loonfs.kind").is_reserved());
    }

    #[test]
    fn attribute_value_serializes_as_a_bare_string() {
        let value = value("hello");

        assert_eq!(
            serde_json::to_string(&value).expect("serialize attribute value"),
            r#""hello""#
        );
        assert_eq!(
            serde_json::from_str::<AttributeValue>(r#""hello""#)
                .expect("deserialize attribute value"),
            value
        );
    }

    #[test]
    fn attribute_value_rejects_the_old_tagged_shape() {
        assert!(
            serde_json::from_str::<AttributeValue>(r#"{"kind":"string","value":"hello"}"#).is_err()
        );
        assert!(serde_json::from_str::<AttributeValue>(
            r#"{"kind":"string_list","values":["a","b"]}"#
        )
        .is_err());
    }

    #[test]
    fn attribute_value_accepts_empty_and_free_text() {
        for text in ["", "a\n\u{0}b", "draft,review", "café ☃ 日本語 🙂"] {
            assert_eq!(value(text).as_str(), text);
        }
    }

    #[test]
    fn attribute_value_enforces_the_utf8_byte_cap_with_a_named_error() {
        let at_cap = "🐧".repeat(MAX_ATTRIBUTE_VALUE_BYTES / 4);
        assert_eq!(value(&at_cap).logical_bytes(), MAX_ATTRIBUTE_VALUE_BYTES);

        let oversized = format!("{at_cap}🐧");
        let error = AttributeValue::parse(&oversized).expect_err("over cap");
        assert_eq!(error.value(), "");
        assert_eq!(
            error.reason(),
            "exceeds the maximum attribute value length of 4096 bytes"
        );
        assert!(!error.to_string().contains(&oversized));
    }

    #[test]
    fn attribute_map_enforces_the_entry_count() {
        let at_cap: Vec<_> = (0..MAX_ATTRIBUTE_ENTRIES)
            .map(|index| (key(&format!("k{index}")), string_value(1)))
            .collect();
        let over_cap: Vec<_> = (0..MAX_ATTRIBUTE_ENTRIES + 1)
            .map(|index| (key(&format!("k{index}")), string_value(1)))
            .collect();

        assert_eq!(map(at_cap).len(), MAX_ATTRIBUTE_ENTRIES);
        assert_eq!(
            map_error(over_cap),
            AttributesError::TooManyEntries {
                entries: MAX_ATTRIBUTE_ENTRIES + 1
            }
        );
    }

    #[test]
    fn attribute_map_enforces_the_total_size() {
        // Sixteen entries of one 4,096-byte string each hold 65,536 value
        // bytes, which is over the total before any key is counted. Dropping
        // one entry brings the same map back under it.
        let entries: Vec<_> = (0..16)
            .map(|index| {
                (
                    key(&format!("k{index:02}")),
                    string_value(MAX_ATTRIBUTE_VALUE_BYTES),
                )
            })
            .collect();
        let smaller: Vec<_> = entries.iter().skip(1).cloned().collect();

        assert_eq!(
            map_error(entries),
            AttributesError::TooLarge {
                total_bytes: 16 * (MAX_ATTRIBUTE_VALUE_BYTES + "k00".len())
            }
        );
        assert_eq!(map(smaller).len(), 15);
    }

    #[test]
    fn attribute_map_total_counts_key_bytes() {
        // These sixteen entries sit exactly 128 bytes under the total, so one
        // more entry fits only if its 128-byte key is not counted. It is
        // counted, so the map is rejected.
        let entries: Vec<_> = (0..16)
            .map(|index| {
                (
                    key(&format!("k{index:02}")),
                    string_value((MAX_ATTRIBUTES_TOTAL_BYTES - MAX_ATTRIBUTE_KEY_BYTES) / 16 - 3),
                )
            })
            .collect();
        let fits = map(entries.clone());
        assert_eq!(
            fits.logical_bytes(),
            MAX_ATTRIBUTES_TOTAL_BYTES - MAX_ATTRIBUTE_KEY_BYTES
        );

        let mut with_long_key: Vec<_> = entries;
        with_long_key.push((key(&"k".repeat(MAX_ATTRIBUTE_KEY_BYTES)), string_value(1)));
        assert_eq!(
            map_error(with_long_key),
            AttributesError::TooLarge {
                total_bytes: MAX_ATTRIBUTES_TOTAL_BYTES + 1
            }
        );
    }

    #[test]
    fn attribute_map_validates_on_deserialize_too() {
        // A durable row that breaks a limit must fail to decode rather than
        // decode to something within the limits.
        let over_entries = serde_json::to_string(
            &(0..MAX_ATTRIBUTE_ENTRIES + 1)
                .map(|index| (format!("k{index}"), string_value(1)))
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("serialize oversized map");
        let over_value = format!(
            r#"{{"a":{}}}"#,
            serde_json::to_string(&"v".repeat(MAX_ATTRIBUTE_VALUE_BYTES + 1))
                .expect("serialize oversized value")
        );

        assert!(serde_json::from_str::<Attributes>(&over_entries).is_err());
        assert!(serde_json::from_str::<Attributes>(&over_value).is_err());
        // The key grammar is enforced on the way in as well.
        assert!(serde_json::from_str::<Attributes>(r#"{"":"a"}"#).is_err());
    }

    #[test]
    fn attribute_map_round_trips_and_reads_back() {
        let attributes = map([
            (key("a"), string_value(3)),
            (key("b"), value("draft,review")),
            (key("empty"), value("")),
        ]);

        let json = serde_json::to_string(&attributes).expect("serialize attributes");
        assert_eq!(json, r#"{"a":"vvv","b":"draft,review","empty":""}"#);
        assert_eq!(
            serde_json::from_str::<Attributes>(&json).expect("deserialize attributes"),
            attributes
        );
        assert_eq!(attributes.get(&key("a")), Some(&string_value(3)));
        assert_eq!(attributes.get(&key("missing")), None);
        assert_eq!(attributes.get(&key("empty")), Some(&value("")));
        assert_eq!(attributes.iter().count(), 3);
        assert_eq!(attributes.as_map().len(), 3);
        assert_eq!(attributes.logical_bytes(), 1 + 3 + 1 + 12 + 5);
        assert_eq!(
            BTreeMap::from(attributes.clone()),
            attributes.as_map().clone()
        );
    }

    #[test]
    fn empty_attribute_map_is_a_valid_state() {
        let empty = Attributes::default();

        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.logical_bytes(), 0);
        assert_eq!(Attributes::new(BTreeMap::new()).expect("empty map"), empty);
        let json = serde_json::to_string(&empty).expect("serialize empty map");
        assert_eq!(json, "{}");
        assert_eq!(
            serde_json::from_str::<Attributes>(&json).expect("deserialize empty map"),
            empty
        );
    }

    #[test]
    fn attribute_revision_no_serializes_like_a_revision_no() {
        let revision = AttributeRevisionNo(7);

        assert_eq!(
            serde_json::to_string(&revision).expect("serialize attribute revision"),
            serde_json::to_string(&RevisionNo(7)).expect("serialize revision")
        );
        assert_eq!(
            serde_json::to_string(&revision).expect("serialize attribute revision"),
            "7"
        );
        assert_eq!(
            serde_json::from_str::<AttributeRevisionNo>("7").expect("deserialize"),
            revision
        );
        assert_eq!(AttributeRevisionNo::from(7), revision);
        assert_eq!(revision.to_string(), "7");
    }
}
