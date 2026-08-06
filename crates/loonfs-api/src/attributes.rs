//! Inode attributes: bounded, validated key/value metadata held against one
//! inode.
//!
//! Attributes belong to the resource, not to the commit that wrote them, and
//! they travel with inode identity: a rename or a move leaves them unchanged.
//! Every size limit here counts logical UTF-8 bytes, so the encoding that
//! carries a map never changes what a caller is allowed to store.

use crate::ids::{numeric_id, string_id, validation_error};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;

/// Maximum attribute key length in UTF-8 bytes.
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 128;
/// Maximum length of one attribute string in UTF-8 bytes. Each member of a
/// string list obeys the same limit.
pub const MAX_ATTRIBUTE_VALUE_BYTES: usize = 4096;
/// Maximum number of members in one string-list value.
pub const MAX_ATTRIBUTE_LIST_MEMBERS: usize = 256;
/// Maximum number of entries in one attribute map.
pub const MAX_ATTRIBUTE_ENTRIES: usize = 100;
/// Maximum total size of one attribute map in logical UTF-8 bytes. The total
/// counts every key's bytes plus every value's bytes, with each list member
/// counted. It excludes encoder framing, so the limit does not move when the
/// map is written as JSON instead of CBOR.
pub const MAX_ATTRIBUTES_TOTAL_BYTES: usize = 65_536;

/// Key prefix reserved for system-owned attributes.
pub const RESERVED_ATTRIBUTE_KEY_PREFIX: &str = "loonfs.";

validation_error!(
    AttributeKeyValidationError,
    "invalid attribute key {value:?}: {reason}"
);

string_id! {
    /// Validated name of one inode attribute.
    ///
    /// A key is 1 to 128 UTF-8 bytes and carries no Unicode control
    /// character, which is also what rejects NUL. Keys are compared exactly:
    /// nothing case-folds or normalizes them, so two spellings that differ in
    /// any byte name two different attributes.
    ///
    /// The `loonfs.` prefix is reserved for system-owned attributes. This
    /// type accepts a reserved key, because a durable row has to be able to
    /// carry a system attribute. The write operation is where a caller's
    /// attempt to write a reserved key is rejected.
    AttributeKey,
    error = AttributeKeyValidationError,
    validate = validate_attribute_key
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

/// One attribute value.
///
/// The kind is part of the value's identity. A string and a one-member string
/// list are different values, and reading one never yields the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttributeValue {
    /// One text value.
    String {
        /// The stored text, at most [`MAX_ATTRIBUTE_VALUE_BYTES`] UTF-8 bytes.
        value: String,
    },
    /// An ordered list of text values.
    StringList {
        /// The stored members in written order, at most
        /// [`MAX_ATTRIBUTE_LIST_MEMBERS`] of them, each at most
        /// [`MAX_ATTRIBUTE_VALUE_BYTES`] UTF-8 bytes.
        values: Vec<String>,
    },
}

impl AttributeValue {
    /// Returns this value's logical size in UTF-8 bytes.
    ///
    /// A string counts its own bytes and a string list counts the bytes of
    /// every member. Neither counts encoder framing.
    pub fn logical_bytes(&self) -> usize {
        match self {
            Self::String { value } => value.len(),
            Self::StringList { values } => values.iter().map(String::len).sum(),
        }
    }
}

/// A validated attribute map for one inode.
///
/// Construction and decoding both enforce the same limits: a map holds at
/// most [`MAX_ATTRIBUTE_ENTRIES`] entries, each string and each list member
/// is at most [`MAX_ATTRIBUTE_VALUE_BYTES`] UTF-8 bytes, each list holds at
/// most [`MAX_ATTRIBUTE_LIST_MEMBERS`] members, and the whole map is at most
/// [`MAX_ATTRIBUTES_TOTAL_BYTES`] logical UTF-8 bytes. The total counts key
/// bytes and value bytes and nothing else, so it does not depend on the
/// encoding the map is written in. Durable state that breaks a limit fails to
/// decode rather than decoding to something smaller.
///
/// An empty map is valid. It is the cleared state, and clearing an inode's
/// attributes is a real update with its own revision.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
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
        for (key, value) in &entries {
            match value {
                AttributeValue::String { value } => {
                    check_value_bytes(key, value)?;
                }
                AttributeValue::StringList { values } => {
                    if values.len() > MAX_ATTRIBUTE_LIST_MEMBERS {
                        return Err(AttributesError::TooManyListMembers {
                            key: key.as_str().to_owned(),
                            members: values.len(),
                        });
                    }
                    for member in values {
                        check_value_bytes(key, member)?;
                    }
                }
            }
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

fn check_value_bytes(key: &AttributeKey, value: &str) -> Result<(), AttributesError> {
    if value.len() > MAX_ATTRIBUTE_VALUE_BYTES {
        return Err(AttributesError::ValueTooLarge {
            key: key.as_str().to_owned(),
            value_bytes: value.len(),
        });
    }
    Ok(())
}

fn logical_bytes_of(entries: &BTreeMap<AttributeKey, AttributeValue>) -> usize {
    entries
        .iter()
        .map(|(key, value)| key.as_str().len() + value.logical_bytes())
        .sum()
}

/// Describes which attribute-map limit an input broke.
///
/// The variants that name a key echo a value that is already validated: a
/// key is at most [`MAX_ATTRIBUTE_KEY_BYTES`] bytes and holds no control
/// characters, so naming it in a message that reaches the wire is bounded and
/// safe.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AttributesError {
    /// The map holds more entries than the limit allows.
    #[error("attribute map holds {entries} entries, which exceeds the maximum of {MAX_ATTRIBUTE_ENTRIES}")]
    TooManyEntries {
        /// Number of entries the rejected map held.
        entries: usize,
    },
    /// One string, or one member of one string list, is too long.
    #[error("attribute `{key}` holds a {value_bytes} byte string, which exceeds the maximum of {MAX_ATTRIBUTE_VALUE_BYTES} bytes")]
    ValueTooLarge {
        /// Key whose value was rejected.
        key: String,
        /// Length in UTF-8 bytes of the rejected string.
        value_bytes: usize,
    },
    /// One string list holds more members than the limit allows.
    #[error("attribute `{key}` holds {members} list members, which exceeds the maximum of {MAX_ATTRIBUTE_LIST_MEMBERS}")]
    TooManyListMembers {
        /// Key whose list was rejected.
        key: String,
        /// Number of members the rejected list held.
        members: usize,
    },
    /// The whole map is larger than the limit allows.
    #[error("attribute map holds {total_bytes} logical bytes, which exceeds the maximum of {MAX_ATTRIBUTES_TOTAL_BYTES} bytes")]
    TooLarge {
        /// Total logical size in UTF-8 bytes of the rejected map.
        total_bytes: usize,
    },
}

numeric_id! {
    /// Monotonically increasing attribute revision counter within one inode.
    ///
    /// Every inode starts at revision 0 with an empty attribute map, and every
    /// effective update — one that changes the map — advances the counter by
    /// one. The counter is the optimistic-concurrency token for attribute
    /// writes: a writer states the revision it expects, and the write fails
    /// when the inode has moved past it. The counter is not an index into a
    /// history. LoonFS keeps the current map and this number, and promises no
    /// queryable record of earlier maps.
    AttributeRevisionNo
}

#[cfg(test)]
mod tests {
    use super::{
        AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, AttributesError,
        MAX_ATTRIBUTES_TOTAL_BYTES, MAX_ATTRIBUTE_ENTRIES, MAX_ATTRIBUTE_KEY_BYTES,
        MAX_ATTRIBUTE_LIST_MEMBERS, MAX_ATTRIBUTE_VALUE_BYTES,
    };
    use crate::RevisionNo;
    use std::collections::BTreeMap;

    fn key(value: &str) -> AttributeKey {
        AttributeKey::parse(value).expect("valid attribute key")
    }

    fn string_value(bytes: usize) -> AttributeValue {
        AttributeValue::String {
            value: "v".repeat(bytes),
        }
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
    fn attribute_value_serializes_with_a_kind_tag() {
        let string = AttributeValue::String {
            value: "hello".to_owned(),
        };
        let list = AttributeValue::StringList {
            values: vec!["a".to_owned(), "b".to_owned()],
        };

        assert_eq!(
            serde_json::to_string(&string).expect("serialize string value"),
            r#"{"kind":"string","value":"hello"}"#
        );
        assert_eq!(
            serde_json::to_string(&list).expect("serialize string list value"),
            r#"{"kind":"string_list","values":["a","b"]}"#
        );
        assert_eq!(
            serde_json::from_str::<AttributeValue>(r#"{"kind":"string","value":"hello"}"#)
                .expect("deserialize string value"),
            string
        );
        assert_eq!(
            serde_json::from_str::<AttributeValue>(r#"{"kind":"string_list","values":["a","b"]}"#)
                .expect("deserialize string list value"),
            list
        );
    }

    #[test]
    fn attribute_value_rejects_unknown_kinds_and_unknown_fields() {
        assert!(
            serde_json::from_str::<AttributeValue>(r#"{"kind":"number","value":"1"}"#).is_err()
        );
        assert!(serde_json::from_str::<AttributeValue>(
            r#"{"kind":"string","value":"a","extra":1}"#
        )
        .is_err());
        assert!(serde_json::from_str::<AttributeValue>(
            r#"{"kind":"string_list","values":[],"extra":1}"#
        )
        .is_err());
        // The wrong variant's field is an unknown field too.
        assert!(
            serde_json::from_str::<AttributeValue>(r#"{"kind":"string","values":[]}"#).is_err()
        );
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
    fn attribute_map_enforces_the_string_length() {
        assert_eq!(
            map([(key("a"), string_value(MAX_ATTRIBUTE_VALUE_BYTES))]).len(),
            1
        );
        assert_eq!(
            map_error([(key("a"), string_value(MAX_ATTRIBUTE_VALUE_BYTES + 1))]),
            AttributesError::ValueTooLarge {
                key: "a".to_owned(),
                value_bytes: MAX_ATTRIBUTE_VALUE_BYTES + 1
            }
        );
    }

    #[test]
    fn attribute_map_enforces_the_list_member_count() {
        let at_cap = AttributeValue::StringList {
            values: vec!["m".to_owned(); MAX_ATTRIBUTE_LIST_MEMBERS],
        };
        let over_cap = AttributeValue::StringList {
            values: vec!["m".to_owned(); MAX_ATTRIBUTE_LIST_MEMBERS + 1],
        };

        assert_eq!(map([(key("a"), at_cap)]).len(), 1);
        assert_eq!(
            map_error([(key("a"), over_cap)]),
            AttributesError::TooManyListMembers {
                key: "a".to_owned(),
                members: MAX_ATTRIBUTE_LIST_MEMBERS + 1
            }
        );
    }

    #[test]
    fn attribute_map_enforces_the_list_member_length() {
        let at_cap = AttributeValue::StringList {
            values: vec!["m".repeat(MAX_ATTRIBUTE_VALUE_BYTES)],
        };
        let over_cap = AttributeValue::StringList {
            values: vec!["m".to_owned(), "m".repeat(MAX_ATTRIBUTE_VALUE_BYTES + 1)],
        };

        assert_eq!(map([(key("a"), at_cap)]).len(), 1);
        assert_eq!(
            map_error([(key("a"), over_cap)]),
            AttributesError::ValueTooLarge {
                key: "a".to_owned(),
                value_bytes: MAX_ATTRIBUTE_VALUE_BYTES + 1
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
        let over_value = serde_json::to_string(&BTreeMap::from([(
            "a".to_owned(),
            string_value(MAX_ATTRIBUTE_VALUE_BYTES + 1),
        )]))
        .expect("serialize oversized value");
        let over_members = serde_json::to_string(&BTreeMap::from([(
            "a".to_owned(),
            AttributeValue::StringList {
                values: vec!["m".to_owned(); MAX_ATTRIBUTE_LIST_MEMBERS + 1],
            },
        )]))
        .expect("serialize oversized list");

        assert!(serde_json::from_str::<Attributes>(&over_entries).is_err());
        assert!(serde_json::from_str::<Attributes>(&over_value).is_err());
        assert!(serde_json::from_str::<Attributes>(&over_members).is_err());
        // The key grammar is enforced on the way in as well.
        assert!(
            serde_json::from_str::<Attributes>(r#"{"":{"kind":"string","value":"a"}}"#).is_err()
        );
    }

    #[test]
    fn attribute_map_round_trips_and_reads_back() {
        let attributes = map([
            (key("a"), string_value(3)),
            (
                key("b"),
                AttributeValue::StringList {
                    values: vec!["x".to_owned()],
                },
            ),
        ]);

        let json = serde_json::to_string(&attributes).expect("serialize attributes");
        assert_eq!(
            json,
            r#"{"a":{"kind":"string","value":"vvv"},"b":{"kind":"string_list","values":["x"]}}"#
        );
        assert_eq!(
            serde_json::from_str::<Attributes>(&json).expect("deserialize attributes"),
            attributes
        );
        assert_eq!(attributes.get(&key("a")), Some(&string_value(3)));
        assert_eq!(attributes.get(&key("missing")), None);
        assert_eq!(attributes.iter().count(), 2);
        assert_eq!(attributes.as_map().len(), 2);
        assert_eq!(attributes.logical_bytes(), 1 + 3 + 1 + 1);
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
