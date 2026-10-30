//! The inode ID type, its spellings, and the root directory's ID.

use crate::id_macros::validation_error;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use thiserror::Error;

/// The identity of a file or directory within a namespace.
///
/// An inode ID is a version 7 UUID, whose top 48 bits are the inode's
/// creation time in milliseconds since the Unix epoch and whose other bits
/// are random, except the root directory's [`ROOT_INODE_ID`], which is all
/// zero bits. An inode keeps its ID when it's renamed, and a fork keeps the
/// IDs of the inodes it inherits. Row keys sort by inode ID, but no rule
/// depends on the order of two IDs.
///
/// An inode ID uses the spelling of every generated ID: `Display` writes the
/// public form, `ino_` and the 26-character body, such as
/// `ino_01k087hae0ep1tzg4zw1ghyvqb`, and [`InodeId::parse`] reads it. Serde
/// writes the body to a human-readable format and the 16 big-endian bytes to
/// a binary format, and a public API field uses [`public`] to write the
/// public form instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InodeId(pub u128);

/// The ID of every namespace's root directory: all zero bits.
pub const ROOT_INODE_ID: InodeId = InodeId(0);

/// The prefix of every public inode ID.
const PREFIX: &str = "ino_";

/// The OpenAPI pattern for public inode IDs.
pub const PATTERN: &str = r"^ino_[0-7][0-9a-hjkmnp-tv-z]{25}$";

/// The public inode ID that OpenAPI shows as an example.
pub const EXAMPLE: &str = "ino_01k087hae0ep1tzg4zw1ghyvqb";

/// The OpenAPI description for public inode IDs.
pub const DESCRIPTION: &str =
    "The stable ID of one inode within a namespace. An inode's ID doesn't change.";

const INVALID_REASON: &str =
    "must use `ino_` followed by 26 lowercase Crockford base 32 characters, \
     the first from `0` through `7`";

validation_error!(
    InodeIdValidationError,
    "invalid inode id {value:?}: {reason}"
);

impl InodeId {
    /// Generates the ID of an inode created at `created_at_ms`, which is
    /// never [`ROOT_INODE_ID`] because a version 7 UUID has nonzero version
    /// bits.
    pub fn generate(created_at_ms: u64) -> Self {
        Self(crate::ids::random_uuid_v7(created_at_ms))
    }

    /// Parses a public inode ID, `ino_` followed by the 26-character body.
    pub fn parse(value: &str) -> Result<Self, InodeIdValidationError> {
        value
            .strip_prefix(PREFIX)
            .and_then(Self::parse_body)
            .ok_or_else(|| InodeIdValidationError::new(value, INVALID_REASON))
    }

    /// Builds an inode ID from its 16 bytes, most significant first.
    pub const fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(bytes))
    }

    /// Returns the ID's 16 bytes, most significant first.
    pub const fn to_be_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    /// Returns the ID's 26-character body, the spelling that every generated
    /// ID uses after its prefix, and the spelling that row keys use.
    ///
    /// Every body has the same width, and its alphabet is in byte order, so
    /// the byte order of two bodies is the numeric order of the IDs.
    pub fn body(self) -> impl fmt::Display {
        InodeIdBody(self.0)
    }

    /// Parses an inode ID from its 26-character body, with the validation
    /// that every generated ID body gets.
    pub fn parse_body(body: &str) -> Option<Self> {
        crate::ids::decode_generated_id_body(body).map(Self)
    }
}

/// Writes the 26-character body of one inode ID.
struct InodeIdBody(u128);

impl fmt::Display for InodeIdBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::ids::encode_generated_id_body(self.0))
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{PREFIX}{}", self.body())
    }
}

impl Serialize for InodeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(&self.body())
        } else {
            serializer.serialize_bytes(&self.to_be_bytes())
        }
    }
}

impl<'de> Deserialize<'de> for InodeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(InodeIdVisitor)
        } else {
            deserializer.deserialize_bytes(InodeIdVisitor)
        }
    }
}

/// Decodes an inode ID from its body or its 16 bytes.
//
// A JSON document stores the body, and a CBOR record stores the 16 bytes.
// One visitor decodes both because serde buffers the fields of a tagged enum,
// and the buffer reports every format as human readable, so a CBOR record's
// 16 bytes can reach this visitor through `deserialize_str`.
struct InodeIdVisitor;

impl Visitor<'_> for InodeIdVisitor {
    type Value = InodeId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an inode ID as its 26-character body or its 16 bytes")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<InodeId, E> {
        InodeId::parse_body(value)
            .ok_or_else(|| E::invalid_value(de::Unexpected::Str(value), &self))
    }

    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<InodeId, E> {
        <[u8; 16]>::try_from(value)
            .map(InodeId::from_be_bytes)
            .map_err(|_| E::invalid_length(value.len(), &self))
    }
}

#[cfg(feature = "openapi")]
#[allow(
    deprecated,
    reason = "the published schema uses the requested singular example field"
)]
impl utoipa::PartialSchema for InodeId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::String)
            .pattern(Some(PATTERN))
            .example(Some(serde_json::json!(EXAMPLE)))
            .description(Some(DESCRIPTION))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for InodeId {}

/// Serde support for public API fields, which write an inode ID's public
/// form, such as `ino_01k087hae0ep1tzg4zw1ghyvqb`.
pub mod public {
    use super::InodeId;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serializes an inode ID as its public form.
    pub fn serialize<S: Serializer>(id: &InodeId, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(id)
    }

    /// Deserializes an inode ID from its public form.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<InodeId, D::Error> {
        let value = String::deserialize(deserializer)?;
        InodeId::parse(&value).map_err(serde::de::Error::custom)
    }

    /// Serde support for optional public API inode ID fields.
    pub mod option {
        use super::InodeId;
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        /// Serializes an optional inode ID as its public form.
        pub fn serialize<S: Serializer>(
            id: &Option<InodeId>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            id.map(|id| id.to_string()).serialize(serializer)
        }

        /// Deserializes an optional inode ID from its public form.
        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<InodeId>, D::Error> {
            Option::<String>::deserialize(deserializer)?
                .map(|value| InodeId::parse(&value))
                .transpose()
                .map_err(serde::de::Error::custom)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InodeId, EXAMPLE, INVALID_REASON, ROOT_INODE_ID};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeSet;

    /// The ID that [`EXAMPLE`] spells, generated at 2025-07-15T23:43:20Z.
    const SAMPLE: InodeId = InodeId(0x0198_1078_a9c0_7583_afc0_9fe0_611f_6eeb);

    #[test]
    fn generated_ids_are_version_7_uuids_that_begin_with_the_creation_time() {
        let created_at_ms = 1_752_623_000_000;
        let ids: BTreeSet<InodeId> = (0..1_000)
            .map(|_| InodeId::generate(created_at_ms))
            .collect();
        assert_eq!(ids.len(), 1_000, "IDs generated in one millisecond differ");
        for id in &ids {
            let bytes = id.to_be_bytes();
            assert_eq!(id.0 >> 80, u128::from(created_at_ms), "{id}");
            assert_eq!(bytes[6] >> 4, 7, "version 7: {id}");
            assert_eq!(bytes[8] >> 6, 0b10, "RFC 9562 variant: {id}");
        }
        let later = InodeId::generate(created_at_ms + 1);
        assert!(
            ids.iter().all(|id| *id < later),
            "a later millisecond sorts after"
        );
        assert!(!ids.contains(&ROOT_INODE_ID));
        assert_ne!(InodeId::generate(0), ROOT_INODE_ID);
        assert_eq!(
            InodeId::generate(u64::MAX).0 >> 80,
            (1 << 48) - 1,
            "only the low 48 bits of the time are kept"
        );
    }

    #[test]
    fn spellings_round_trip_and_keep_numeric_order() {
        assert_eq!(SAMPLE.to_string(), EXAMPLE);
        assert_eq!(SAMPLE.body().to_string(), "01k087hae0ep1tzg4zw1ghyvqb");
        assert_eq!(SAMPLE.0 >> 80, 1_752_623_000_000);
        assert_eq!(SAMPLE.to_be_bytes()[..2], [0x01, 0x98], "big-endian bytes");
        assert_eq!(InodeId::from_be_bytes(SAMPLE.to_be_bytes()), SAMPLE);
        for (id, public) in [
            (ROOT_INODE_ID, "ino_00000000000000000000000000"),
            (SAMPLE, EXAMPLE),
            (InodeId(u128::MAX), "ino_7zzzzzzzzzzzzzzzzzzzzzzzzz"),
        ] {
            assert_eq!(id.to_string(), public);
            assert_eq!(InodeId::parse(public), Ok(id));
            assert_eq!(InodeId::parse_body(&public[4..]), Some(id));
        }

        let ids = [
            InodeId(9),
            InodeId(u128::MAX),
            ROOT_INODE_ID,
            InodeId(1 << 64),
            SAMPLE,
        ];
        let mut by_body: Vec<String> = ids.iter().map(|id| id.body().to_string()).collect();
        by_body.sort();
        let mut by_value = ids;
        by_value.sort();
        assert_eq!(
            by_body,
            by_value
                .iter()
                .map(|id| id.body().to_string())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn parse_applies_the_generated_id_rules() {
        for body in [
            "",
            "01k087hae0ep1tzg4zw1ghyvq",
            "01k087hae0ep1tzg4zw1ghyvqb0",
            "01K087HAE0EP1TZG4ZW1GHYVQB",
            "81k087hae0ep1tzg4zw1ghyvqb",
            "01k087hae0ep1tzg4zw1ghyvqi",
            "ino_01k087hae0ep1tzg4zw1gh",
        ] {
            assert_eq!(InodeId::parse_body(body), None, "{body:?}");
        }
        for value in [
            "ino_42",
            "ino_01k087hae0ep1tzg4zw1ghyvq",
            "ino_01k087hae0ep1tzg4zw1ghyvqb0",
            "ino_01981078a9c07583afc09fe0611f6eeb",
            "ino_01K087HAE0EP1TZG4ZW1GHYVQB",
            "ino_81k087hae0ep1tzg4zw1ghyvqb",
            "ino_01k087hae0ep1tzg4zw1ghyvqi",
            "INO_01k087hae0ep1tzg4zw1ghyvqb",
            "Ino_01k087hae0ep1tzg4zw1ghyvqb",
            "01k087hae0ep1tzg4zw1ghyvqb",
            "ino_",
            " ino_01k087hae0ep1tzg4zw1ghyvqb",
            "ino_01k087hae0ep1tzg4zw1ghyvqb ",
            "",
        ] {
            let error = InodeId::parse(value).expect_err(value);
            assert_eq!(error.value(), value);
            assert_eq!(error.reason(), INVALID_REASON);
            assert_eq!(
                error.to_string(),
                format!("invalid inode id {value:?}: {INVALID_REASON}")
            );
        }
    }

    #[test]
    fn serde_writes_the_body_to_json_and_bytes_to_cbor() {
        let json = serde_json::to_string(&SAMPLE).expect("encode JSON");
        assert_eq!(json, r#""01k087hae0ep1tzg4zw1ghyvqb""#);
        assert_eq!(
            serde_json::from_str::<InodeId>(&json).expect("decode JSON"),
            SAMPLE
        );
        serde_json::from_str::<InodeId>("42").expect_err("a JSON integer is not an inode ID");
        serde_json::from_str::<InodeId>(&format!(r#""{EXAMPLE}""#))
            .expect_err("the durable spelling has no prefix");

        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&SAMPLE, &mut cbor).expect("encode CBOR");
        assert_eq!(cbor.len(), 17, "a 16-byte CBOR byte string and its header");
        assert_eq!(cbor[0], 0x50);
        assert_eq!(&cbor[1..], &SAMPLE.to_be_bytes());
        assert_eq!(
            ciborium::de::from_reader::<InodeId, _>(cbor.as_slice()).expect("decode CBOR"),
            SAMPLE
        );
        let mut integer = Vec::new();
        ciborium::ser::into_writer(&42_u64, &mut integer).expect("encode integer");
        ciborium::de::from_reader::<InodeId, _>(integer.as_slice())
            .expect_err("a CBOR integer is not an inode ID");
    }

    #[test]
    fn tagged_enums_decode_the_binary_form() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum Tagged {
            Row { inode_id: InodeId },
        }

        let row = Tagged::Row {
            inode_id: InodeId(7),
        };
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&row, &mut cbor).expect("encode CBOR");
        assert_eq!(
            ciborium::de::from_reader::<Tagged, _>(cbor.as_slice()).expect("decode CBOR"),
            row
        );
        let json = serde_json::to_string(&row).expect("encode JSON");
        assert_eq!(
            json,
            r#"{"kind":"row","inode_id":"00000000000000000000000007"}"#
        );
        assert_eq!(
            serde_json::from_str::<Tagged>(&json).expect("decode JSON"),
            row
        );
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct PublicFields {
        #[serde(with = "super::public")]
        inode_id: InodeId,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "super::public::option"
        )]
        parent_inode_id: Option<InodeId>,
    }

    #[test]
    fn public_fields_use_only_the_public_form() {
        let present = PublicFields {
            inode_id: SAMPLE,
            parent_inode_id: Some(ROOT_INODE_ID),
        };
        let absent = PublicFields {
            inode_id: SAMPLE,
            parent_inode_id: None,
        };
        let present_json = format!(
            r#"{{"inode_id":"{EXAMPLE}","parent_inode_id":"ino_00000000000000000000000000"}}"#
        );
        let absent_json = format!(r#"{{"inode_id":"{EXAMPLE}"}}"#);

        for (value, json) in [(&present, &present_json), (&absent, &absent_json)] {
            assert_eq!(serde_json::to_string(value).expect("encode"), *json);
            assert_eq!(
                serde_json::from_str::<PublicFields>(json).expect("decode"),
                *value
            );
        }
        assert_eq!(
            serde_json::from_str::<PublicFields>(&format!(
                r#"{{"inode_id":"{EXAMPLE}","parent_inode_id":null}}"#
            ))
            .expect("decode a null optional ID"),
            absent
        );
        for json in [
            r#"{"inode_id":42}"#.to_owned(),
            r#"{"inode_id":"01k087hae0ep1tzg4zw1ghyvqb"}"#.to_owned(),
            format!(r#"{{"inode_id":"{EXAMPLE}","parent_inode_id":42}}"#),
            format!(r#"{{"inode_id":"{EXAMPLE}","parent_inode_id":"00000000000000000000000000"}}"#),
        ] {
            serde_json::from_str::<PublicFields>(&json)
                .expect_err("a public field takes only the public form");
        }
    }
}
