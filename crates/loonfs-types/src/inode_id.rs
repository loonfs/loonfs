//! The inode ID type, its spellings, and the root directory's ID.

use crate::id_macros::validation_error;
use crate::ChangeSeq;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::num::NonZeroU32;
use thiserror::Error;

/// The identity of a file or directory within a namespace.
///
/// An inode ID is a 128-bit number. Its high 64 bits are the low 32 bits of
/// the creating commit's `committed_seq` and the inode's create number within
/// that commit, and its low 64 bits are random. The high word orders IDs, and
/// the low word identifies them. The root directory's [`ROOT_INODE_ID`] is
/// all zero bits, and no generated ID is, because create numbers start at 1.
/// An inode keeps its ID when it's renamed, and a fork keeps the IDs of the
/// inodes it inherits. Row keys sort by inode ID, but no rule depends on the
/// order of two IDs.
///
/// An inode ID uses the spelling of every generated ID: `Display` writes the
/// public form, `ino_` and the 26-character body, such as
/// `ino_000000r0000013kje0d47gc8kw`, and [`InodeId::parse`] reads it. Serde
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
pub const EXAMPLE: &str = "ino_000000r0000013kje0d47gc8kw";

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
    /// Generates the ID of the inode with create number `create_no` in the
    /// commit at `committed_seq`.
    ///
    /// The ID's top 32 bits are the low 32 bits of `committed_seq`, so they
    /// wrap after 2^32 commits. The next 32 bits are `create_no`, which is 1
    /// for the first inode that the commit creates and is never 0, so no
    /// generated ID is [`ROOT_INODE_ID`]. The low 64 bits are random.
    pub fn generate(committed_seq: ChangeSeq, create_no: NonZeroU32) -> Self {
        let seq_bits = (u128::from(committed_seq.0) & u128::from(u32::MAX)) << 96;
        let create_no_bits = u128::from(create_no.get()) << 64;
        let random_bits = u128::from(crate::ids::random_u64());
        Self(seq_bits | create_no_bits | random_bits)
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
/// form, such as `ino_000000r0000013kje0d47gc8kw`.
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
    use crate::ChangeSeq;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;

    /// The ID that [`EXAMPLE`] spells, create number 2 in the commit at
    /// sequence 12, with a fixed random word.
    const SAMPLE: InodeId = InodeId(0x0000_000c_0000_0002_39c9_c069_0f06_227c);

    fn create_no(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("a test's create number should be nonzero")
    }

    /// Returns the sequence and the create number in `id`'s high word.
    fn high_word(id: InodeId) -> (u32, u32) {
        let seq = u32::try_from(id.0 >> 96).expect("the top 32 bits fit in a `u32`");
        let create_no = u32::try_from((id.0 >> 64) & u128::from(u32::MAX))
            .expect("32 masked bits fit in a `u32`");
        (seq, create_no)
    }

    #[test]
    fn generated_ids_order_by_commit_then_create_number() {
        let first = InodeId::generate(ChangeSeq(7), create_no(2));
        let next_in_commit = InodeId::generate(ChangeSeq(7), create_no(3));
        let next_commit = InodeId::generate(ChangeSeq(8), NonZeroU32::MIN);
        assert!(first < next_in_commit, "{first} < {next_in_commit}");
        assert!(
            next_in_commit < next_commit,
            "{next_in_commit} < {next_commit}"
        );
    }

    #[test]
    fn the_high_word_records_the_sequence_and_the_create_number() {
        for (committed_seq, number, seq) in [
            (ChangeSeq(1), 1, 1),
            (ChangeSeq(418), 7, 418),
            (ChangeSeq(u64::from(u32::MAX)), u32::MAX, u32::MAX),
            (ChangeSeq((1 << 32) + 5), 2, 5),
        ] {
            let id = InodeId::generate(committed_seq, create_no(number));
            assert_eq!(high_word(id), (seq, number), "{committed_seq}");
        }
    }

    #[test]
    fn ids_generated_for_one_create_number_differ_in_the_random_word() {
        let ids: BTreeSet<InodeId> = (0..1_000)
            .map(|_| InodeId::generate(ChangeSeq(12), NonZeroU32::MIN))
            .collect();
        assert_eq!(ids.len(), 1_000);
        assert!(ids.iter().all(|id| high_word(*id) == (12, 1)));
    }

    #[test]
    fn no_generated_id_is_the_root() {
        for committed_seq in [ChangeSeq(0), ChangeSeq(1), ChangeSeq(1 << 32)] {
            let id = InodeId::generate(committed_seq, NonZeroU32::MIN);
            assert_ne!(id, ROOT_INODE_ID);
            assert_ne!(id.0 >> 64, 0, "{committed_seq}: the high word isn't zero");
        }
    }

    #[test]
    fn spellings_round_trip_and_keep_numeric_order() {
        assert_eq!(SAMPLE.to_string(), EXAMPLE);
        assert_eq!(SAMPLE.body().to_string(), "000000r0000013kje0d47gc8kw");
        assert_eq!(
            SAMPLE.to_be_bytes(),
            [
                0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x39, 0xc9, 0xc0, 0x69, 0x0f, 0x06,
                0x22, 0x7c,
            ],
            "the sequence, the create number, and the random word, most significant first"
        );
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
            "000000r0000013kje0d47gc8k",
            "000000r0000013kje0d47gc8kw0",
            "000000R0000013KJE0D47GC8KW",
            "800000r0000013kje0d47gc8kw",
            "000000r0000013kje0d47gc8ki",
            "ino_000000r0000013kje0d47g",
        ] {
            assert_eq!(InodeId::parse_body(body), None, "{body:?}");
        }
        for value in [
            "ino_42",
            "ino_000000r0000013kje0d47gc8k",
            "ino_000000r0000013kje0d47gc8kw0",
            "ino_0000000c0000000239c9c0690f06227c",
            "ino_000000R0000013KJE0D47GC8KW",
            "ino_800000r0000013kje0d47gc8kw",
            "ino_000000r0000013kje0d47gc8ki",
            "INO_000000r0000013kje0d47gc8kw",
            "Ino_000000r0000013kje0d47gc8kw",
            "000000r0000013kje0d47gc8kw",
            "ino_",
            " ino_000000r0000013kje0d47gc8kw",
            "ino_000000r0000013kje0d47gc8kw ",
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
        assert_eq!(json, r#""000000r0000013kje0d47gc8kw""#);
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
            r#"{"inode_id":"000000r0000013kje0d47gc8kw"}"#.to_owned(),
            format!(r#"{{"inode_id":"{EXAMPLE}","parent_inode_id":42}}"#),
            format!(r#"{{"inode_id":"{EXAMPLE}","parent_inode_id":"00000000000000000000000000"}}"#),
        ] {
            serde_json::from_str::<PublicFields>(&json)
                .expect_err("a public field takes only the public form");
        }
    }
}
