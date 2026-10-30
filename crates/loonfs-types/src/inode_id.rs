//! The inode ID type, its spellings, and the root directory's ID.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// The identity of a file or directory within a namespace.
///
/// An inode ID is a random version 4 UUID, except the root directory's
/// [`ROOT_INODE_ID`], which is all zero bits. An inode keeps its ID when it's
/// renamed, and a fork keeps the IDs of the inodes it inherits. The numeric
/// order of two IDs means nothing beyond being a stable order.
///
/// An inode ID uses the spelling of every generated ID: `Display` writes the
/// public form, `ino_` and the 26-character body, such as
/// `ino_1z5ae0x2rx99qswz2v78egz3kc`. Serde writes the body to a
/// human-readable format and the 16 big-endian bytes to a binary format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InodeId(pub u128);

/// The ID of every namespace's root directory: all zero bits.
pub const ROOT_INODE_ID: InodeId = InodeId(0);

impl InodeId {
    /// Generates a random inode ID, which is never [`ROOT_INODE_ID`] because
    /// a version 4 UUID has nonzero version bits.
    pub fn generate() -> Self {
        // Benchmark variant: the top 48 bits are the Unix time in
        // milliseconds and the remaining 80 bits are random, so IDs sort by
        // creation time while staying unique across namespaces.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u128)
            .unwrap_or(1);
        let random = crate::ids::random_uuid_v4() & ((1_u128 << 80) - 1);
        Self((now_ms << 80) | random)
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
        write!(f, "{}{}", crate::public_inode_id::PREFIX, self.body())
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
// Each container has one encoding, and the visitor accepts both only because
// serde's buffered decoding of a tagged enum hides `is_human_readable`: the
// buffer reports every format as human readable, so a CBOR record's 16 bytes
// reach `deserialize_str` and arrive here as bytes. It isn't a compatibility
// path.
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
            .pattern(Some(crate::public_inode_id::PATTERN))
            .example(Some(serde_json::json!(crate::public_inode_id::EXAMPLE)))
            .description(Some(crate::public_inode_id::DESCRIPTION))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for InodeId {}

#[cfg(test)]
mod tests {
    use super::{InodeId, ROOT_INODE_ID};
    use std::collections::BTreeSet;

    const SAMPLE: InodeId = InodeId(0x3f2a_9c0e_8b1d_4a6f_9e7c_5b3a_1d0f_8e6c);

    #[test]
    fn generated_ids_are_distinct_version_4_uuids() {
        let ids: BTreeSet<InodeId> = (0..1_000).map(|_| InodeId::generate()).collect();
        assert_eq!(ids.len(), 1_000);
        assert!(!ids.contains(&ROOT_INODE_ID));
        for id in ids {
            let bytes = id.to_be_bytes();
            assert_eq!(bytes[6] >> 4, 4, "version 4");
            assert_eq!(bytes[8] >> 6, 0b10, "RFC 4122 variant");
        }
    }

    #[test]
    fn spellings_round_trip_and_keep_numeric_order() {
        assert_eq!(SAMPLE.to_string(), "ino_1z5ae0x2rx99qswz2v78egz3kc");
        assert_eq!(SAMPLE.body().to_string(), "1z5ae0x2rx99qswz2v78egz3kc");
        assert_eq!(
            InodeId::parse_body(&SAMPLE.body().to_string()),
            Some(SAMPLE)
        );
        assert_eq!(ROOT_INODE_ID.to_string(), "ino_00000000000000000000000000");
        assert_eq!(
            InodeId(u128::MAX).body().to_string(),
            "7zzzzzzzzzzzzzzzzzzzzzzzzz"
        );
        assert_eq!(InodeId::from_be_bytes(SAMPLE.to_be_bytes()), SAMPLE);
        assert_eq!(SAMPLE.to_be_bytes()[0], 0x3f, "the byte form is big-endian");

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
    fn parse_body_applies_the_generated_id_rules() {
        for value in [
            "",
            "1z5ae0x2rx99qswz2v78egz3k",
            "1z5ae0x2rx99qswz2v78egz3kc0",
            "1Z5AE0X2RX99QSWZ2V78EGZ3KC",
            "8z5ae0x2rx99qswz2v78egz3kc",
            "1z5ae0x2rx99qswz2v78egz3ki",
            "ino_1z5ae0x2rx99qswz2v78egz3k",
        ] {
            assert_eq!(InodeId::parse_body(value), None, "{value:?}");
        }
    }

    #[test]
    fn serde_writes_the_body_to_json_and_bytes_to_cbor() {
        let json = serde_json::to_string(&SAMPLE).expect("encode JSON");
        assert_eq!(json, r#""1z5ae0x2rx99qswz2v78egz3kc""#);
        assert_eq!(
            serde_json::from_str::<InodeId>(&json).expect("decode JSON"),
            SAMPLE
        );
        serde_json::from_str::<InodeId>("42").expect_err("a JSON integer is not an inode ID");
        serde_json::from_str::<InodeId>(r#""ino_1z5ae0x2rx99qswz2v78egz3kc""#)
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
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
}
