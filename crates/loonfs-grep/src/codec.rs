//! Gram tokenizer and durable segment codecs for LoonFS grep.
//!
//! The gram index accelerates content grep: every eligible file revision
//! contributes its three-byte substrings (grams, ASCII-case-folded), and
//! each gram maps to the `(inode_id, revision_no)` pairs whose content
//! contains it. Queries intersect posting lists to find candidates and
//! verify every candidate against the real pattern, so the index can only
//! make a grep slower, never wrong.
//!
//! Durable layout, copied byte-for-byte from the original API codec:
//!
//! - The **tokenizer** is every overlapping three-byte window of the
//!   content after folding ASCII letters to lower case. Grams are bytes,
//!   not characters.
//! - A **posting batch** is a varint-packed run of postings sorted
//!   strictly ascending by `(inode_id, revision_no)`: the posting count,
//!   the first posting's inode id and revision number, then for each
//!   subsequent posting its inode delta and absolute revision number. All
//!   integers are LEB128 varints, the same encoding data blocks use.
//! - An **index row** ([`IndexRow::GramPostings`]) carries one gram, the
//!   batch's first inode id (repeated from the batch so row keys derive
//!   without decoding it), and the packed batch. Its row key is
//!   `gram-{gram hex}-{first inode id:020}`; its filter key is the
//!   `gram-{gram hex}` prefix, so a segment's bloom filter answers "no
//!   postings for this gram here". Rows use the shared `loonfs-api`
//!   segment-block grammar.

use loonfs_api::{InodeId, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

/// Bytes per gram. Fixed by the copied durable format.
pub const GRAM_LEN: usize = 3;

/// One tokenizer gram: three content bytes, ASCII-case-folded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gram(pub [u8; GRAM_LEN]);

impl Gram {
    /// The gram as the six lowercase hex characters row keys embed.
    pub fn as_hex(&self) -> String {
        loonfs_api::wire::manifest::hex_encode_bytes(&self.0)
    }

    /// Parses the row-key hex form back into a gram.
    pub fn from_hex(hex: &str) -> Result<Self, IndexGramsCodecError> {
        let bytes = loonfs_api::wire::manifest::hex_decode_bytes(hex)
            .map_err(|reason| IndexGramsCodecError::InvalidGram { reason })?;
        let bytes: [u8; GRAM_LEN] =
            bytes
                .try_into()
                .map_err(|bytes: Vec<u8>| IndexGramsCodecError::InvalidGram {
                    reason: format!("expected {GRAM_LEN} bytes, got {}", bytes.len()),
                })?;
        Ok(Self(bytes))
    }
}

impl Serialize for Gram {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_hex())
    }
}

impl<'de> Deserialize<'de> for Gram {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(deserializer)?;
        Self::from_hex(&hex).map_err(serde::de::Error::custom)
    }
}

/// Extracts the distinct grams of one content buffer, in gram order.
/// Content shorter than one gram contributes nothing.
pub fn extract_grams(content: &[u8]) -> BTreeSet<Gram> {
    content
        .windows(GRAM_LEN)
        .map(|window| {
            let mut gram = [0u8; GRAM_LEN];
            for (folded, byte) in gram.iter_mut().zip(window) {
                *folded = byte.to_ascii_lowercase();
            }
            Gram(gram)
        })
        .collect()
}

/// One posting: a file revision whose content contains the gram.
///
/// Postings name durable `(inode_id, revision_no)` identity, never paths:
/// inode identity survives renames and forks, and paths are derived at
/// query time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GramPosting {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IndexGramsCodecError {
    #[error("a posting batch must contain at least one posting")]
    EmptyPostings,
    #[error(
        "posting inode {inode}/revision {revision} is not in strictly ascending \
         order after inode {previous_inode}/revision {previous_revision}"
    )]
    PostingsOutOfOrder {
        previous_inode: u64,
        previous_revision: u64,
        inode: u64,
        revision: u64,
    },
    #[error("malformed posting batch: {0}")]
    Malformed(String),
    #[error("invalid gram hex: {reason}")]
    InvalidGram { reason: String },
    #[error("gram postings row first inode {declared} does not match its batch's {actual}")]
    FirstInodeMismatch { declared: u64, actual: u64 },
}

/// Packs postings into the frozen batch layout. The slice must be
/// non-empty and strictly ascending by `(inode_id, revision_no)`.
pub fn encode_gram_postings(postings: &[GramPosting]) -> Result<Vec<u8>, IndexGramsCodecError> {
    let first = postings
        .first()
        .ok_or(IndexGramsCodecError::EmptyPostings)?;
    let mut bytes = Vec::with_capacity(postings.len() * 3);
    write_varint(&mut bytes, postings.len() as u64);
    write_varint(&mut bytes, first.inode_id.0);
    write_varint(&mut bytes, first.revision_no.0);
    let mut previous = *first;
    for posting in &postings[1..] {
        if *posting <= previous {
            return Err(IndexGramsCodecError::PostingsOutOfOrder {
                previous_inode: previous.inode_id.0,
                previous_revision: previous.revision_no.0,
                inode: posting.inode_id.0,
                revision: posting.revision_no.0,
            });
        }
        write_varint(&mut bytes, posting.inode_id.0 - previous.inode_id.0);
        write_varint(&mut bytes, posting.revision_no.0);
        previous = *posting;
    }
    Ok(bytes)
}

/// Unpacks a posting batch, validating the count, strict ascending order,
/// and that the batch ends exactly where its bytes do.
pub fn decode_gram_postings(bytes: &[u8]) -> Result<Vec<GramPosting>, IndexGramsCodecError> {
    let mut cursor = 0usize;
    let count = batch_varint(bytes, &mut cursor)?;
    if count == 0 {
        return Err(IndexGramsCodecError::EmptyPostings);
    }
    let count = usize::try_from(count)
        .map_err(|_| IndexGramsCodecError::Malformed("posting count exceeds usize".to_owned()))?;
    if count > bytes.len() {
        // Every posting costs at least two bytes past the count varint; a
        // count beyond the byte length is corrupt, not just implausible.
        return Err(IndexGramsCodecError::Malformed(format!(
            "posting count {count} exceeds batch bytes {}",
            bytes.len()
        )));
    }
    let mut postings = Vec::with_capacity(count);
    let mut previous = GramPosting {
        inode_id: InodeId(batch_varint(bytes, &mut cursor)?),
        revision_no: RevisionNo(batch_varint(bytes, &mut cursor)?),
    };
    postings.push(previous);
    for _ in 1..count {
        let inode_delta = batch_varint(bytes, &mut cursor)?;
        let inode_id = previous
            .inode_id
            .0
            .checked_add(inode_delta)
            .ok_or_else(|| {
                IndexGramsCodecError::Malformed("posting inode delta overflows".to_owned())
            })?;
        let posting = GramPosting {
            inode_id: InodeId(inode_id),
            revision_no: RevisionNo(batch_varint(bytes, &mut cursor)?),
        };
        if posting <= previous {
            return Err(IndexGramsCodecError::PostingsOutOfOrder {
                previous_inode: previous.inode_id.0,
                previous_revision: previous.revision_no.0,
                inode: posting.inode_id.0,
                revision: posting.revision_no.0,
            });
        }
        postings.push(posting);
        previous = posting;
    }
    if cursor != bytes.len() {
        return Err(IndexGramsCodecError::Malformed(format!(
            "{} trailing bytes after the last posting",
            bytes.len() - cursor
        )));
    }
    Ok(postings)
}

fn batch_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, IndexGramsCodecError> {
    read_varint(bytes, cursor)
}

fn write_varint(bytes: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            bytes.push(byte);
            return;
        }
        bytes.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, IndexGramsCodecError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            IndexGramsCodecError::Malformed("varint runs past the block".to_owned())
        })?;
        *cursor += 1;
        if shift >= 64 {
            return Err(IndexGramsCodecError::Malformed(
                "varint exceeds 64 bits".to_owned(),
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

/// One row of a gram index segment. Kind-tagged so the family can grow
/// additional row kinds under future format versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexRow {
    GramPostings {
        gram: Gram,
        /// The batch's first inode id, repeated from the packed postings so
        /// row keys derive without decoding the batch. [`Self::postings`]
        /// validates the pairing.
        first_inode_id: InodeId,
        postings: serde_bytes::ByteBuf,
    },
}

impl IndexRow {
    /// Packs postings into a row for `gram`. The postings must be
    /// non-empty and strictly ascending, as for [`encode_gram_postings`].
    pub fn gram_postings(
        gram: Gram,
        postings: &[GramPosting],
    ) -> Result<Self, IndexGramsCodecError> {
        let first = postings
            .first()
            .ok_or(IndexGramsCodecError::EmptyPostings)?;
        let first_inode_id = first.inode_id;
        let bytes = encode_gram_postings(postings)?;
        Ok(Self::GramPostings {
            gram,
            first_inode_id,
            postings: serde_bytes::ByteBuf::from(bytes),
        })
    }

    pub fn row_key(&self) -> String {
        match self {
            Self::GramPostings {
                gram,
                first_inode_id,
                ..
            } => format!("gram-{}-{:020}", gram.as_hex(), first_inode_id.0),
        }
    }

    /// The lookup prefix a query probes for this row, and therefore the key
    /// inserted into the segment's bloom filter.
    pub fn filter_key(&self) -> String {
        match self {
            Self::GramPostings { gram, .. } => lookup::gram_probe(*gram),
        }
    }

    /// Unpacks and validates this row's posting batch, including that the
    /// declared first inode matches the batch.
    pub fn postings(&self) -> Result<Vec<GramPosting>, IndexGramsCodecError> {
        match self {
            Self::GramPostings {
                first_inode_id,
                postings,
                ..
            } => {
                let decoded = decode_gram_postings(postings)?;
                let actual = decoded
                    .first()
                    .expect("decode_gram_postings rejects empty batches")
                    .inode_id;
                if actual != *first_inode_id {
                    return Err(IndexGramsCodecError::FirstInodeMismatch {
                        declared: first_inode_id.0,
                        actual: actual.0,
                    });
                }
                Ok(decoded)
            }
        }
    }
}

/// Reader-side lookup grammar for gram index rows, defined beside the row
/// keys they select: a probe must equal the filter key the writer stored,
/// and a prefix must be a prefix of the row keys it selects.
pub mod lookup {
    use super::Gram;

    pub fn gram_probe(gram: Gram) -> String {
        format!("gram-{}", gram.as_hex())
    }

    pub fn gram_prefix(gram: Gram) -> String {
        format!("{}-", gram_probe(gram))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting(inode: u64, revision: u64) -> GramPosting {
        GramPosting {
            inode_id: InodeId(inode),
            revision_no: RevisionNo(revision),
        }
    }

    #[test]
    fn extraction_case_folds_ascii_and_dedups() {
        let grams = extract_grams(b"AbaBA");
        let expected: BTreeSet<Gram> = [Gram(*b"aba"), Gram(*b"bab")].into_iter().collect();
        assert_eq!(grams, expected);
    }

    #[test]
    fn extraction_keeps_non_ascii_bytes_verbatim() {
        // UTF-8 continuation bytes are above the ASCII range; folding must
        // not touch them, so the grams of multi-byte text stay exact.
        let content = "Grüße".as_bytes();
        let grams = extract_grams(content);
        assert!(grams.contains(&Gram([b'g', b'r', 0xc3])));
        assert!(grams.contains(&Gram([0xc3, 0xbc, 0xc3])));
    }

    #[test]
    fn extraction_of_short_content_is_empty() {
        assert!(extract_grams(b"").is_empty());
        assert!(extract_grams(b"ab").is_empty());
        assert_eq!(extract_grams(b"abc").len(), 1);
    }

    #[test]
    fn posting_batches_round_trip() {
        let postings = vec![
            posting(1, 1),
            posting(1, 4),
            posting(7, 2),
            posting(900, 1),
            posting(900, 2),
        ];
        let bytes = encode_gram_postings(&postings).expect("encode");
        assert_eq!(decode_gram_postings(&bytes).expect("decode"), postings);
    }

    #[test]
    fn posting_batches_reject_disorder_duplicates_and_empties() {
        assert!(matches!(
            encode_gram_postings(&[]),
            Err(IndexGramsCodecError::EmptyPostings)
        ));
        assert!(matches!(
            encode_gram_postings(&[posting(2, 1), posting(1, 1)]),
            Err(IndexGramsCodecError::PostingsOutOfOrder { .. })
        ));
        assert!(matches!(
            encode_gram_postings(&[posting(1, 1), posting(1, 1)]),
            Err(IndexGramsCodecError::PostingsOutOfOrder { .. })
        ));
    }

    #[test]
    fn posting_batch_decode_rejects_trailing_and_truncated_bytes() {
        let bytes = encode_gram_postings(&[posting(1, 1), posting(2, 1)]).expect("encode");

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            decode_gram_postings(&trailing),
            Err(IndexGramsCodecError::Malformed(_))
        ));

        let truncated = &bytes[..bytes.len() - 1];
        assert!(matches!(
            decode_gram_postings(truncated),
            Err(IndexGramsCodecError::Malformed(_))
        ));

        assert!(matches!(
            decode_gram_postings(&[]),
            Err(IndexGramsCodecError::Malformed(_))
        ));
    }

    #[test]
    fn posting_batch_decode_rejects_reordered_bytes() {
        // A hand-built batch whose second posting repeats the first: valid
        // varints, invalid order.
        let mut bytes = Vec::new();
        write_varint(&mut bytes, 2);
        write_varint(&mut bytes, 5);
        write_varint(&mut bytes, 3);
        write_varint(&mut bytes, 0);
        write_varint(&mut bytes, 3);
        assert!(matches!(
            decode_gram_postings(&bytes),
            Err(IndexGramsCodecError::PostingsOutOfOrder { .. })
        ));
    }

    #[test]
    fn row_keys_and_filter_keys_pin_their_shapes() {
        let row =
            IndexRow::gram_postings(Gram(*b"fox"), &[posting(42, 7), posting(43, 1)]).expect("row");
        assert_eq!(row.row_key(), "gram-666f78-00000000000000000042");
        assert_eq!(row.filter_key(), "gram-666f78");
        assert_eq!(lookup::gram_prefix(Gram(*b"fox")), "gram-666f78-");
        assert_eq!(
            row.postings().expect("postings"),
            vec![posting(42, 7), posting(43, 1)]
        );
    }

    #[test]
    fn rows_reject_first_inode_mismatch() {
        let row = IndexRow::GramPostings {
            gram: Gram(*b"fox"),
            first_inode_id: InodeId(9),
            postings: serde_bytes::ByteBuf::from(
                encode_gram_postings(&[posting(42, 7)]).expect("encode"),
            ),
        };
        assert!(matches!(
            row.postings(),
            Err(IndexGramsCodecError::FirstInodeMismatch {
                declared: 9,
                actual: 42
            })
        ));
    }

    #[test]
    fn gram_hex_round_trips_and_rejects_bad_input() {
        let gram = Gram([0x00, 0xab, 0xff]);
        assert_eq!(gram.as_hex(), "00abff");
        assert_eq!(Gram::from_hex("00abff").expect("parse"), gram);
        assert!(Gram::from_hex("00ab").is_err());
        assert!(Gram::from_hex("00abff00").is_err());
        assert!(Gram::from_hex("00abzz").is_err());
    }
}
