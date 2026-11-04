//! Gram tokenizer and durable segment codecs for LoonFS grep.
//!
//! The frozen tokenizer, the posting batch, and the row grammar of the
//! index's three row families are specified in
//! [`docs/specs/format.md` Appendix D](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#appendix-d-grep-extension-format).

use loonfs_types::format::segment_blocks::{read_varint, write_varint, SegmentBlockCodecError};
use loonfs_types::{ChangeSeq, ContentId, ContentRef, InodeId, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

/// Bytes per gram. Fixed by the frozen durable format.
pub(crate) const GRAM_LEN: usize = 3;
/// Largest content the version-1 eligibility rule admits, in bytes.
pub const MAX_INDEX_GRAMS_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// True when content participates in the gram index: within the size cap
/// and text by the grep-family sniff (no NUL byte and valid UTF-8 in the
/// leading sample; a sample that ends inside a multi-byte character still
/// counts as valid).
pub(crate) fn is_indexable_text_content(content: &[u8]) -> bool {
    const ELIGIBILITY_SAMPLE_BYTES: usize = 8 * 1024;

    if content.len() as u64 > MAX_INDEX_GRAMS_FILE_BYTES {
        return false;
    }
    let sample = &content[..content.len().min(ELIGIBILITY_SAMPLE_BYTES)];
    if sample.contains(&0) {
        return false;
    }
    match std::str::from_utf8(sample) {
        Ok(_) => true,
        Err(error) => error.error_len().is_none() && error.valid_up_to() > 0,
    }
}

/// One tokenizer gram: three content bytes, ASCII-case-folded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gram(pub [u8; GRAM_LEN]);

impl Gram {
    /// The gram as the six lowercase hex characters row keys embed.
    pub(crate) fn as_hex(&self) -> String {
        loonfs_types::format::hex::hex_encode_bytes(&self.0)
    }

    /// Parses the row-key hex form back into a gram.
    pub(crate) fn from_hex(hex: &str) -> Result<Self, IndexGramsCodecError> {
        let bytes = loonfs_types::format::hex::hex_decode_bytes(hex).map_err(|reason| {
            IndexGramsCodecError::InvalidGram {
                reason: reason.to_string(),
            }
        })?;
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
pub(crate) fn extract_grams(content: &[u8]) -> BTreeSet<Gram> {
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

/// One content value: the bytes that a content reference identifies, by the
/// reference's content ID and size.
///
/// Two references with the same content ID and size refer to the same bytes in
/// every namespace of a fork family, so an index of content values is valid
/// in every member. Values order by content ID, then size, which is the order
/// of their row keys and of a run's value table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ContentValue {
    /// The content ID's 128-bit value, whose order is the order of the IDs'
    /// bodies. A value keeps the number rather than the ID's string, because
    /// steps and queries copy and compare many values.
    content_id: u128,
    pub size_bytes: u64,
}

impl ContentValue {
    /// The value of the content ID `content_id` at `size_bytes`.
    pub fn new(content_id: &ContentId, size_bytes: u64) -> Self {
        Self {
            content_id: u128::from_be_bytes(content_id.to_be_bytes()),
            size_bytes,
        }
    }

    /// The value that `content_ref` identifies.
    pub fn of(content_ref: &ContentRef) -> Self {
        Self::new(&content_ref.content_id, content_ref.size_bytes)
    }

    /// Returns the value's content ID.
    pub fn content_id(&self) -> ContentId {
        ContentId::from_be_bytes(self.content_id.to_be_bytes())
    }

    /// The value's content ID body and size, which a pruning filter keys a
    /// value by.
    pub(crate) fn filter_key(&self) -> String {
        format!("{}-{:020}", self.content_id().body(), self.size_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum IndexGramsCodecError {
    #[error("a posting batch must contain at least one posting")]
    EmptyPostings,
    #[error("posting ordinal {ordinal} is not in strictly ascending order after {previous}")]
    PostingsOutOfOrder { previous: u64, ordinal: u64 },
    #[error("malformed posting batch: {0}")]
    Malformed(String),
    #[error("invalid gram hex: {reason}")]
    InvalidGram { reason: String },
    #[error("gram postings row first ordinal {declared} does not match its batch's {actual}")]
    FirstOrdinalMismatch { declared: u64, actual: u64 },
}

/// Packs a run's value ordinals into the batch layout: the count, the first
/// ordinal, and then the gap from each ordinal to the next. The slice must
/// be non-empty and strictly ascending.
pub(crate) fn encode_gram_postings(ordinals: &[u64]) -> Result<Vec<u8>, IndexGramsCodecError> {
    let (&first, rest) = ordinals
        .split_first()
        .ok_or(IndexGramsCodecError::EmptyPostings)?;
    // A gap is usually one or two bytes.
    let mut bytes = Vec::with_capacity(2 + ordinals.len() * 2);
    write_varint(&mut bytes, ordinals.len() as u64);
    write_varint(&mut bytes, first);
    let mut previous = first;
    for &ordinal in rest {
        if ordinal <= previous {
            return Err(IndexGramsCodecError::PostingsOutOfOrder { previous, ordinal });
        }
        write_varint(&mut bytes, ordinal - previous);
        previous = ordinal;
    }
    Ok(bytes)
}

/// Unpacks a posting batch, validating the count, strict ascending order,
/// and that the batch ends exactly where its bytes do.
pub(crate) fn decode_gram_postings(bytes: &[u8]) -> Result<Vec<u64>, IndexGramsCodecError> {
    let mut cursor = 0usize;
    let count = read_varint(bytes, &mut cursor).map_err(malformed_varint)?;
    if count == 0 {
        return Err(IndexGramsCodecError::EmptyPostings);
    }
    let count = usize::try_from(count)
        .map_err(|_| IndexGramsCodecError::Malformed("posting count exceeds usize".to_owned()))?;
    if count > bytes.len() {
        // Every posting costs at least one byte past the count varint, so a
        // count beyond that is corrupt, not just implausible.
        return Err(IndexGramsCodecError::Malformed(format!(
            "posting count {count} exceeds batch bytes {}",
            bytes.len()
        )));
    }
    let mut ordinals = Vec::with_capacity(count);
    let mut previous = read_varint(bytes, &mut cursor).map_err(malformed_varint)?;
    ordinals.push(previous);
    for _ in 1..count {
        let gap = read_varint(bytes, &mut cursor).map_err(malformed_varint)?;
        if gap == 0 {
            return Err(IndexGramsCodecError::PostingsOutOfOrder {
                previous,
                ordinal: previous,
            });
        }
        previous = previous.checked_add(gap).ok_or_else(|| {
            IndexGramsCodecError::Malformed(format!(
                "posting gap {gap} after ordinal {previous} overflows"
            ))
        })?;
        ordinals.push(previous);
    }
    if cursor != bytes.len() {
        return Err(IndexGramsCodecError::Malformed(format!(
            "{} trailing bytes after the last posting",
            bytes.len() - cursor
        )));
    }
    Ok(ordinals)
}

fn malformed_varint(error: SegmentBlockCodecError) -> IndexGramsCodecError {
    IndexGramsCodecError::Malformed(error.to_string())
}

/// One row of a grep index segment, from one of the index's five row
/// families. Kind-tagged so a later released format version can define
/// additional row kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IndexRow {
    /// A batch of the run's value ordinals whose bytes contain one gram.
    GramPostings(GramPostingsRow),
    /// Records that a grep step examined one content value.
    Text(TextRow),
    /// Indexes one file revision by the content value that it refers to.
    ContentRevision(ContentRevisionRow),
    /// One entry of the run's value table.
    Value(ValueRow),
    /// One chunk of the revision filter of a reorganization into the base
    /// level.
    Filter(FilterRow),
}

/// A batch of the ordinals, in the run's value table, of the content values
/// whose bytes contain one gram, in ascending order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GramPostingsRow {
    pub gram: Gram,
    /// The batch's first ordinal, repeated from the packed postings so row
    /// keys derive without decoding the batch. [`Self::ordinals`] validates
    /// the pairing.
    pub first_ordinal: u64,
    pub postings: serde_bytes::ByteBuf,
}

impl GramPostingsRow {
    /// Unpacks and validates this row's batch, including that the declared
    /// first ordinal matches the batch.
    pub fn ordinals(&self) -> Result<Vec<u64>, IndexGramsCodecError> {
        let decoded = decode_gram_postings(&self.postings)?;
        let actual = decoded[0];
        if actual != self.first_ordinal {
            return Err(IndexGramsCodecError::FirstOrdinalMismatch {
                declared: self.first_ordinal,
                actual,
            });
        }
        Ok(decoded)
    }
}

/// One entry of a run's value table: the content value that the run's
/// postings refer to by `ordinal`.
///
/// A run's table lists each content value that the run has a `texts` row
/// for once, ordered by content ID and then size, and an entry's ordinal is
/// its position in that order, from zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueRow {
    pub ordinal: u64,
    pub content_id: ContentId,
    pub size_bytes: u64,
}

impl ValueRow {
    /// The content value that this entry describes.
    pub fn value(&self) -> ContentValue {
        ContentValue::new(&self.content_id, self.size_bytes)
    }
}

/// One chunk of the revision filter that a reorganization into the base level
/// writes in its output run, so that its later steps prune
/// `content_revisions` rows without scanning the namespace again.
///
/// The chunks' payloads, in chunk order, form the payload of a segment
/// filter block over the kept revisions' `{inode_id_body}-{revision_no:020}`
/// keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterRow {
    pub chunk: u64,
    pub payload: serde_bytes::ByteBuf,
}

/// Records that a grep step examined one content value, and whether the
/// value passed the eligibility rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextRow {
    pub content_id: ContentId,
    pub size_bytes: u64,
    /// Whether the value passed the eligibility rule, so the index has its
    /// postings.
    pub eligible: bool,
}

impl TextRow {
    /// The content value that this row records.
    pub fn value(&self) -> ContentValue {
        ContentValue::new(&self.content_id, self.size_bytes)
    }
}

/// Indexes one file revision by the content value that it refers to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentRevisionRow {
    pub content_id: ContentId,
    pub size_bytes: u64,
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    /// The sequence of the commit that published the revision.
    pub committed_seq: ChangeSeq,
}

impl ContentRevisionRow {
    /// The content value that the revision refers to.
    pub fn value(&self) -> ContentValue {
        ContentValue::new(&self.content_id, self.size_bytes)
    }
}

impl IndexRow {
    /// Packs a run's value ordinals into a row for `gram`. The ordinals must
    /// be non-empty and strictly ascending, as for `encode_gram_postings`.
    pub fn gram_postings(gram: Gram, ordinals: &[u64]) -> Result<Self, IndexGramsCodecError> {
        let first_ordinal = *ordinals
            .first()
            .ok_or(IndexGramsCodecError::EmptyPostings)?;
        let bytes = encode_gram_postings(ordinals)?;
        Ok(Self::GramPostings(GramPostingsRow {
            gram,
            first_ordinal,
            postings: serde_bytes::ByteBuf::from(bytes),
        }))
    }

    /// The `texts` row of `value`.
    pub fn text(value: &ContentValue, eligible: bool) -> Self {
        Self::Text(TextRow {
            content_id: value.content_id(),
            size_bytes: value.size_bytes,
            eligible,
        })
    }

    /// The `content_revisions` row of one file revision.
    pub fn content_revision(
        value: &ContentValue,
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
    ) -> Self {
        Self::ContentRevision(ContentRevisionRow {
            content_id: value.content_id(),
            size_bytes: value.size_bytes,
            inode_id,
            revision_no,
            committed_seq,
        })
    }

    /// The revision filter chunk `chunk`, whose payload is `payload`.
    pub fn filter(chunk: u64, payload: &[u8]) -> Self {
        Self::Filter(FilterRow {
            chunk,
            payload: serde_bytes::ByteBuf::from(payload.to_vec()),
        })
    }

    /// The value table entry of `value` at `ordinal`.
    pub fn value(ordinal: u64, value: &ContentValue) -> Self {
        Self::Value(ValueRow {
            ordinal,
            content_id: value.content_id(),
            size_bytes: value.size_bytes,
        })
    }

    pub fn row_key(&self) -> String {
        match self {
            Self::GramPostings(row) => lookup::gram_row_key(row.gram, row.first_ordinal),
            Self::Text(row) => lookup::text_row_key(&row.content_id, row.size_bytes),
            Self::ContentRevision(row) => lookup::content_revision_row_key(
                &row.content_id,
                row.size_bytes,
                row.inode_id,
                row.revision_no,
            ),
            Self::Value(row) => lookup::value_row_key(row.ordinal),
            Self::Filter(row) => lookup::filter_row_key(row.chunk),
        }
    }

    /// The lookup prefix a query probes for this row, and therefore the key
    /// inserted into the segment's bloom filter.
    pub fn filter_key(&self) -> String {
        match self {
            Self::GramPostings(row) => lookup::gram_probe(row.gram),
            Self::Text(row) => lookup::text_probe(&row.content_id),
            Self::ContentRevision(row) => {
                lookup::content_revision_probe(&row.content_id, row.size_bytes)
            }
            Self::Value(_) => lookup::VALUE_FILTER_KEY.to_owned(),
            Self::Filter(_) => lookup::FILTER_ROWS_FILTER_KEY.to_owned(),
        }
    }
}

/// Reader-side lookup grammar for grep index rows, defined beside the row
/// keys they select: a probe must equal the filter key the writer stored,
/// and a prefix must be a prefix of the row keys it selects.
pub mod lookup {
    use super::Gram;
    use loonfs_types::{ContentId, InodeId, RevisionNo};

    /// Scan floor shared by every gram row key.
    pub(crate) const GRAM_ROW_PREFIX: &str = "gram-";
    /// Scan floor shared by every `texts` row key.
    pub(crate) const TEXT_ROW_PREFIX: &str = "text-";
    /// Scan floor shared by every `content_revisions` row key.
    pub(crate) const CONTENT_REVISION_ROW_PREFIX: &str = "content-revision-";
    /// Scan floor shared by every value table row key. It sorts after every
    /// other family's prefix, so a run's value table ends the run's keys.
    pub(crate) const VALUE_ROW_PREFIX: &str = "value-";
    /// The filter key of every value table row. The rows share it, so a
    /// run's table adds one key to a segment's filter, and no lookup probes
    /// it: a value table's rows end the run's keys, so a segment's key range
    /// says whether it has any.
    pub(crate) const VALUE_FILTER_KEY: &str = "value";
    /// Scan floor shared by every revision filter row key.
    pub(crate) const FILTER_ROW_PREFIX: &str = "filter-";
    /// The filter key of every revision filter row, which, like the value
    /// table's, no lookup probes.
    pub(crate) const FILTER_ROWS_FILTER_KEY: &str = "filter";

    pub(crate) fn gram_probe(gram: Gram) -> String {
        format!("{GRAM_ROW_PREFIX}{}", gram.as_hex())
    }

    pub(crate) fn gram_prefix(gram: Gram) -> String {
        format!("{}-", gram_probe(gram))
    }

    pub(crate) fn gram_row_key(gram: Gram, first_ordinal: u64) -> String {
        format!("{}{first_ordinal:020}", gram_prefix(gram))
    }

    pub(crate) fn value_row_key(ordinal: u64) -> String {
        format!("{VALUE_ROW_PREFIX}{ordinal:020}")
    }

    pub(crate) fn filter_row_key(chunk: u64) -> String {
        format!("{FILTER_ROW_PREFIX}{chunk:020}")
    }

    pub(crate) fn text_probe(content_id: &ContentId) -> String {
        format!("{TEXT_ROW_PREFIX}{}", content_id.body())
    }

    pub(crate) fn text_row_key(content_id: &ContentId, size_bytes: u64) -> String {
        format!("{}-{size_bytes:020}", text_probe(content_id))
    }

    pub(crate) fn content_revision_probe(content_id: &ContentId, size_bytes: u64) -> String {
        format!(
            "{CONTENT_REVISION_ROW_PREFIX}{}-{size_bytes:020}",
            content_id.body()
        )
    }

    /// The prefix of one content value's `content_revisions` rows.
    pub(crate) fn content_revision_prefix(content_id: &ContentId, size_bytes: u64) -> String {
        format!("{}-", content_revision_probe(content_id, size_bytes))
    }

    /// The prefix of one content value's `content_revisions` rows for one
    /// inode.
    pub(crate) fn content_revision_inode_prefix(
        content_id: &ContentId,
        size_bytes: u64,
        inode_id: InodeId,
    ) -> String {
        format!(
            "{}{}-",
            content_revision_prefix(content_id, size_bytes),
            inode_id.body()
        )
    }

    /// A `content_revisions` row key, which sorts one value's revisions by
    /// inode and then newest first.
    pub(crate) fn content_revision_row_key(
        content_id: &ContentId,
        size_bytes: u64,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> String {
        format!(
            "{}{:020}",
            content_revision_inode_prefix(content_id, size_bytes, inode_id),
            u64::MAX - revision_no.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    fn content_id(value: u128) -> ContentId {
        ContentId::from_be_bytes(value.to_be_bytes())
    }

    fn content_value(content: u128, size_bytes: u64) -> ContentValue {
        ContentValue::new(&content_id(content), size_bytes)
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
        let ordinals = vec![0, 1, 4, 900, 70_000, u64::MAX];
        let bytes = encode_gram_postings(&ordinals).expect("encode");
        assert_eq!(decode_gram_postings(&bytes).expect("decode"), ordinals);
    }

    #[test]
    fn posting_batches_reject_disorder_duplicates_and_empties() {
        assert_matches!(
            encode_gram_postings(&[]),
            Err(IndexGramsCodecError::EmptyPostings)
        );
        assert_matches!(
            encode_gram_postings(&[2, 1]),
            Err(IndexGramsCodecError::PostingsOutOfOrder {
                previous: 2,
                ordinal: 1
            })
        );
        assert_matches!(
            encode_gram_postings(&[1, 1]),
            Err(IndexGramsCodecError::PostingsOutOfOrder { .. })
        );
    }

    #[test]
    fn posting_batch_decode_rejects_trailing_truncated_and_overflowing_bytes() {
        let bytes = encode_gram_postings(&[1, 2]).expect("encode");

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_matches!(
            decode_gram_postings(&trailing),
            Err(IndexGramsCodecError::Malformed(_))
        );

        let truncated = &bytes[..bytes.len() - 1];
        assert_matches!(
            decode_gram_postings(truncated),
            Err(IndexGramsCodecError::Malformed(_))
        );

        assert_matches!(
            decode_gram_postings(&[]),
            Err(IndexGramsCodecError::Malformed(_))
        );

        let mut overflowing = Vec::new();
        for value in [2, u64::MAX, 1] {
            write_varint(&mut overflowing, value);
        }
        assert_matches!(
            decode_gram_postings(&overflowing),
            Err(IndexGramsCodecError::Malformed(message)) if message.contains("overflows")
        );
    }

    #[test]
    fn posting_batches_pin_their_byte_layout() {
        let bytes = encode_gram_postings(&[7, 8, 300]).expect("encode");
        assert_eq!(bytes, [3, 7, 1, 0xa4, 0x02]);
    }

    #[test]
    fn posting_batch_decode_rejects_a_zero_gap() {
        let mut bytes = Vec::new();
        for value in [2, 5, 0] {
            write_varint(&mut bytes, value);
        }
        assert_matches!(
            decode_gram_postings(&bytes),
            Err(IndexGramsCodecError::PostingsOutOfOrder {
                previous: 5,
                ordinal: 5
            })
        );
    }

    #[test]
    fn row_keys_and_filter_keys_pin_their_shapes() {
        let value = content_value(42, 7);
        let row = IndexRow::gram_postings(Gram(*b"fox"), &[3, 9]).expect("row");
        assert_eq!(row.row_key(), "gram-666f78-00000000000000000003");
        assert_eq!(row.filter_key(), "gram-666f78");
        assert_eq!(lookup::gram_prefix(Gram(*b"fox")), "gram-666f78-");
        let IndexRow::GramPostings(gram_row) = &row else {
            panic!("expected a gram postings row");
        };
        assert_eq!(gram_row.ordinals().expect("ordinals"), vec![3, 9]);

        let entry = IndexRow::value(12, &value);
        assert_eq!(entry.row_key(), "value-00000000000000000012");
        assert_eq!(entry.filter_key(), "value");

        let filter = IndexRow::filter(2, &[1, 2, 3]);
        assert_eq!(filter.row_key(), "filter-00000000000000000002");
        assert_eq!(filter.filter_key(), "filter");
        let IndexRow::Value(entry) = entry else {
            panic!("expected a value row");
        };
        assert_eq!(entry.value(), value);

        let text = IndexRow::text(&value, true);
        assert_eq!(
            text.row_key(),
            "text-0000000000000000000000001a-00000000000000000007"
        );
        assert_eq!(text.filter_key(), "text-0000000000000000000000001a");

        let revision = IndexRow::content_revision(&value, InodeId(9), RevisionNo(3), ChangeSeq(12));
        assert_eq!(
            revision.row_key(),
            "content-revision-0000000000000000000000001a-00000000000000000007-00000000000000000000000009-18446744073709551612"
        );
        assert_eq!(
            revision.filter_key(),
            "content-revision-0000000000000000000000001a-00000000000000000007"
        );
        assert_eq!(
            lookup::content_revision_inode_prefix(&value.content_id(), 7, InodeId(9)),
            "content-revision-0000000000000000000000001a-00000000000000000007-00000000000000000000000009-"
        );
        assert!(revision
            .row_key()
            .starts_with(&lookup::content_revision_prefix(&value.content_id(), 7)));
    }

    /// Checks that the five families sort in the order a step writes them,
    /// with a run's value table last, and that one value's
    /// `content_revisions` rows sort by inode, then newest revision first,
    /// apart from another size of the same content ID.
    #[test]
    fn content_revision_rows_sort_each_values_revisions_by_inode_newest_first() {
        let mut keys = [(5, 42, 1), (12, 9, 3), (5, 9, 2), (5, 42, 7), (5, 9, 10)].map(
            |(size_bytes, inode_id, revision_no)| {
                let row = IndexRow::content_revision(
                    &content_value(1, size_bytes),
                    InodeId(inode_id),
                    RevisionNo(revision_no),
                    ChangeSeq(1),
                );
                (row.row_key(), size_bytes, inode_id, revision_no)
            },
        );
        keys.sort();
        assert_eq!(
            keys.map(|(_, size_bytes, inode_id, revision_no)| (size_bytes, inode_id, revision_no)),
            [(5, 9, 10), (5, 9, 2), (5, 42, 7), (5, 42, 1), (12, 9, 3)]
        );
        let rows = [
            IndexRow::content_revision(
                &content_value(9, 1),
                InodeId(1),
                RevisionNo(1),
                ChangeSeq(1),
            ),
            IndexRow::filter(u64::MAX, &[0]),
            IndexRow::gram_postings(Gram(*b"abc"), &[0]).expect("row"),
            IndexRow::text(&content_value(1, 1), true),
            IndexRow::value(0, &content_value(1, 1)),
        ];
        assert!(rows
            .windows(2)
            .all(|pair| pair[0].row_key() < pair[1].row_key()));
    }

    #[test]
    fn segment_max_row_key_survives_a_full_final_block() {
        use loonfs_types::format::segment_blocks::SegmentBlocksBuilder;

        let mut builder = SegmentBlocksBuilder::new(std::num::NonZeroUsize::MIN);
        let mut last_row_key = String::new();
        for gram in [Gram(*b"abc"), Gram(*b"abd"), Gram(*b"abe")] {
            let row = IndexRow::gram_postings(gram, &[7]).expect("row");
            last_row_key = row.row_key();
            builder
                .push(&last_row_key, &row.filter_key(), &row)
                .expect("push");
        }
        let built = builder.build().expect("finish");
        assert_eq!(built.max_row_key, last_row_key);
        assert!(built.min_row_key <= built.max_row_key);
    }

    #[test]
    fn rows_reject_first_ordinal_mismatch() {
        let row = GramPostingsRow {
            gram: Gram(*b"fox"),
            first_ordinal: 9,
            postings: serde_bytes::ByteBuf::from(encode_gram_postings(&[42]).expect("encode")),
        };
        assert_matches!(
            row.ordinals(),
            Err(IndexGramsCodecError::FirstOrdinalMismatch {
                declared: 9,
                actual: 42
            })
        );
    }

    #[test]
    fn gram_hex_round_trips_and_rejects_bad_input() {
        let gram = Gram([0x00, 0xab, 0xff]);
        assert_eq!(gram.as_hex(), "00abff");
        assert_eq!(Gram::from_hex("00abff").expect("parse"), gram);
        Gram::from_hex("00ab").expect_err("a two-byte gram should be rejected");
        Gram::from_hex("00abff00").expect_err("a four-byte gram should be rejected");
        Gram::from_hex("00abzz").expect_err("a gram with non-hex digits should be rejected");
    }
}
