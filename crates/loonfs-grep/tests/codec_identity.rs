//! Byte-identity tests between the extracted grep codec and its API source.

use loonfs_api::wire::index_grams as api_codec;
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_index_block, BlockHandle, BuiltSegmentBlocks,
    SegmentBlocksBuilder,
};
use loonfs_api::{InodeId, RevisionNo};
use loonfs_grep::codec as grep_codec;

const GOLDEN_POSTING_BATCH: &[u8] = &[0x04, 0x02, 0x01, 0x00, 0x03, 0x03, 0x01, 0xff, 0x06, 0x07];
const GOLDEN_POSTING_ROW: &[u8] = &[
    0xa4, 0x64, 0x6b, 0x69, 0x6e, 0x64, 0x6d, 0x67, 0x72, 0x61, 0x6d, 0x5f, 0x70, 0x6f, 0x73, 0x74,
    0x69, 0x6e, 0x67, 0x73, 0x64, 0x67, 0x72, 0x61, 0x6d, 0x66, 0x36, 0x36, 0x36, 0x66, 0x37, 0x38,
    0x6e, 0x66, 0x69, 0x72, 0x73, 0x74, 0x5f, 0x69, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x69, 0x64, 0x02,
    0x68, 0x70, 0x6f, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x73, 0x4a, 0x04, 0x02, 0x01, 0x00, 0x03, 0x03,
    0x01, 0xff, 0x06, 0x07,
];

fn posting_values(size: usize) -> Vec<(u64, u64)> {
    (0..size)
        .map(|index| ((index / 4) as u64 + 1, (index % 4) as u64 + 1))
        .collect()
}

fn grep_postings(values: &[(u64, u64)]) -> Vec<grep_codec::GramPosting> {
    values
        .iter()
        .map(|&(inode, revision)| grep_codec::GramPosting {
            inode_id: InodeId(inode),
            revision_no: RevisionNo(revision),
        })
        .collect()
}

fn api_postings(values: &[(u64, u64)]) -> Vec<api_codec::GramPosting> {
    values
        .iter()
        .map(|&(inode, revision)| api_codec::GramPosting {
            inode_id: InodeId(inode),
            revision_no: RevisionNo(revision),
        })
        .collect()
}

fn grep_values(postings: &[grep_codec::GramPosting]) -> Vec<(u64, u64)> {
    postings
        .iter()
        .map(|posting| (posting.inode_id.0, posting.revision_no.0))
        .collect()
}

fn api_values(postings: &[api_codec::GramPosting]) -> Vec<(u64, u64)> {
    postings
        .iter()
        .map(|posting| (posting.inode_id.0, posting.revision_no.0))
        .collect()
}

#[test]
fn tokenizer_is_identical_for_the_format_corpus() {
    let corpus: &[&[u8]] = &[
        b"The quick brown fox",
        b"AbCdEf",
        b"aBcDeF",
        "Grüße".as_bytes(),
        &[0xff, b'A', 0x80, b'Z'],
        b"",
        b"a",
        b"ab",
        b"abc",
        b"aaaaaa",
        b"abcabcabc",
    ];

    for content in corpus {
        let grep: Vec<[u8; 3]> = grep_codec::extract_grams(content)
            .into_iter()
            .map(|gram| gram.0)
            .collect();
        let api: Vec<[u8; 3]> = api_codec::extract_grams(content)
            .into_iter()
            .map(|gram| gram.0)
            .collect();
        assert_eq!(grep, api, "tokenizer diverged for {content:?}");
    }

    assert_eq!(
        grep_codec::extract_grams(b"AbCdEf"),
        grep_codec::extract_grams(b"aBcDeF")
    );
}

#[test]
fn posting_batches_are_byte_identical_and_cross_decode() {
    assert!(matches!(
        grep_codec::encode_gram_postings(&[]),
        Err(grep_codec::IndexGramsCodecError::EmptyPostings)
    ));
    assert!(matches!(
        api_codec::encode_gram_postings(&[]),
        Err(api_codec::IndexGramsCodecError::EmptyPostings)
    ));

    let cases = [
        vec![(42, 7)],
        vec![(2, 1), (2, 3), (5, 1), (900, 7)],
        posting_values(255),
        posting_values(256),
        posting_values(257),
    ];
    for values in cases {
        let grep_bytes =
            grep_codec::encode_gram_postings(&grep_postings(&values)).expect("grep encode");
        let api_bytes =
            api_codec::encode_gram_postings(&api_postings(&values)).expect("api encode");
        assert_eq!(
            grep_bytes,
            api_bytes,
            "encoding diverged for {} postings",
            values.len()
        );
        assert_eq!(
            grep_values(&grep_codec::decode_gram_postings(&api_bytes).expect("grep decode API")),
            values
        );
        assert_eq!(
            api_values(&api_codec::decode_gram_postings(&grep_bytes).expect("API decode grep")),
            values
        );
    }

    let fixture_values = [(2, 1), (2, 3), (5, 1), (900, 7)];
    let encoded = grep_codec::encode_gram_postings(&grep_postings(&fixture_values))
        .expect("encode golden posting batch");
    assert_eq!(encoded, GOLDEN_POSTING_BATCH);
}

#[test]
fn row_cbor_is_golden_and_cross_decodes() {
    let values = [(2, 1), (2, 3), (5, 1), (900, 7)];
    let grep_row =
        grep_codec::IndexRow::gram_postings(grep_codec::Gram(*b"fox"), &grep_postings(&values))
            .expect("grep row");
    let api_row =
        api_codec::IndexRow::gram_postings(api_codec::Gram(*b"fox"), &api_postings(&values))
            .expect("api row");

    let mut grep_bytes = Vec::new();
    ciborium::ser::into_writer(&grep_row, &mut grep_bytes).expect("encode grep row");
    let mut api_bytes = Vec::new();
    ciborium::ser::into_writer(&api_row, &mut api_bytes).expect("encode API row");
    assert_eq!(grep_bytes, api_bytes);
    assert_eq!(grep_bytes, GOLDEN_POSTING_ROW);

    let decoded_grep: grep_codec::IndexRow =
        ciborium::de::from_reader(api_bytes.as_slice()).expect("grep decodes API row");
    let decoded_api: api_codec::IndexRow =
        ciborium::de::from_reader(grep_bytes.as_slice()).expect("API decodes grep row");
    assert_eq!(decoded_grep.row_key(), api_row.row_key());
    assert_eq!(decoded_grep.filter_key(), api_row.filter_key());
    assert_eq!(
        grep_values(&decoded_grep.postings().expect("grep postings")),
        values
    );
    assert_eq!(
        api_values(&decoded_api.postings().expect("API postings")),
        values
    );
}

#[test]
fn row_keys_and_lookup_grammar_are_identical() {
    let cases = [(*b"fox", 42), ([0x00, 0xab, 0xff], 0), (*b"ABC", u64::MAX)];
    for (gram, inode) in cases {
        let values = [(inode, 7)];
        let grep_gram = grep_codec::Gram(gram);
        let api_gram = api_codec::Gram(gram);
        let grep_row = grep_codec::IndexRow::gram_postings(grep_gram, &grep_postings(&values))
            .expect("grep row");
        let api_row =
            api_codec::IndexRow::gram_postings(api_gram, &api_postings(&values)).expect("api row");

        assert_eq!(grep_row.row_key(), api_row.row_key());
        assert_eq!(grep_row.filter_key(), api_row.filter_key());
        assert_eq!(
            grep_codec::lookup::gram_probe(grep_gram),
            api_codec::lookup::gram_probe(api_gram)
        );
        assert_eq!(
            grep_codec::lookup::gram_prefix(grep_gram),
            api_codec::lookup::gram_prefix(api_gram)
        );
    }
}

fn grep_rows() -> Vec<grep_codec::IndexRow> {
    [b"box", b"fox", b"the"]
        .into_iter()
        .map(|gram| {
            let values = if gram == b"fox" {
                vec![(2, 1), (2, 3), (5, 1), (900, 7)]
            } else {
                vec![(2, 1)]
            };
            grep_codec::IndexRow::gram_postings(grep_codec::Gram(*gram), &grep_postings(&values))
                .expect("grep row")
        })
        .collect()
}

fn api_rows() -> Vec<api_codec::IndexRow> {
    [b"box", b"fox", b"the"]
        .into_iter()
        .map(|gram| {
            let values = if gram == b"fox" {
                vec![(2, 1), (2, 3), (5, 1), (900, 7)]
            } else {
                vec![(2, 1)]
            };
            api_codec::IndexRow::gram_postings(api_codec::Gram(*gram), &api_postings(&values))
                .expect("api row")
        })
        .collect()
}

fn build_grep_segment() -> BuiltSegmentBlocks {
    let mut builder = SegmentBlocksBuilder::new(256);
    for row in grep_rows() {
        builder
            .push(&row.row_key(), &row.filter_key(), &row)
            .expect("push grep row");
    }
    builder.finish().expect("finish grep segment")
}

fn build_api_segment() -> BuiltSegmentBlocks {
    let mut builder = SegmentBlocksBuilder::new(256);
    for row in api_rows() {
        builder
            .push(&row.row_key(), &row.filter_key(), &row)
            .expect("push API row");
    }
    builder.finish().expect("finish API segment")
}

fn section<'a>(bytes: &'a [u8], handle: &BlockHandle) -> &'a [u8] {
    &bytes[handle.offset as usize..handle.offset as usize + handle.stored_len as usize]
}

#[test]
fn shared_segment_block_encoding_is_identical_and_cross_decodes() {
    let grep = build_grep_segment();
    let api = build_api_segment();
    assert_eq!(grep, api);

    let grep_index = decode_index_block(section(&grep.bytes, &grep.index), &grep.index)
        .expect("decode grep index");
    let api_index =
        decode_index_block(section(&api.bytes, &api.index), &api.index).expect("decode API index");
    assert_eq!(grep_index, api_index);

    let mut grep_from_api = Vec::new();
    let mut api_from_grep = Vec::new();
    for (grep_entry, api_entry) in grep_index.iter().zip(&api_index) {
        let grep_block = decode_data_block_rows::<grep_codec::IndexRow>(
            section(&api.bytes, &api_entry.block),
            &api_entry.block,
        )
        .expect("grep decodes API block");
        let api_block = decode_data_block_rows::<api_codec::IndexRow>(
            section(&grep.bytes, &grep_entry.block),
            &grep_entry.block,
        )
        .expect("API decodes grep block");
        grep_from_api.extend(grep_block.rows.into_iter().map(|row| row.row_key()));
        api_from_grep.extend(api_block.rows.into_iter().map(|row| row.row_key()));
    }
    assert_eq!(grep_from_api, api_from_grep);
    assert_eq!(grep_from_api.len(), 3);
}
