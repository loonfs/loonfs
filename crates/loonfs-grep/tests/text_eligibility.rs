//! Equality lock between the core build-side and grep query-side text sniff.

use loonfs_api::wire::index_grams::INDEX_GRAMS_MAX_FILE_BYTES;

fn assert_same(label: &str, content: &[u8]) {
    assert_eq!(
        loonfs_grep::is_indexable_text_content(content),
        loonfs_core::is_indexable_text_content(content),
        "eligibility diverged for {label}"
    );
}

#[test]
fn query_text_eligibility_matches_core_across_content_boundaries() {
    const SAMPLE_BYTES: usize = 8 * 1024;

    assert_same("empty", b"");
    assert_same("ascii text", b"plain text\n");
    assert_same("utf-8 text", "snowman: ☃\n".as_bytes());
    assert_same("leading nul", b"\0binary");

    let mut nul_at_sample_end = vec![b'a'; SAMPLE_BYTES];
    nul_at_sample_end[SAMPLE_BYTES - 1] = 0;
    assert_same("nul at sample end", &nul_at_sample_end);

    let mut invalid_utf8 = vec![b'a'; SAMPLE_BYTES];
    invalid_utf8[SAMPLE_BYTES / 2] = 0xff;
    assert_same("invalid utf-8 inside sample", &invalid_utf8);

    let mut split_character = vec![b'a'; SAMPLE_BYTES - 1];
    split_character.extend_from_slice(&[0xc3, 0xa9]);
    assert_same("sample ending inside utf-8 character", &split_character);

    let mut invalid_after_sample = vec![b'a'; SAMPLE_BYTES + 1];
    invalid_after_sample[SAMPLE_BYTES] = 0xff;
    assert_same("invalid byte after sample", &invalid_after_sample);

    let at_cap = vec![b'a'; INDEX_GRAMS_MAX_FILE_BYTES as usize];
    assert_same("exact size cap", &at_cap);
    let over_cap = vec![b'a'; INDEX_GRAMS_MAX_FILE_BYTES as usize + 1];
    assert_same("one byte over size cap", &over_cap);
}
