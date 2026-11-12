//! Grep queries over an index of content values: matches in content order,
//! cursors that resume inside a value, pages after the first that read at
//! the first page's sequence, cursors that a rebuilt index rejects, and
//! stale results that leave out what the index hasn't reached.

use crate::common::{
    default_block_cache, grep_with, in_walk_order, page_request, publish_same_content_files,
};
use crate::content_index::{request, ContentFixture};
use loonfs::{ErrorCode, LoonFs};
use loonfs_grep::{GrepError, GrepService};
use loonfs_types::{decode_cursor, ContentId, GrepPageCursor, GrepResponse};
use std::assert_matches;

impl ContentFixture {
    /// Runs one page of `pattern` with at most `limit` matches at `cursor`.
    async fn page(&self, pattern: &str, cursor: Option<String>, limit: u32) -> GrepResponse {
        self.host
            .grep(
                &self.namespace_id,
                &request(pattern),
                page_request(cursor, limit),
            )
            .await
            .expect("grep page")
    }
}

#[tokio::test]
async fn matches_come_in_content_order_and_a_cursor_resumes_inside_a_value() {
    let fixture = ContentFixture::new("content-order").await;
    fixture.put("/alpha.txt", b"order needle alpha\n").await;
    fixture.put("/bravo.txt", b"order needle bravo\n").await;
    fixture.put("/shared-1.txt", b"order needle shared\n").await;
    let shared = fixture.content_ref("/shared-1.txt").await;
    for path in ["/shared-2.txt", "/shared-3.txt"] {
        fixture.copy("/shared-1.txt", path).await;
    }
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");

    let mut paths = Vec::new();
    let mut cursor_values: Vec<ContentId> = Vec::new();
    let mut cursor = None;
    loop {
        let page = fixture.page("order needle", cursor, 1).await;
        paths.extend(
            page.matches
                .iter()
                .map(|found| found.path.as_str().to_owned()),
        );
        let Some(next) = page.next_cursor else {
            break;
        };
        let decoded: GrepPageCursor = decode_cursor(&next).expect("decode cursor");
        cursor_values.push(decoded.last_content_id);
        cursor = Some(next);
    }
    let expected = in_walk_order(
        &fixture.namespace,
        [
            "/alpha.txt",
            "/bravo.txt",
            "/shared-1.txt",
            "/shared-2.txt",
            "/shared-3.txt",
        ],
    )
    .await;
    assert_eq!(paths, expected, "matches follow content value, then inode");
    assert!(
        cursor_values
            .windows(2)
            .any(|pair| pair[0] == shared.content_id && pair[1] == shared.content_id),
        "two pages in a row end inside the shared value: {cursor_values:?}"
    );
}

#[tokio::test]
async fn allow_stale_leaves_out_a_newer_revision_and_does_not_resurrect_a_deletion() {
    let fixture = ContentFixture::new("stale-cut").await;
    fixture.put("/changed.txt", b"stale needle one\n").await;
    fixture.put("/deleted.txt", b"stale needle two\n").await;
    fixture.put("/kept.txt", b"stale needle three\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    fixture.put("/changed.txt", b"stale needle newer\n").await;
    fixture
        .namespace
        .delete_path("/deleted.txt", &loonfs_test_support::test_actor())
        .await
        .expect("delete");
    // More files than one query scans, so the tail is over its budget.
    publish_same_content_files(&fixture.namespace, "filler", 513, b"filler\n").await;

    let lagging = fixture
        .host
        .grep(
            &fixture.namespace_id,
            &request("stale needle"),
            page_request(None, 100),
        )
        .await
        .expect_err("the tail is over its budget");
    assert_matches!(lagging, GrepError::IndexLagging { .. });
    let mut stale_request = request("stale needle");
    stale_request.allow_stale = true;
    let stale = fixture
        .host
        .grep(
            &fixture.namespace_id,
            &stale_request,
            page_request(None, 100),
        )
        .await
        .expect("stale grep");
    assert!(!stale.tail_scanned);
    assert_eq!(
        stale
            .matches
            .iter()
            .map(|found| found.path.as_str())
            .collect::<Vec<_>>(),
        ["/kept.txt"],
        "a file revised or deleted after the index's watermark is left out"
    );
}

#[tokio::test]
async fn a_stale_search_keeps_its_first_pages_cut_after_the_index_moves() {
    let fixture = ContentFixture::new("stale-pages").await;
    for path in ["/one.txt", "/two.txt", "/three.txt"] {
        fixture
            .put(path, format!("pages needle {path}\n").as_bytes())
            .await;
    }
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let [first, second, third] =
        in_walk_order(&fixture.namespace, ["/one.txt", "/two.txt", "/three.txt"]).await;
    // After the watermark, the second file takes the third's content, which
    // sorts after the first page's cursor, and the tail grows past its
    // budget.
    fixture
        .namespace
        .delete_path(&second, &loonfs_test_support::test_actor())
        .await
        .expect("delete the second file");
    fixture.copy(&third, &second).await;
    publish_same_content_files(&fixture.namespace, "filler", 513, b"filler\n").await;
    let mut stale_request = request("pages needle");
    stale_request.allow_stale = true;
    let paths = |page: &GrepResponse| {
        page.matches
            .iter()
            .map(|found| found.path.as_str().to_owned())
            .collect::<Vec<_>>()
    };

    let page_one = fixture
        .host
        .grep(&fixture.namespace_id, &stale_request, page_request(None, 1))
        .await
        .expect("first page");
    assert!(!page_one.tail_scanned);
    assert_eq!(paths(&page_one), [first]);
    // A run of index steps brings the index past the first page's sequence, and a new
    // service loads the index as it stands then.
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("catch up");
    let page_two = grep_with(
        &GrepService::new(default_block_cache()),
        &fixture.host.reader,
        &fixture.store,
        &fixture.namespace_id,
        &stale_request,
        page_request(page_one.next_cursor.clone(), 10),
    )
    .await
    .expect("second page");
    assert!(
        !page_two.tail_scanned,
        "the second page leaves out what the first page's cut left out"
    );
    assert_eq!(
        paths(&page_two),
        [third],
        "the second file's new revision is after the first page's cut"
    );
    assert!(page_two.next_cursor.is_none());
}

#[tokio::test]
async fn pages_after_the_first_read_at_the_cursors_head_sequence() {
    let fixture = ContentFixture::new("cursor-sequence").await;
    for (path, bytes) in [
        ("/one.txt", b"page needle one\n".as_slice()),
        ("/two.txt", b"page needle two\n"),
        ("/three.txt", b"page needle three\n"),
    ] {
        fixture.put(path, bytes).await;
    }
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let [first, second, third] =
        in_walk_order(&fixture.namespace, ["/one.txt", "/two.txt", "/three.txt"]).await;
    let second_line = fixture
        .namespace
        .read_file(&second)
        .await
        .expect("read")
        .bytes;

    let page_one = fixture.page("page needle", None, 1).await;
    assert_eq!(page_one.matches[0].path, first.as_str());
    let head_seq = page_one.head_seq;

    // After the first page, the second file loses its match, the third is
    // deleted, and a new file matches, and the index reaches all of it.
    fixture.put(&second, b"no match now\n").await;
    fixture
        .namespace
        .delete_path(&third, &loonfs_test_support::test_actor())
        .await
        .expect("delete");
    fixture.put("/four.txt", b"page needle four\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("catch up");

    let page_two = fixture.page("page needle", page_one.next_cursor, 1).await;
    assert_eq!(page_two.head_seq, head_seq);
    assert_eq!(page_two.matches[0].path, second.as_str());
    assert_eq!(
        format!("{}\n", page_two.matches[0].line).as_bytes(),
        second_line.as_slice()
    );
    let page_three = fixture.page("page needle", page_two.next_cursor, 1).await;
    assert_eq!(page_three.head_seq, head_seq);
    assert_eq!(page_three.matches[0].path, third.as_str());
    assert!(page_three.next_cursor.is_none());
}

#[tokio::test]
async fn a_page_after_the_floor_passes_its_cursor_returns_below_retention_floor() {
    let fixture = ContentFixture::new("cursor-floor").await;
    fixture.put("/one.txt", b"floor needle one\n").await;
    fixture.put("/two.txt", b"floor needle two\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let page_one = fixture.page("floor needle", None, 1).await;
    let cursor = page_one.next_cursor.expect("a cursor");

    fixture.put("/three.txt", b"later\n").await;
    fixture.advance_retention_floor().await;
    // A new reader loads the manifest that records the new floor.
    let reader = LoonFs::builder_with_store(fixture.store.clone())
        .read_only()
        .build()
        .await
        .expect("reader");
    let error = grep_with(
        &GrepService::new(default_block_cache()),
        &reader,
        &fixture.store,
        &fixture.namespace_id,
        &request("floor needle"),
        page_request(Some(cursor), 1),
    )
    .await
    .expect_err("the floor passed the cursor's sequence");
    assert_eq!(error.code(), ErrorCode::BelowRetentionFloor);
}

#[tokio::test]
async fn pages_combine_the_runs_values_in_content_order_as_they_walk_them() {
    let fixture = ContentFixture::new("content-order-runs").await;
    // The backfill's run has more values than one read of its value table
    // maps, and a build step adds a second run.
    let mut paths: Vec<String> = (0..300).map(|index| format!("/a-{index:03}.txt")).collect();
    for (index, path) in paths.iter().enumerate() {
        fixture
            .put(path, format!("runs needle {index}\n").as_bytes())
            .await;
    }
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    for index in 0..20 {
        let path = format!("/b-{index:02}.txt");
        fixture
            .put(&path, format!("runs needle later {index}\n").as_bytes())
            .await;
        paths.push(path);
    }
    fixture.build_step().await;

    let mut walked = Vec::new();
    let mut cursor = None;
    loop {
        let page = fixture.page("runs needle", cursor, 23).await;
        walked.extend(
            page.matches
                .iter()
                .map(|found| found.path.as_str().to_owned()),
        );
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    let paths: [&str; 320] = paths.try_into().expect("320 paths");
    assert_eq!(walked, in_walk_order(&fixture.namespace, paths).await);
}

#[tokio::test]
async fn a_file_deleted_at_the_cursors_sequence_stays_out_after_a_restore() {
    let fixture = ContentFixture::new("pages-restore").await;
    fixture.put("/a.txt", b"restore needle a\n").await;
    fixture.put("/b.txt", b"restore needle b\n").await;
    let [first, last] = in_walk_order(&fixture.namespace, ["/a.txt", "/b.txt"]).await;
    // The deleted file shares the last file's content, so its place in the
    // walk is after the first page's cursor.
    fixture.copy(&last, "/deleted.txt").await;
    let deleted = fixture
        .namespace
        .stat("/deleted.txt")
        .await
        .expect("stat the file")
        .inode_id;
    let deletion = fixture
        .namespace
        .delete_path("/deleted.txt", &loonfs_test_support::test_actor())
        .await
        .expect("delete");
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");

    let page_one = fixture.page("restore needle", None, 1).await;
    assert_eq!(page_one.matches[0].path, first.as_str());
    fixture
        .namespace
        .restore_deleted(
            deleted,
            deletion.committed_seq,
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("restore_deleted");
    let page_two = fixture
        .page("restore needle", page_one.next_cursor, 10)
        .await;
    assert_eq!(
        page_two
            .matches
            .iter()
            .map(|found| found.path.as_str())
            .collect::<Vec<_>>(),
        [last.as_str()],
        "the first page's cut hid the deleted file"
    );
}

#[tokio::test]
async fn a_cursor_from_another_build_of_the_index_is_invalid() {
    let fixture = ContentFixture::new("cursor-build").await;
    fixture.put("/one.txt", b"build needle one\n").await;
    fixture.put("/two.txt", b"build needle two\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let page_one = fixture.page("build needle", None, 1).await;
    let cursor = page_one.next_cursor.expect("a cursor");

    // Disabling and enabling grep again rebuilds the index from a backfill
    // at a later sequence, as a rebootstrap does.
    fixture.put("/three.txt", b"later\n").await;
    fixture
        .host
        .disable_grep_index(&fixture.namespace_id)
        .await
        .expect("disable");
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable again");
    // A new service loads the rebuilt index's manifest.
    let error = grep_with(
        &GrepService::new(default_block_cache()),
        &fixture.host.reader,
        &fixture.store,
        &fixture.namespace_id,
        &request("build needle"),
        page_request(Some(cursor), 1),
    )
    .await
    .expect_err("the cursor refers to the earlier build");
    assert_matches!(
        error,
        GrepError::Runtime(loonfs::Error::Core(loonfs::CoreError::InvalidCursor(_)))
    );
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn a_page_reads_the_bytes_of_at_most_its_budget_of_values() {
    let fixture = ContentFixture::new("value-budget").await;
    for index in 0..257 {
        fixture
            .put(
                &format!("/value-{index:03}.txt"),
                format!("a budget needle {index}\n").as_bytes(),
            )
            .await;
    }
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    fixture.content_reads();

    // Every value is a false positive, so the first page stops when it has
    // read 256 values, and the next page reads the last one.
    let page_one = fixture.page("budget needle.*z", None, 100).await;
    assert!(page_one.matches.is_empty());
    assert_eq!(fixture.content_reads(), 256);
    let page_two = fixture
        .page(
            "budget needle.*z",
            Some(page_one.next_cursor.expect("a cursor")),
            100,
        )
        .await;
    assert!(page_two.matches.is_empty());
    assert!(page_two.next_cursor.is_none());
    assert_eq!(fixture.content_reads(), 1);
}
