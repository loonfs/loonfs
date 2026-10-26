//! Directory listings: depth-first order, cursors that resume across pages and across a move,
//! and the 128-level rule.

use crate::support::{fixture, mkdir, path, put, Fixture};
use loonfs_types::api::v0::FilesystemOperation;
use loonfs_types::{ListInodeChildrenResponse, ListPathEntriesResponse, PathEntry};

async fn listing(fixture: &Fixture, query: &str) -> ListPathEntriesResponse {
    let (status, _, body) = fixture
        .get(&format!("/v0/namespaces/demo/filesystem/entries?{query}"))
        .await;
    assert!(
        status.is_success(),
        "{query}: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).expect("a listing page")
}

fn paths(entries: &[PathEntry]) -> Vec<&str> {
    entries.iter().map(|entry| entry.path.as_str()).collect()
}

/// Lists `/` recursively in pages of `limit`, and returns every page's paths.
async fn pages(fixture: &Fixture, limit: u32) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut query = format!("path=%2F&recursive=true&limit={limit}");
        if let Some(cursor) = &cursor {
            query.push_str(&format!("&cursor={cursor}"));
        }
        let page = listing(fixture, &query).await;
        pages.push(
            paths(&page.entries)
                .into_iter()
                .map(str::to_owned)
                .collect(),
        );
        cursor = page.next_cursor;
        if cursor.is_none() {
            return pages;
        }
    }
}

#[tokio::test]
async fn a_recursive_listing_walks_depth_first_in_name_key_order_across_pages() {
    let fixture = fixture();
    fixture.commit(vec![
        put("/b.txt", b"b"),
        mkdir("/c"),
        put("/a/y/z.txt", b"z"),
        put("/a/x.txt", b"x"),
    ]);
    let expected = ["/a", "/a/x.txt", "/a/y", "/a/y/z.txt", "/b.txt", "/c"];
    let whole = listing(&fixture, "path=%2F&recursive=true").await;
    assert_eq!(paths(&whole.entries), expected);
    assert!(whole
        .entries
        .iter()
        .all(|entry| entry.head_seq == whole.head_seq));

    for limit in 1..=6 {
        let pages = pages(&fixture, limit).await;
        assert!(
            pages.iter().all(|page| page.len() <= limit as usize),
            "limit {limit}"
        );
        assert_eq!(pages.concat(), expected, "limit {limit}");
    }

    let children = listing(&fixture, "path=%2Fa").await;
    assert_eq!(paths(&children.entries), ["/a/x.txt", "/a/y"]);
    let file = listing(&fixture, "path=%2Fb.txt&recursive=true").await;
    assert_eq!(
        paths(&file.entries),
        ["/b.txt"],
        "a file target lists the file itself"
    );
}

#[tokio::test]
async fn a_cursor_continues_in_the_parent_of_a_directory_that_moved_away() {
    let fixture = fixture();
    fixture.commit(vec![
        put("/a/y/1", b"1"),
        put("/a/y/2", b"2"),
        put("/a/z", b"z"),
        mkdir("/elsewhere"),
    ]);
    let first = listing(&fixture, "path=%2Fa&recursive=true&limit=2").await;
    assert_eq!(paths(&first.entries), ["/a/y", "/a/y/1"]);
    let y = fixture.stat("/a/y").await.inode_id;

    fixture.commit(vec![FilesystemOperation::MovePath {
        source_path: path("/a/y"),
        destination_path: path("/elsewhere/y"),
        precondition: Default::default(),
    }]);
    let cursor = first.next_cursor.expect("a cursor");
    let rest = listing(
        &fixture,
        &format!("path=%2Fa&recursive=true&cursor={cursor}"),
    )
    .await;
    assert_eq!(
        paths(&rest.entries),
        ["/a/z"],
        "the walk resumes in `/a` after `y`"
    );

    let (status, _, body) = fixture
        .get(&format!(
            "/v0/namespaces/demo/inodes/{}/children?recursive=true",
            loonfs_types::public_inode_id::encode(y)
        ))
        .await;
    assert!(status.is_success());
    let children: ListInodeChildrenResponse = serde_json::from_slice(&body).expect("a listing");
    assert_eq!(
        paths(&children.entries),
        ["/elsewhere/y/1", "/elsewhere/y/2"]
    );

    let (status, _, _) = fixture
        .get(&format!(
            "/v0/namespaces/demo/filesystem/entries?path=%2Felsewhere&cursor={cursor}"
        ))
        .await;
    assert_eq!(
        status.as_u16(),
        400,
        "a recursive cursor doesn't resume another listing"
    );
}

#[tokio::test]
async fn a_recursive_listing_stops_descending_128_levels_below_the_listed_directory() {
    let fixture = fixture();
    let deep = format!("/{}", vec!["d"; 128].join("/"));
    fixture.commit(vec![
        FilesystemOperation::CreateDirectory {
            path: path(&deep),
            parents: true,
        },
        mkdir("/top"),
        FilesystemOperation::MovePath {
            source_path: path("/d"),
            destination_path: path("/top/d"),
            precondition: Default::default(),
        },
    ]);
    let page = listing(&fixture, "path=%2F&recursive=true").await;
    let depths: Vec<usize> = page
        .entries
        .iter()
        .map(|entry| entry.path.components().len())
        .collect();
    assert_eq!(
        depths,
        (1..=128).collect::<Vec<_>>(),
        "the entry at depth 129 isn't listed"
    );

    let deepest = page.entries.last().expect("an entry").inode_id;
    let (_, _, body) = fixture
        .get(&format!(
            "/v0/namespaces/demo/inodes/{}/children",
            loonfs_types::public_inode_id::encode(deepest)
        ))
        .await;
    let below: ListInodeChildrenResponse = serde_json::from_slice(&body).expect("a listing");
    assert_eq!(
        below.entries.len(),
        1,
        "a client lists the directory at the limit by inode"
    );
}
