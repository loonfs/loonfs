//! Change feed pages that end inside a commit, with `split_commits`.

use super::{list_changes_within, CommitSplitting};
use crate::commit::engine::CommitCandidate;
use crate::error::{CoreError, ErrorCode};
use crate::heap_bytes::HeapBytes;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::test_support::{namespace_store, writer_at, NamespaceFixture};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_types::{
    ChangeFeedPosition, ChangeSeq, Commit, CommitId, DisplayName, EffectiveLimit, NamespaceId,
    ROOT_INODE_ID,
};
use std::assert_matches;
use tempfile::TempDir;

/// A namespace with three commits: one directory creation, a commit that
/// creates `LARGE` directories, and another directory creation.
struct Feed {
    _directory: TempDir,
    store: LocalFsStore,
    namespace_id: NamespaceId,
    commits: Vec<Commit>,
}

const LARGE: usize = 40;

impl Feed {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = NamespaceId::parse("feed").expect("namespace");
        let writer = writer_at("writer", 1_000);
        let fixture = NamespaceFixture::new(&store, &namespace_id, &writer);
        fixture.create().await.expect("create");
        let mut commits = Vec::new();
        for (commit_id, count) in [("before", 1), ("large", LARGE), ("after", 1)] {
            let operations = (0..count)
                .map(|index| FilesystemOperation::CreateDirectoryByInode {
                    parent_inode_id: ROOT_INODE_ID,
                    display_name: DisplayName::parse(format!("{commit_id}-{index}")).expect("name"),
                })
                .collect();
            let intent = CommitIntent {
                commit_id: CommitId::parse(commit_id).expect("commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations,
                preconditions: Vec::new(),
            };
            commits.push(
                fixture
                    .publish_candidates(vec![CommitCandidate::new(intent)])
                    .await
                    .pop()
                    .expect("one result")
                    .expect("publish"),
            );
        }
        Self {
            _directory: directory,
            store,
            namespace_id,
            commits,
        }
    }

    /// A page budget that the large commit exceeds about four times over,
    /// and that each small commit fits.
    fn budget(&self) -> usize {
        self.commits[1].events.heap_bytes() / 4
    }

    async fn page(
        &self,
        position: ChangeFeedPosition,
        splitting: CommitSplitting,
        budget: usize,
    ) -> crate::error::Result<loonfs_types::ListChangesResponse> {
        let namespace = namespace_store(&self.store, &self.namespace_id);
        let view = namespace.load_current_metadata_view().await.expect("view");
        list_changes_within(&view, position, EffectiveLimit::MAX, splitting, budget).await
    }

    /// Reads the feed from the start in pages, and returns the pages.
    async fn pages(
        &self,
        splitting: CommitSplitting,
        budget: usize,
    ) -> Vec<loonfs_types::ListChangesResponse> {
        let mut position = ChangeFeedPosition::after(ChangeSeq(0));
        let mut pages = Vec::new();
        loop {
            let page = self
                .page(position, splitting, budget)
                .await
                .expect("a page");
            let next = page.next_after_seq.map(|after_seq| ChangeFeedPosition {
                after_seq,
                event_index: page.next_event_index.unwrap_or(0),
            });
            pages.push(page);
            match next {
                Some(next) => position = next,
                None => return pages,
            }
        }
    }
}

/// The commits of `pages` with the events of each split commit joined, in
/// page order, and the shape of each page: its commits' sequences and event
/// counts, and its `next_event_index`.
fn joined(pages: &[loonfs_types::ListChangesResponse]) -> (Vec<Commit>, Vec<String>) {
    let mut commits: Vec<Commit> = Vec::new();
    let mut shapes = Vec::new();
    for page in pages {
        let mut shape = Vec::new();
        for change in &page.changes {
            shape.push(format!("{}:{}", change.committed_seq, change.events.len()));
            match commits.last_mut() {
                Some(last) if last.committed_seq == change.committed_seq => {
                    last.events.extend(change.events.iter().cloned());
                }
                _ => commits.push(change.clone()),
            }
        }
        shapes.push(format!(
            "{} next={:?}",
            shape.join(" "),
            page.next_event_index
        ));
    }
    (commits, shapes)
}

/// Checks that a commit larger than the page budget is returned across
/// pages with `split_commits`, that each page that ends inside it reports
/// the next event's index, that the page with the commit's last events
/// continues with the next commit, and that the joined events equal the
/// commit read whole.
#[tokio::test]
async fn a_commit_over_the_page_budget_is_returned_across_pages() {
    let feed = Feed::new().await;
    let pages = feed
        .pages(CommitSplitting::AtPageBudget, feed.budget())
        .await;
    let (commits, shapes) = joined(&pages);
    assert_eq!(commits, feed.commits);
    let [before, large, after] = [0, 1, 2].map(|index| feed.commits[index].committed_seq);
    assert_eq!(shapes.first(), Some(&format!("{before}:1 next=None")));
    assert!(pages.len() > 3, "{shapes:?}");
    for page in &pages[1..pages.len() - 1] {
        assert_eq!(page.next_after_seq, Some(large), "{shapes:?}");
        assert!(page.next_event_index.is_some(), "{shapes:?}");
    }
    let last = pages.last().expect("a last page");
    assert_eq!(
        last.changes
            .iter()
            .map(|change| change.committed_seq)
            .collect::<Vec<_>>(),
        [large, after],
        "{shapes:?}"
    );
    assert_eq!(last.next_after_seq, None);
}

/// Checks that without `split_commits` the same budget returns the large
/// commit whole, on a page of its own, as the feed always has.
#[tokio::test]
async fn without_split_commits_a_large_commit_is_a_page_of_its_own() {
    let feed = Feed::new().await;
    let pages = feed.pages(CommitSplitting::Whole, feed.budget()).await;
    let (commits, shapes) = joined(&pages);
    assert_eq!(commits, feed.commits);
    let [before, large, after] = [0, 1, 2].map(|index| feed.commits[index].committed_seq);
    assert_eq!(
        shapes,
        [
            format!("{before}:1 next=None"),
            format!("{large}:{LARGE} next=None"),
            format!("{after}:1 next=None"),
        ]
    );
}

/// Checks that a commit splits at the same events whether the feed reads
/// it from the WAL tail or from its commit row after a fold.
#[tokio::test]
async fn a_commit_splits_the_same_way_before_and_after_a_fold() {
    let feed = Feed::new().await;
    let budget = feed.budget();
    let before = feed.pages(CommitSplitting::AtPageBudget, budget).await;
    namespace_store(&feed.store, &feed.namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let after = feed.pages(CommitSplitting::AtPageBudget, budget).await;
    assert_eq!(joined(&before).1, joined(&after).1);
    assert_eq!(before, after);
}

/// Checks that a position inside a commit is refused without
/// `split_commits`, and at or past the end of the commit's events, with
/// `invalid_request`.
#[tokio::test]
async fn an_event_index_past_the_end_or_without_split_commits_is_refused() {
    let feed = Feed::new().await;
    let [small, large] = [0, 1].map(|index| feed.commits[index].committed_seq);
    let past_large = u32::try_from(LARGE).expect("index");
    // An index far past a commit's last event is refused as soon as the
    // commit's events run out, without stepping through every index.
    for (after_seq, event_index, splitting) in [
        (large, 1, CommitSplitting::Whole),
        (large, past_large, CommitSplitting::AtPageBudget),
        (large, past_large + 5, CommitSplitting::AtPageBudget),
        (small, u32::MAX, CommitSplitting::AtPageBudget),
    ] {
        let position = ChangeFeedPosition {
            after_seq,
            event_index,
        };
        let error = feed
            .page(position, splitting, feed.budget())
            .await
            .expect_err("an invalid event index");
        assert_matches!(error, CoreError::InvalidEventIndex(_), "{event_index}");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }
    let resumed = feed
        .page(
            ChangeFeedPosition {
                after_seq: large,
                event_index: u32::try_from(LARGE - 1).expect("index"),
            },
            CommitSplitting::AtPageBudget,
            usize::MAX,
        )
        .await
        .expect("the large commit's last event");
    assert_eq!(resumed.changes[0].events.len(), 1);
    assert_eq!(resumed.changes.len(), 2);
}
