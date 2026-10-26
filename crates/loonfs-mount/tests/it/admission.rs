//! Admitting a mount: the scan at a snapshot and the requests it makes, the
//! entry budget, and the checks that refuse a mount.

use crate::support::{engine_tree, fixture, mkdir, operations, path, put, Fixture, MOUNT_PATH};
use loonfs_mount::{MountAccess, MountError, Readiness};
use loonfs_mount_sim::{Operation, SimulatedServerConfig};
use loonfs_types::NamespaceNaming;
use std::assert_matches;

/// Commits 10 directories below the mount's root, with 10 subdirectories in
/// each and 10 files in each subdirectory, and returns the number of
/// entries: 1,110, which admission lists in two pages.
///
/// The simulated server checks every binding after each operation, so a
/// commit costs time in proportion to its operations times the square of the
/// namespace's rows, and a larger tree makes this test slow.
fn commit_nested_tree(fixture: &Fixture) -> u64 {
    for directory in 0..10 {
        let top = format!("d{directory}");
        let mut operations = vec![mkdir(&top, false)];
        for subdirectory in 0..10 {
            let sub = format!("{top}/s{subdirectory}");
            operations.push(mkdir(&sub, false));
            for file in 0..10 {
                let name = format!("{sub}/f{file}.txt");
                operations.push(put(&name, name.as_bytes()));
            }
        }
        fixture.commit(operations);
    }
    10 * (1 + 10 * (1 + 10))
}

/// Checks that admission reads the capabilities and the namespace, creates
/// a snapshot, reads the root and its two ancestors and lists the subtree at
/// it, and deletes it, with no other request, and that the tree it builds
/// equals a listing of the namespace at the sequence it applied.
#[tokio::test]
async fn a_nested_namespace_admits_at_a_snapshot_with_the_requests_the_note_describes() {
    let fixture = fixture();
    let entries = commit_nested_tree(&fixture);
    let head = fixture.head_seq();
    fixture.server.take_trace();
    let mut config = fixture.config();
    config.max_entries = entries;
    let engine = fixture.admit(config).await.expect("the tree fits");
    let trace = fixture.server.take_trace();
    let admission: Vec<_> = trace
        .iter()
        .take_while(|record| record.operation != Operation::ListChanges)
        .cloned()
        .collect();
    use Operation::*;
    assert_eq!(
        operations(&admission),
        [
            GetCapabilities,
            GetNamespace,
            CreateSnapshot,
            GetPathEntry,
            GetInode,
            GetInode,
            ListInodeChildren,
            ListInodeChildren,
            DeleteSnapshot,
        ]
    );
    assert!(
        trace[admission.len()..]
            .iter()
            .all(|record| record.operation == ListChanges),
        "the engine only polls the feed after admission"
    );
    let snapshot_id = admission[3]
        .query_param("snapshot_id")
        .expect("the root is read at the snapshot");
    assert_eq!(admission[3].query_param("path"), Some(MOUNT_PATH));
    for read in &admission[3..8] {
        assert_eq!(read.query_param("snapshot_id"), Some(snapshot_id));
        assert_eq!(read.query_param("include_attributes"), Some("true"));
    }
    let limits: Vec<_> = admission[6..8]
        .iter()
        .map(|page| {
            assert_eq!(page.query_param("recursive"), Some("true"));
            page.query_param("limit")
        })
        .collect();
    assert_eq!(
        limits,
        [Some("1000"), Some("111")],
        "pages of 1,000 entries, and no more than the budget plus one in all"
    );
    assert!(admission[8].path.ends_with(snapshot_id));
    let snapshots = fixture.server.snapshots(&fixture.namespace_id);
    assert!(
        snapshots.is_empty(),
        "admission deletes its snapshot: {snapshots:?}"
    );

    let status = engine.status();
    assert_eq!(
        (status.applied_seq, status.readiness, status.entries),
        (head, Readiness::Ready, entries + 1),
        "the applied sequence is the head that the snapshot captured"
    );
    assert_eq!(engine_tree(&engine), fixture.listed_tree().await);
}

/// Checks that a namespace with one entry more than the budget, and one with
/// many more, are refused after the scan listed the budget plus one entries,
/// that the snapshot is deleted, and that no tree is left.
#[tokio::test]
async fn a_namespace_over_the_budget_is_refused_after_listing_the_budget_plus_one() {
    let fixture = fixture();
    let operations: Vec<_> = (0..5)
        .flat_map(|directory| {
            let top = format!("d{directory}");
            let files: Vec<_> = (0..4)
                .map(|file| put(&format!("{top}/f{file}"), b"x"))
                .collect();
            std::iter::once(mkdir(&top, false)).chain(files)
        })
        .collect();
    fixture.commit(operations);
    for max_entries in [24, 10] {
        fixture.server.take_trace();
        let mut config = fixture.config();
        config.max_entries = max_entries;
        let refused = fixture.admit(config).await.expect_err("over the budget");
        assert_matches!(
            refused,
            MountError::EntryBudgetExceeded { listed_entries, max_entries: budget, .. }
                if listed_entries == max_entries + 1 && budget == max_entries
        );
        let trace = fixture.server.take_trace();
        let pages: Vec<_> = trace
            .iter()
            .filter(|record| record.operation == Operation::ListInodeChildren)
            .collect();
        let limit = (max_entries + 1).to_string();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].query_param("limit"), Some(limit.as_str()));
        assert_eq!(
            trace.last().map(|record| record.operation),
            Some(Operation::DeleteSnapshot)
        );
        let snapshots = fixture.server.snapshots(&fixture.namespace_id);
        assert!(snapshots.is_empty(), "{snapshots:?}");
        assert!(
            !fixture.has_tree_database(),
            "a refused mount leaves no tree"
        );
    }
}

/// Checks the refusals that don't depend on the tree's size: a `.loonfs`
/// child at the root, a root that's a file, and a writable mount of a
/// case-insensitive namespace, which a read-only mount accepts.
#[tokio::test]
async fn admission_refuses_a_reserved_child_a_file_and_a_writable_case_insensitive_namespace() {
    let reserved = fixture();
    reserved.commit(vec![mkdir(".loonfs", false)]);
    assert_matches!(
        reserved.admit(reserved.config()).await,
        Err(MountError::ReservedName { .. })
    );

    let file_root = fixture();
    file_root.commit(vec![put("readme.md", b"read me")]);
    let mut config = file_root.config();
    config.path = path("readme.md");
    assert_matches!(
        file_root.admit(config).await,
        Err(MountError::NotADirectory { .. })
    );

    let case_insensitive = Fixture::new(
        SimulatedServerConfig::default(),
        NamespaceNaming::CaseInsensitive,
    );
    let mut config = case_insensitive.config();
    config.access = MountAccess::Writable;
    assert_matches!(
        case_insensitive.admit(config).await,
        Err(MountError::CaseInsensitiveNaming { .. })
    );
    case_insensitive
        .admit(case_insensitive.config())
        .await
        .expect("a read-only mount of a case-insensitive namespace");
}
