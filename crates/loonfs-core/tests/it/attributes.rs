//! Inode attributes end to end: what the planner accepts, what the commit
//! guards reject, and what survives a flush, a fork, and every operation that
//! moves an inode around.
//!
//! There is no attribute read API yet, so these read attributes back through
//! the change feed — which is also what pins the feed's own contract — and
//! through the revision number a later update publishes, which is only right
//! if the write path read the current state from where it was stored.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use loonfs_api::{
    v0::FilesystemChange, AbsolutePath, AttributeKey, AttributeRevisionNo, AttributeValue,
    Attributes, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId,
    NamespaceId, RevisionNo, MAX_ATTRIBUTES_TOTAL_BYTES, MAX_ATTRIBUTE_VALUE_BYTES,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::publish::{CommitRequest, FilesystemOperation};
use loonfs_core::{Error as CoreError, ErrorCode, MutationContext};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use std::collections::BTreeMap;
use tempfile::tempdir;

fn mutation_context() -> MutationContext {
    crate::common::mutation_context("writer", 1)
}

fn commit_id(value: &str) -> CommitId {
    CommitId::parse(value).expect("valid commit id")
}

fn path(value: &str) -> AbsolutePath {
    AbsolutePath::parse(value).expect("valid path")
}

fn key(value: &str) -> AttributeKey {
    AttributeKey::parse(value).expect("valid attribute key")
}

fn text(value: &str) -> AttributeValue {
    AttributeValue::parse(value).expect("valid attribute value")
}

fn map(entries: &[(&str, &str)]) -> Attributes {
    Attributes::new(
        entries
            .iter()
            .map(|(name, value)| (key(name), text(value)))
            .collect(),
    )
    .expect("valid attribute map")
}

fn set_attributes(target: &str, entries: &[(&str, &str)]) -> FilesystemOperation {
    FilesystemOperation::UpdateAttributes {
        path: path(target),
        set: entries
            .iter()
            .map(|(name, value)| (key(name), text(value)))
            .collect(),
        remove: Vec::new(),
        expected_inode_id: None,
        expected_attributes_revision_no: None,
    }
}

fn remove_attributes(target: &str, names: &[&str]) -> FilesystemOperation {
    FilesystemOperation::UpdateAttributes {
        path: path(target),
        set: BTreeMap::new(),
        remove: names.iter().map(|name| key(name)).collect(),
        expected_inode_id: None,
        expected_attributes_revision_no: None,
    }
}

async fn setup() -> (
    tempfile::TempDir,
    LocalFsStore,
    NamespaceId,
    MutationContext,
) {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    (temp_dir, store, namespace_id, context)
}

/// Runs one update as its own commit.
async fn update<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    id: &str,
    operation: FilesystemOperation,
    context: &MutationContext,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_operation(store, namespace_id, commit_id(id), operation, context).await
}

/// Every attribute event the feed still holds, oldest first.
///
/// A fork's target starts with its retention floor at the basis it inherited,
/// so the read starts there rather than at zero.
async fn attribute_events<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<(InodeId, AttributeRevisionNo, Attributes)> {
    let after_seq = loonfs_core::cache::load_namespace_head_summary(store, namespace_id)
        .await
        .expect("namespace head summary")
        .retention_floor_seq;
    list_changes_after(store, namespace_id, after_seq)
        .await
        .expect("read the change feed")
        .changes
        .into_iter()
        .flat_map(|change| change.events)
        .filter_map(|event| match event {
            FilesystemChange::AttributesChanged {
                inode_id,
                attributes_revision_no,
                attributes,
            } => Some((inode_id, attributes_revision_no, attributes)),
            _ => None,
        })
        .collect()
}

/// The attribute map the feed says an inode currently holds, folded from the
/// events in order. Absent when the inode never had one published.
async fn attributes_of<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Option<(AttributeRevisionNo, Attributes)> {
    attribute_events(store, namespace_id)
        .await
        .into_iter()
        .rfind(|(event_inode_id, _, _)| *event_inode_id == inode_id)
        .map(|(_, revision, attributes)| (revision, attributes))
}

async fn inode_of<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> InodeId {
    resolve_path(store, namespace_id, absolute_path)
        .await
        .expect("resolve path")
        .inode_id
}

// ---------------------------------------------------------------------------
// What the planner accepts and rejects
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_update_writes_attributes_on_a_file_and_on_a_directory() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");

    update(
        &store,
        &namespace_id,
        "set-on-file",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("attributes on a file");
    update(
        &store,
        &namespace_id,
        "set-on-directory",
        set_attributes("/docs", &[("owner", "grace")]),
        &context,
    )
    .await
    .expect("attributes on a directory");

    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;
    let directory_inode = inode_of(&store, &namespace_id, "/docs").await;
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "ada")])))
    );
    assert_eq!(
        attributes_of(&store, &namespace_id, directory_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "grace")])))
    );
}

#[tokio::test]
async fn an_empty_string_is_stored_until_the_key_is_removed() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;

    update(
        &store,
        &namespace_id,
        "set-empty",
        set_attributes("/docs/a.txt", &[("owner", "")]),
        &context,
    )
    .await
    .expect("store the empty string");
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "")])))
    );

    update(
        &store,
        &namespace_id,
        "remove-empty",
        remove_attributes("/docs/a.txt", &["owner"]),
        &context,
    )
    .await
    .expect("remove the key explicitly");
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(2), Attributes::default()))
    );
}

#[tokio::test]
async fn an_update_rejects_the_root_and_a_missing_path() {
    let (_temp_dir, store, namespace_id, context) = setup().await;

    let root = update(
        &store,
        &namespace_id,
        "set-on-root",
        set_attributes("/", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect_err("the root cannot be mutated");
    assert_eq!(root.code(), ErrorCode::InvalidRequest);

    let missing = update(
        &store,
        &namespace_id,
        "set-on-missing",
        set_attributes("/missing.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect_err("a missing path has no inode to write to");
    assert_eq!(missing.code(), ErrorCode::PathNotFound);
}

#[tokio::test]
async fn an_update_rejects_a_request_that_does_not_describe_one_change() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");

    let cases: Vec<(&str, FilesystemOperation, &str)> = vec![
        (
            "overlap",
            FilesystemOperation::UpdateAttributes {
                path: path("/docs/a.txt"),
                set: BTreeMap::from([(key("owner"), text("ada"))]),
                remove: vec![key("owner")],
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            },
            "both set and removed",
        ),
        (
            "duplicate-remove",
            FilesystemOperation::UpdateAttributes {
                path: path("/docs/a.txt"),
                set: BTreeMap::new(),
                remove: vec![key("owner"), key("owner")],
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            },
            "removed more than once",
        ),
        (
            "empty",
            FilesystemOperation::UpdateAttributes {
                path: path("/docs/a.txt"),
                set: BTreeMap::new(),
                remove: Vec::new(),
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            },
            "sets no attribute and removes none",
        ),
        (
            "reserved-set",
            set_attributes("/docs/a.txt", &[("loonfs.kind", "file")]),
            "system-owned",
        ),
        (
            "reserved-remove",
            remove_attributes("/docs/a.txt", &["loonfs.kind"]),
            "system-owned",
        ),
    ];

    for (label, operation, expected_message) in cases {
        let error = match update(&store, &namespace_id, label, operation, &context).await {
            Ok(_) => panic!("`{label}` must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::InvalidRequest, "for `{label}`");
        assert!(
            error.to_string().contains(expected_message),
            "for `{label}`: {error}"
        );
    }
}

/// An update that leaves the map exactly as it was publishes nothing. This is
/// the rule that differs from put: an identical-content put appends a
/// revision because a file's revisions are its history.
#[tokio::test]
async fn an_update_that_changes_nothing_is_rejected() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &namespace_id,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("first write");

    for (label, operation) in [
        (
            "same-value",
            set_attributes("/docs/a.txt", &[("owner", "ada")]),
        ),
        (
            "remove-absent",
            remove_attributes("/docs/a.txt", &["never-written"]),
        ),
    ] {
        let error = match update(&store, &namespace_id, label, operation, &context).await {
            Ok(_) => panic!("`{label}` must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::InvalidRequest, "for `{label}`");
        assert!(
            error.to_string().contains("unchanged"),
            "for `{label}`: {error}"
        );
    }

    // A first write on an inode that has no attributes is not a no-op even
    // though it removes nothing: the map goes from empty to populated.
    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "ada")]))),
        "the rejected updates published nothing"
    );
}

/// The limits are checked against the map the update produces, not against
/// what it carries: one small value can push an already-large map over a cap.
#[tokio::test]
async fn an_update_is_rejected_when_the_resulting_map_breaks_a_limit() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");

    // Fifteen maximum-length values sit just under the total; a sixteenth
    // pushes the resulting map over it.
    let filler: Vec<(String, String)> = (0..15)
        .map(|index| {
            (
                format!("k{index:02}"),
                "v".repeat(MAX_ATTRIBUTE_VALUE_BYTES),
            )
        })
        .collect();
    let filler_pairs: Vec<(&str, &str)> = filler
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    update(
        &store,
        &namespace_id,
        "fill",
        set_attributes("/docs/a.txt", &filler_pairs),
        &context,
    )
    .await
    .expect("a map under the total is accepted");

    let one_more = "v".repeat(MAX_ATTRIBUTE_VALUE_BYTES);
    let error = update(
        &store,
        &namespace_id,
        "over-cap",
        set_attributes("/docs/a.txt", &[("k15", one_more.as_str())]),
        &context,
    )
    .await
    .expect_err("the resulting map breaks the total");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert!(
        error
            .to_string()
            .contains(&MAX_ATTRIBUTES_TOTAL_BYTES.to_string()),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// Race guards
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_caller_supplied_stale_expectation_reports_expected_and_actual() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &namespace_id,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("first write");

    let error = update(
        &store,
        &namespace_id,
        "stale-expectation",
        FilesystemOperation::UpdateAttributes {
            path: path("/docs/a.txt"),
            set: BTreeMap::from([(key("owner"), text("grace"))]),
            remove: Vec::new(),
            expected_inode_id: None,
            expected_attributes_revision_no: Some(AttributeRevisionNo(0)),
        },
        &context,
    )
    .await
    .expect_err("the stated revision is no longer current");

    assert_eq!(error.code(), ErrorCode::StaleAttributes);
    let details = error.details().expect("stale attributes carries details");
    assert_eq!(
        details.expected_attributes_revision_no,
        Some(AttributeRevisionNo(0))
    );
    assert_eq!(
        details.actual_attributes_revision_no,
        Some(AttributeRevisionNo(1))
    );
}

/// A caller that names the inode it means gets the same protection without
/// waiting for the commit guards.
#[tokio::test]
async fn a_wrong_expected_inode_is_rejected() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");

    let error = update(
        &store,
        &namespace_id,
        "wrong-inode",
        FilesystemOperation::UpdateAttributes {
            path: path("/docs/a.txt"),
            set: BTreeMap::from([(key("owner"), text("ada"))]),
            remove: Vec::new(),
            expected_inode_id: Some(InodeId(999)),
            expected_attributes_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("the path resolves to another inode");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

// ---------------------------------------------------------------------------
// Multi-operation requests
// ---------------------------------------------------------------------------

/// The update resolves against what the put in the same request did, and both
/// land at one sequence.
#[tokio::test]
async fn a_put_and_an_update_of_the_new_path_commit_together() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");

    let response = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("put-then-set"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                FilesystemOperation::PutFile {
                    path: path("/docs/a.txt"),
                    content_ref: content.content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_revision_no: None,
                },
                set_attributes("/docs/a.txt", &[("owner", "ada")]),
            ],
        },
        &context,
    )
    .await
    .expect("the batch commits");
    assert_eq!(response.committed_seq, ChangeSeq(1));

    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "ada")])))
    );
}

/// Two updates of one key in one request advance the counter twice, so the
/// second observed what the first wrote.
#[tokio::test]
async fn two_updates_in_one_request_advance_the_revision_twice() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");

    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("twice"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                set_attributes("/docs/a.txt", &[("owner", "ada")]),
                set_attributes("/docs/a.txt", &[("owner", "grace")]),
            ],
        },
        &context,
    )
    .await
    .expect("the batch commits");

    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;
    let events: Vec<_> = attribute_events(&store, &namespace_id)
        .await
        .into_iter()
        .filter(|(inode_id, _, _)| *inode_id == file_inode)
        .map(|(_, revision, attributes)| (revision, attributes))
        .collect();
    assert_eq!(
        events,
        vec![
            (AttributeRevisionNo(1), map(&[("owner", "ada")])),
            (AttributeRevisionNo(2), map(&[("owner", "grace")])),
        ],
        "internal-op order survives into the feed"
    );
}

#[tokio::test]
async fn a_request_that_stops_at_a_bad_update_publishes_nothing() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");

    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("stops"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                set_attributes("/docs/a.txt", &[("owner", "ada")]),
                set_attributes("/missing.txt", &[("owner", "grace")]),
            ],
        },
        &context,
    )
    .await
    .expect_err("the second operation cannot resolve");
    assert_eq!(error.code(), ErrorCode::PathNotFound);
    assert_eq!(
        error
            .details()
            .expect("a stopped batch carries details")
            .operation_index,
        Some(1)
    );

    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        None,
        "the accepted operation of a stopped request publishes nothing"
    );
}

// ---------------------------------------------------------------------------
// Durability: across a flush, and across a fork
// ---------------------------------------------------------------------------

/// After a flush the attributes live in metadata tables rather than in the
/// replayed WAL tail. The next update's revision number is what says the
/// write path read them from there: a lost map would restart the counter.
#[tokio::test]
async fn attributes_survive_a_flush_and_the_counter_keeps_going() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &namespace_id,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("first write");
    update(
        &store,
        &namespace_id,
        "set-stage",
        set_attributes("/docs/a.txt", &[("stage", "draft")]),
        &context,
    )
    .await
    .expect("second write");

    namespace_engine(&store, &namespace_id, &context)
        .flush_wal()
        .await
        .expect("flush the WAL tail into metadata tables");

    // A repeat of the current state is still a no-op, which it could only be
    // if the flushed map is what the planner read.
    let no_op = update(
        &store,
        &namespace_id,
        "repeat-after-flush",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect_err("the flushed map is unchanged by this update");
    assert!(no_op.to_string().contains("unchanged"), "{no_op}");

    update(
        &store,
        &namespace_id,
        "set-after-flush",
        set_attributes("/docs/a.txt", &[("owner", "grace")]),
        &context,
    )
    .await
    .expect("an update after the flush commits");

    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((
            AttributeRevisionNo(3),
            map(&[("owner", "grace"), ("stage", "draft")])
        )),
        "the counter continues from the flushed revision and the map is whole"
    );
}

/// A fork reads its source's basis until it publishes a manifest of its own,
/// and its first flush carries the inherited rows forward.
#[tokio::test]
async fn a_fork_reads_the_sources_attributes_before_and_after_its_first_flush() {
    let (_temp_dir, store, source, context) = setup().await;
    put_file_bytes(
        &store,
        &source,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &source,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("write attributes in the source");
    namespace_engine(&store, &source, &context)
        .flush_wal()
        .await
        .expect("flush the source");

    let target = namespace_id("fork");
    namespace_engine(&store, &source, &context)
        .fork_namespace(&target)
        .await
        .expect("fork the namespace");

    // Before the fork's first flush, its basis is the source's manifest.
    let inherited = update(
        &store,
        &target,
        "repeat-in-fork",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect_err("the fork already holds the source's map");
    assert!(inherited.to_string().contains("unchanged"), "{inherited}");

    update(
        &store,
        &target,
        "set-in-fork",
        set_attributes("/docs/a.txt", &[("owner", "grace")]),
        &context,
    )
    .await
    .expect("the fork writes over the inherited map");
    namespace_engine(&store, &target, &context)
        .flush_wal()
        .await
        .expect("flush the fork");

    update(
        &store,
        &target,
        "set-after-fork-flush",
        set_attributes("/docs/a.txt", &[("stage", "draft")]),
        &context,
    )
    .await
    .expect("an update after the fork's flush commits");

    let file_inode = inode_of(&store, &target, "/docs/a.txt").await;
    assert_eq!(
        attributes_of(&store, &target, file_inode).await,
        Some((
            AttributeRevisionNo(3),
            map(&[("owner", "grace"), ("stage", "draft")])
        )),
        "the fork's own flush carries the inherited revision forward"
    );

    // The source is untouched by what the fork wrote.
    let source_inode = inode_of(&store, &source, "/docs/a.txt").await;
    assert_eq!(
        attributes_of(&store, &source, source_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "ada")])))
    );
}

// ---------------------------------------------------------------------------
// What every other operation does to attributes
// ---------------------------------------------------------------------------

/// Attributes travel with inode identity, so nothing that moves an inode or
/// changes its content touches them.
#[tokio::test]
async fn move_rename_replace_and_restore_preserve_attributes() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"first",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &namespace_id,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("write attributes");
    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;

    update(
        &store,
        &namespace_id,
        "move",
        FilesystemOperation::MovePath {
            from_path: path("/docs/a.txt"),
            to_path: path("/moved.txt"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("move the file");
    write_file_bytes(
        &store,
        &namespace_id,
        "/moved.txt",
        b"second",
        &context,
        Some("replace"),
    )
    .await
    .expect("replace the content");
    update(
        &store,
        &namespace_id,
        "restore",
        FilesystemOperation::RestoreRevision {
            path: path("/moved.txt"),
            source_revision_no: RevisionNo(1),
        },
        &context,
    )
    .await
    .expect("restore an older revision");

    assert_eq!(
        inode_of(&store, &namespace_id, "/moved.txt").await,
        file_inode,
        "the inode is the same one throughout"
    );
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "ada")]))),
        "nothing that moves or rewrites a file publishes an attribute event"
    );
}

/// A delete hides an inode without touching its attributes, and an undelete
/// gives back the same map at the same revision.
#[tokio::test]
async fn a_delete_keeps_attributes_and_an_undelete_gives_them_back() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &namespace_id,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("write attributes");
    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;

    let deleted = update(
        &store,
        &namespace_id,
        "delete",
        FilesystemOperation::DeletePath {
            path: path("/docs/a.txt"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect("delete the file");
    // A deleted inode is invisible, so nothing can write its attributes.
    let hidden = update(
        &store,
        &namespace_id,
        "set-while-deleted",
        set_attributes("/docs/a.txt", &[("owner", "grace")]),
        &context,
    )
    .await
    .expect_err("a deleted path resolves to nothing");
    assert_eq!(hidden.code(), ErrorCode::PathNotFound);

    update(
        &store,
        &namespace_id,
        "undelete",
        FilesystemOperation::Undelete {
            inode_id: file_inode,
            deletion_seq: deleted.committed_seq,
            path: None,
        },
        &context,
    )
    .await
    .expect("undelete the file");

    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "ada")]))),
        "the recovered inode holds the map it had"
    );
    // The map is the live one, so restating it is still a no-op.
    let no_op = update(
        &store,
        &namespace_id,
        "repeat-after-undelete",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect_err("undelete revealed the same map");
    assert!(no_op.to_string().contains("unchanged"), "{no_op}");
}

/// A copy to a vacant destination is a new resource standing for the source,
/// so it starts with the source's attributes — as its own internal operation
/// with its own event.
#[tokio::test]
async fn a_copy_to_a_vacant_destination_inherits_the_sources_attributes() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &namespace_id,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada"), ("stage", "draft")]),
        &context,
    )
    .await
    .expect("write attributes");

    let response = update(
        &store,
        &namespace_id,
        "copy",
        FilesystemOperation::CopyPath {
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("copy the file");

    let copy_inode = inode_of(&store, &namespace_id, "/docs/b.txt").await;
    assert_eq!(
        attributes_of(&store, &namespace_id, copy_inode).await,
        Some((
            AttributeRevisionNo(1),
            map(&[("owner", "ada"), ("stage", "draft")])
        ))
    );

    // The copy's commit reports two events: the creation, then the inherited
    // attributes, in that order.
    let copy_events: Vec<&'static str> = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("read the change feed")
        .changes
        .into_iter()
        .find(|change| change.committed_seq == response.committed_seq)
        .expect("the copy's commit")
        .events
        .iter()
        .map(|event| match event {
            FilesystemChange::FileCreated { .. } => "file_created",
            FilesystemChange::AttributesChanged { .. } => "attributes_changed",
            other => panic!("unexpected event: {other:?}"),
        })
        .collect();
    assert_eq!(copy_events, vec!["file_created", "attributes_changed"]);
}

/// A copy with nothing to inherit publishes no attribute event at all.
#[tokio::test]
async fn a_copy_of_a_file_without_attributes_publishes_no_attribute_event() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");

    update(
        &store,
        &namespace_id,
        "copy",
        FilesystemOperation::CopyPath {
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("copy the file");

    assert!(
        attribute_events(&store, &namespace_id).await.is_empty(),
        "an empty source map is nothing to carry over"
    );
}

/// Copying over an existing file changes its content and nothing else: the
/// destination is a resource that already exists and keeps what it holds.
#[tokio::test]
async fn a_copy_over_an_existing_file_leaves_its_attributes_alone() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    for (path_value, commit) in [
        ("/docs/a.txt", "seed-source"),
        ("/docs/b.txt", "seed-target"),
    ] {
        put_file_bytes(
            &store,
            &namespace_id,
            path_value,
            b"hello",
            DestinationBehavior::NoReplace,
            &context,
            Some(commit),
        )
        .await
        .expect("seed file");
    }
    update(
        &store,
        &namespace_id,
        "set-source",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("write source attributes");
    update(
        &store,
        &namespace_id,
        "set-target",
        set_attributes("/docs/b.txt", &[("owner", "grace")]),
        &context,
    )
    .await
    .expect("write destination attributes");
    let target_inode = inode_of(&store, &namespace_id, "/docs/b.txt").await;

    update(
        &store,
        &namespace_id,
        "copy-over",
        FilesystemOperation::CopyPath {
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            behavior: DestinationBehavior::Replace,
        },
        &context,
    )
    .await
    .expect("copy over the existing file");

    assert_eq!(
        attributes_of(&store, &namespace_id, target_inode).await,
        Some((AttributeRevisionNo(1), map(&[("owner", "grace")]))),
        "the destination keeps its own attributes"
    );
}

/// Removing every key is a real update publishing the empty map, and the
/// counter advances for it.
#[tokio::test]
async fn clearing_every_attribute_publishes_the_empty_map() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed file");
    update(
        &store,
        &namespace_id,
        "set-owner",
        set_attributes("/docs/a.txt", &[("owner", "ada")]),
        &context,
    )
    .await
    .expect("write attributes");

    update(
        &store,
        &namespace_id,
        "clear",
        remove_attributes("/docs/a.txt", &["owner"]),
        &context,
    )
    .await
    .expect("clear the map");

    let file_inode = inode_of(&store, &namespace_id, "/docs/a.txt").await;
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(2), Attributes::default()))
    );
    // Clearing again changes nothing, so it is rejected like any other no-op.
    let no_op = update(
        &store,
        &namespace_id,
        "clear-again",
        remove_attributes("/docs/a.txt", &["owner"]),
        &context,
    )
    .await
    .expect_err("the map is already empty");
    assert!(no_op.to_string().contains("unchanged"), "{no_op}");

    // A cleared map is a real state that survives a flush.
    namespace_engine(&store, &namespace_id, &context)
        .flush_wal()
        .await
        .expect("flush");
    update(
        &store,
        &namespace_id,
        "set-after-clear",
        set_attributes("/docs/a.txt", &[("owner", "hopper")]),
        &context,
    )
    .await
    .expect("a write after the clear commits");
    assert_eq!(
        attributes_of(&store, &namespace_id, file_inode).await,
        Some((AttributeRevisionNo(3), map(&[("owner", "hopper")]))),
        "the cleared revision is what the write built on"
    );
}
