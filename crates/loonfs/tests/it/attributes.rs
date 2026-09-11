//! The runtime's attribute surface: the one-operation write convenience, and
//! what the read options project.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    AttributeRevisionNo, CommitId, CreateNamespaceOptions, ListPathEntriesOptions, PageRequest,
    PutFileOptions, StatPathOptions, UpdateAttributesOptions,
};
use loonfs_api::semantic_commit_fingerprint;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id, page_limit};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn owner_update() -> UpdateAttributesOptions {
    let mut options = UpdateAttributesOptions::new(loonfs_test_support::test_actor());
    options.set = BTreeMap::from([(attribute_key("owner"), attribute_text("platform"))]);
    options.commit.commit_id = Some(CommitId::parse("annotate-report").expect("commit id"));
    options
}

#[test]
fn maximum_small_attribute_updates_reopen_after_one_wal_publication() {
    use loonfs_api::{Attributes, MAX_ATTRIBUTES_TOTAL_BYTES};
    use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
    use std::sync::Arc;

    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("large-attributes");
    let counted = Arc::new(RecordingStore::new(
        store(temp_dir.path()),
        KeyPredicate::prefix(loonfs_objectstore::keys::wal_segment_prefix(&namespace_id)),
    ));
    let fs = open_runtime(counted.clone(), "large-attributes-writer");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/file",
        b"content",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("file");
    let mut set = BTreeMap::from([(attribute_key("x"), attribute_text("a"))]);
    let mut remaining = MAX_ATTRIBUTES_TOTAL_BYTES - 2 - 16 * 3;
    for index in 0..16 {
        let length = remaining.min(loonfs_api::MAX_ATTRIBUTE_VALUE_BYTES);
        remaining -= length;
        set.insert(
            attribute_key(&format!("k{index:02}")),
            attribute_text(&"v".repeat(length)),
        );
    }
    let initial = Attributes::new(set.clone()).expect("full map");
    assert_eq!(initial.logical_bytes(), MAX_ATTRIBUTES_TOTAL_BYTES);
    let mut options = UpdateAttributesOptions::new(loonfs_test_support::test_actor());
    options.set = set;
    block_on(fs.writer.update_attributes(&namespace_id, "/file", options))
        .expect("fill attributes");
    counted.reset();
    let response = fs
        .mutate_blocking(
            &namespace_id,
            CommitRequest {
                commit_id: CommitId::parse("maximum-attribute-updates").expect("commit"),
                actor_id: loonfs_test_support::test_actor(),
                message: None,
                preconditions: Vec::new(),
                operations: (0..loonfs::publish::MAX_COMMIT_OPERATIONS)
                    .map(|index| FilesystemOperation::UpdateAttributes {
                        path: parse_mutation_path("/file").expect("path"),
                        set: BTreeMap::from([(
                            attribute_key("x"),
                            attribute_text(if index % 2 == 0 { "b" } else { "c" }),
                        )]),
                        remove: Vec::new(),
                        expected_inode_id: None,
                        expected_attributes_revision_no: None,
                    })
                    .collect(),
            },
        )
        .expect("maximum updates fit one segment");
    assert_eq!(counted.count(OperationClass::PutCreateIfAbsent), 1);
    block_on(fs.writer.shutdown()).expect("shutdown");
    drop(fs);
    counted.reset();
    let reopened = open_runtime(counted.clone(), "fresh-reader-runtime");
    let entry = reopened
        .stat_path_blocking(&namespace_id, "/file")
        .expect("reopen committed state");
    let projection = entry.attributes.expect("attributes");
    assert_eq!(
        projection.attributes_revision_no,
        AttributeRevisionNo(1 + loonfs::publish::MAX_COMMIT_OPERATIONS as u64)
    );
    assert_eq!(
        projection.attributes.get(&attribute_key("x")),
        Some(&attribute_text("c"))
    );
    assert_eq!(
        projection.attributes.logical_bytes(),
        MAX_ATTRIBUTES_TOTAL_BYTES
    );
    assert!(response.committed_seq.0 > 0);
    assert!(counted.count(OperationClass::Read) > 0);
    assert_eq!(counted.count(OperationClass::Put), 0);
}

#[test]
fn the_write_convenience_matches_a_hand_built_one_operation_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "attributes-parity");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"body",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    let options = owner_update();
    let explicit = CommitRequest::single(
        options.commit.commit_id.clone().expect("commit id"),
        options.commit.actor_id.clone(),
        options.commit.message.clone(),
        FilesystemOperation::UpdateAttributes {
            path: parse_mutation_path("/docs/report.txt").expect("path"),
            set: options.set.clone(),
            remove: options.remove.clone(),
            expected_inode_id: options.expected_inode_id,
            expected_attributes_revision_no: options.expected_attributes_revision_no,
        },
    );
    let explicit_fingerprint = semantic_commit_fingerprint(
        &namespace_id,
        &explicit.actor_id,
        explicit.message.as_deref(),
        &explicit.operations,
        &[],
    )
    .expect("explicit fingerprint");

    // The convenience call lands the commit; resubmitting the explicit
    // request under the same id replays it instead of committing twice.
    // Replay is decided on the fingerprint, so this passing is the parity
    // statement: the convenience compiled into the same commit.
    let convenience = block_on(fs.writer.update_attributes(
        &namespace_id,
        "/docs/report.txt",
        options,
    ))
    .expect("convenience update");
    let replayed = fs
        .mutate_blocking(&namespace_id, explicit)
        .expect("the explicit request replays the convenience commit");
    assert_eq!(replayed.committed_seq, convenience.committed_seq);
    assert_eq!(replayed.commit_id, convenience.commit_id);

    // The negative control: reusing that id over a different update
    // conflicts, so the replay above was decided on the request and not on
    // the id alone.
    let different = CommitRequest::single(
        CommitId::parse("annotate-report").expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::UpdateAttributes {
            path: parse_mutation_path("/docs/report.txt").expect("path"),
            set: BTreeMap::from([(attribute_key("owner"), attribute_text("someone-else"))]),
            remove: Vec::new(),
            expected_inode_id: None,
            expected_attributes_revision_no: None,
        },
    );
    assert_ne!(
        semantic_commit_fingerprint(
            &namespace_id,
            &different.actor_id,
            different.message.as_deref(),
            &different.operations,
            &[]
        )
        .expect("different fingerprint"),
        explicit_fingerprint
    );
    let conflict = fs
        .mutate_blocking(&namespace_id, different)
        .expect_err("a different update under the same id conflicts");
    assert!(matches!(
        conflict,
        loonfs::RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::CommitIdReuseConflict
    ));
}

#[test]
fn a_write_is_visible_to_the_next_stat() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "attributes-round-trip");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"body",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    block_on(
        fs.writer
            .update_attributes(&namespace_id, "/docs/report.txt", owner_update()),
    )
    .expect("annotate");

    let entry = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat");
    assert_eq!(
        entry
            .attributes
            .as_ref()
            .map(|projection| projection.attributes_revision_no),
        Some(AttributeRevisionNo(1))
    );
    assert_eq!(
        entry
            .attributes
            .as_ref()
            .and_then(|projection| projection.attributes.get(&attribute_key("owner")))
            .cloned(),
        Some(attribute_text("platform"))
    );

    // Removing the only key leaves the cleared map, which is a real answer
    // at its own revision rather than an absent one.
    block_on(fs.writer.update_attributes(
        &namespace_id,
        "/docs/report.txt",
        UpdateAttributesOptions {
            remove: vec![attribute_key("owner")],
            ..UpdateAttributesOptions::new(loonfs_test_support::test_actor())
        },
    ))
    .expect("clear");
    let cleared = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat cleared");
    assert_eq!(
        cleared.attributes.as_ref().map(|projection| {
            (
                projection.attributes_revision_no,
                projection.attributes.len(),
            )
        }),
        Some((AttributeRevisionNo(2), 0))
    );
}

#[test]
fn read_options_project_grouped_attributes_or_none() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "attributes-projection");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/report.txt", "/docs/notes.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            b"body",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }
    block_on(
        fs.writer
            .update_attributes(&namespace_id, "/docs/report.txt", owner_update()),
    )
    .expect("annotate");

    // Stat includes attributes by default.
    let default_stat = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat");
    assert!(default_stat.attributes.is_some());

    let opted_out = block_on(fs.reader.get_path_entry(
        &namespace_id,
        "/docs/report.txt",
        StatPathOptions {
            include_attributes: loonfs_api::AttributeInclusion::Omit,
            snapshot_id: None,
        },
    ))
    .expect("stat without attributes");
    assert!(opted_out.attributes.is_none());

    // Listing omits attributes by default, and includes them on request.
    let default_listing = fs.list_path_blocking(&namespace_id, "/docs").expect("list");
    assert_eq!(default_listing.len(), 2);
    for entry in &default_listing {
        assert!(entry.attributes.is_none());
    }

    let projected = block_on(fs.reader.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(16),
            cursor: None,
        },
        ListPathEntriesOptions {
            include_attributes: loonfs_api::AttributeInclusion::Include,
            snapshot_id: None,
        },
    ))
    .expect("list with attributes");
    assert_eq!(projected.entries.len(), 2);
    for entry in &projected.entries {
        let projection = entry.attributes.as_ref().expect("projected attributes");
        match entry.path.as_str() {
            "/docs/report.txt" => {
                assert_eq!(projection.attributes_revision_no, AttributeRevisionNo(1));
                assert_eq!(
                    projection.attributes.get(&attribute_key("owner")).cloned(),
                    Some(attribute_text("platform"))
                );
            }
            // An inode nobody annotated projects the cleared state, not an
            // absent one.
            _ => {
                assert_eq!(projection.attributes_revision_no, AttributeRevisionNo(0));
                assert_eq!(projection.attributes.len(), 0);
            }
        }
    }
}
