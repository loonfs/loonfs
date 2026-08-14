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
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id, page_limit};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn owner_update() -> UpdateAttributesOptions {
    let mut options = UpdateAttributesOptions::new(loonfs_test_support::test_actor());
    options.set = BTreeMap::from([(attribute_key("owner"), attribute_text("platform"))]);
    options.commit.commit_id = Some(CommitId::parse("annotate-report").expect("commit id"));
    options
}

/// The convenience call and the hand-built one-operation commit are the same
/// request, so they cannot fingerprint differently.
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
        options.commit.actor.clone(),
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
        &explicit.actor,
        explicit.message.as_deref(),
        &explicit.operations,
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
            &different.actor,
            different.message.as_deref(),
            &different.operations
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

/// Writing through the convenience and reading back through stat: the map is
/// there, at the revision the write produced.
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

/// The read options decide whether the grouped projection is present.
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

    let opted_out = block_on(fs.reader.stat_path(
        &namespace_id,
        "/docs/report.txt",
        StatPathOptions {
            include_attributes: false,
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
            include_attributes: true,
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

/// The embedded document advertises attributes as a core feature.
#[test]
fn the_embedded_capability_document_advertises_attributes() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "attributes-capability");
    let document = fs.reader.capabilities();
    assert_eq!(
        document.features.get(loonfs::FEATURE_ATTRIBUTES),
        Some(&true)
    );
    document
        .validate()
        .expect("`core.attributes` is parented by the advertised core plane");
}
