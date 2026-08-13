//! Embedded integration tests for actor and timestamp fields.

#![allow(clippy::panic)]

use crate::common::*;
use loonfs::{
    ActorId, ActorRef, CopyOptions, CreateNamespaceOptions, DeleteDirectoryBehavior, DeleteOptions,
    DestinationBehavior, MaintenancePlan, MoveOptions, PageRequest, PutFileOptions,
    RestoreRevisionOptions, RevisionNo, UpdateAttributesOptions,
};
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id, page_limit};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn actor(id: &str) -> ActorRef {
    ActorRef::user(ActorId::parse(id).expect("actor id"))
}

#[test]
fn embedded_reads_project_commit_attribution_without_rewriting_inode_creation() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "attribution-rows");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    let creator = actor("creator");
    let create = fs
        .put_file_bytes_blocking(
            &namespace_id,
            "/implicit/parent/report.txt",
            b"v1",
            PutFileOptions::new(creator.clone()),
        )
        .expect("create file and parents");
    let created = fs
        .stat_path_blocking(&namespace_id, "/implicit/parent/report.txt")
        .expect("stat created file");
    assert_eq!(created.created_by, creator);
    assert_eq!(created.kind.revision_actor(), Some(&creator));
    for path in ["/implicit", "/implicit/parent"] {
        let parent = fs
            .stat_path_blocking(&namespace_id, path)
            .expect("stat implicit parent");
        assert_eq!(parent.created_by, creator);
        assert_eq!(parent.created_at_ms, created.created_at_ms);
    }
    assert_eq!(created.head_seq, create.committed_seq);

    let replacer = actor("replacer");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/implicit/parent/report.txt",
        b"v2",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::new(replacer.clone())
        },
    )
    .expect("replace file");
    let replaced = fs
        .stat_path_blocking(&namespace_id, "/implicit/parent/report.txt")
        .expect("stat replacement");
    assert_eq!(replaced.created_by, creator);
    assert_eq!(replaced.created_at_ms, created.created_at_ms);
    assert_eq!(replaced.kind.revision_actor(), Some(&replacer));

    let restorer = actor("restorer");
    block_on(fs.writer.restore_file_revision(
        &namespace_id,
        "/implicit/parent/report.txt",
        RevisionNo(1),
        RestoreRevisionOptions::new(restorer.clone()),
    ))
    .expect("restore first revision");
    let revisions = block_on(fs.reader.list_file_revisions_page(
        &namespace_id,
        "/implicit/parent/report.txt",
        PageRequest {
            limit: page_limit(10),
            cursor: None,
        },
    ))
    .expect("list revisions");
    assert_eq!(revisions.revisions[0].actor, restorer);
    assert_eq!(revisions.revisions[2].actor, creator);

    let source_attribute_editor = actor("source-attribute-editor");
    block_on(fs.writer.update_attributes(
        &namespace_id,
        "/implicit/parent/report.txt",
        UpdateAttributesOptions {
            set: BTreeMap::from([(attribute_key("owner"), attribute_text("source"))]),
            ..UpdateAttributesOptions::new(source_attribute_editor.clone())
        },
    ))
    .expect("annotate source before copy");

    let copier = actor("copier");
    fs.copy_path_blocking(
        &namespace_id,
        "/implicit/parent/report.txt",
        "/copy.txt",
        CopyOptions::new(copier.clone()),
    )
    .expect("copy file");
    let copied = fs
        .stat_path_blocking(&namespace_id, "/copy.txt")
        .expect("stat copy");
    assert_eq!(copied.created_by, copier);
    assert_eq!(copied.kind.revision_actor(), Some(&copier));
    assert_eq!(
        copied
            .attributes
            .as_ref()
            .expect("copied attributes")
            .updated_by,
        Some(copier.clone()),
        "copy restates inherited attributes under the copy commit"
    );
    assert_eq!(
        fs.stat_path_blocking(&namespace_id, "/implicit/parent/report.txt")
            .expect("stat source after copy")
            .attributes
            .expect("source attributes")
            .updated_by,
        Some(source_attribute_editor),
        "the source keeps its own attribute attribution"
    );

    let before_move = fs
        .stat_path_blocking(&namespace_id, "/copy.txt")
        .expect("stat before move");
    fs.move_path_blocking(
        &namespace_id,
        "/copy.txt",
        "/moved.txt",
        MoveOptions::new(actor("mover")),
    )
    .expect("move copy");
    let after_move = fs
        .stat_path_blocking(&namespace_id, "/moved.txt")
        .expect("stat after move");
    assert_eq!(after_move.created_by, before_move.created_by);
    assert_eq!(after_move.created_at_ms, before_move.created_at_ms);
    assert_eq!(after_move.kind, before_move.kind);
}

#[test]
fn attributes_root_forks_and_trash_report_their_row_attribution() {
    let temp_dir = tempdir().expect("tempdir");
    let object_store = store(temp_dir.path());
    let fs = open_runtime(object_store.clone(), "attribution-projections");
    let source_id = namespace_id("source");
    fs.create_namespace_blocking(&source_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let root = fs.stat_path_blocking(&source_id, "/").expect("stat root");
    assert_eq!(root.created_by, ActorRef::loonfs_system());
    let root_attributes = root.attributes.expect("root attributes projection");
    assert_eq!(root_attributes.updated_by, None);
    assert_eq!(root_attributes.updated_at_ms, None);

    let creator = actor("file-owner");
    fs.put_file_bytes_blocking(
        &source_id,
        "/report.txt",
        b"report",
        PutFileOptions::new(creator.clone()),
    )
    .expect("create report");
    let updater = actor("metadata-editor");
    block_on(fs.writer.update_attributes(
        &source_id,
        "/report.txt",
        UpdateAttributesOptions {
            set: BTreeMap::from([(attribute_key("owner"), attribute_text("platform"))]),
            ..UpdateAttributesOptions::new(updater.clone())
        },
    ))
    .expect("update attributes");
    let projected = fs
        .stat_path_blocking(&source_id, "/report.txt")
        .expect("stat attributed attributes")
        .attributes
        .expect("attributes projection");
    assert_eq!(projected.updated_by, Some(updater));
    assert!(projected.updated_at_ms.is_some());

    let later_updater = actor("metadata-reviewer");
    block_on(fs.writer.update_attributes(
        &source_id,
        "/report.txt",
        UpdateAttributesOptions {
            set: BTreeMap::from([(attribute_key("stage"), attribute_text("reviewed"))]),
            ..UpdateAttributesOptions::new(later_updater.clone())
        },
    ))
    .expect("update attributes again");
    let later_projection = fs
        .stat_path_blocking(&source_id, "/report.txt")
        .expect("stat later attributes")
        .attributes
        .expect("later attributes projection");
    assert_eq!(later_projection.revision_no.0, 2);
    assert_eq!(later_projection.updated_by, Some(later_updater));
    assert!(later_projection.updated_at_ms.is_some());
    let source_before_fork = fs
        .stat_path_blocking(&source_id, "/report.txt")
        .expect("stat source before fork");

    fs.flush_wal_blocking(&source_id).expect("flush source");
    let fork_id = namespace_id("forked");
    fs.fork_namespace_blocking(&source_id, &fork_id)
        .expect("fork namespace");
    let forked = fs
        .stat_path_blocking(&fork_id, "/report.txt")
        .expect("stat inherited row");
    assert_eq!(forked.created_by, creator);
    assert_eq!(forked.created_at_ms, source_before_fork.created_at_ms);
    assert_eq!(forked.kind, source_before_fork.kind);
    assert_eq!(forked.attributes, source_before_fork.attributes);

    let trash_creator = actor("trash-creator");
    fs.put_file_bytes_blocking(
        &source_id,
        "/trash/subtree/file.txt",
        b"trash",
        PutFileOptions::new(trash_creator),
    )
    .expect("create subtree to delete");
    let deleter = actor("deleter");
    let deletion = fs
        .delete_path_blocking(
            &source_id,
            "/trash",
            DeleteOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..DeleteOptions::new(deleter.clone())
            },
        )
        .expect("delete subtree");
    let trash = block_on(fs.reader.list_trash_page(
        &source_id,
        PageRequest {
            limit: page_limit(10),
            cursor: None,
        },
    ))
    .expect("list trash before checkpoint");
    assert_eq!(trash.entries.len(), 1);
    assert_eq!(trash.entries[0].deleted_by, deleter);
    assert_eq!(trash.entries[0].deleted_at_seq, deletion.committed_seq);

    fs.create_checkpoint_blocking(&source_id)
        .expect("checkpoint deletion rows");
    block_on(fs.admin.maintenance_step_namespace(
        &source_id,
        MaintenancePlan {
            advance_retention: true,
            ..MaintenancePlan::default()
        },
    ))
    .expect("advance retention");
    drop(fs);

    let reopened = open_runtime(object_store, "attribution-projections-reopened");
    let trash = block_on(reopened.reader.list_trash_page(
        &source_id,
        PageRequest {
            limit: page_limit(10),
            cursor: None,
        },
    ))
    .expect("list trash after checkpoint, retention, and restart");
    assert_eq!(trash.entries.len(), 1);
    assert_eq!(trash.entries[0].deleted_by, deleter);
    assert_eq!(trash.entries[0].deleted_at_seq, deletion.committed_seq);
}
