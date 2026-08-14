//! HTTP integration tests for actor and timestamp fields.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_api::{ActorId, ActorRef, ChangeSeq, DestinationBehavior, RevisionNo};
use loonfs_client::{
    CopyOptions, DeleteOptions, MoveOptions, NamespacePath, PutFileOptions, RestoreRevisionOptions,
    UndeleteOptions, UpdateAttributesOptions,
};
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn actor(id: &str) -> ActorRef {
    ActorRef::user(ActorId::parse(id).expect("actor id"))
}

fn path(absolute_path: &str) -> NamespacePath {
    NamespacePath::parse("demo", absolute_path).expect("namespace path")
}

/// Returns the change at `seq` so tests can compare its timestamp with the
/// corresponding metadata rows.
async fn change_at(
    harness: &crate::common::TestServer,
    seq: ChangeSeq,
) -> loonfs_api::v0::CommittedChange {
    harness
        .client
        .list_changes(&namespace_id("demo"), ChangeSeq(0), None)
        .await
        .expect("list changes")
        .changes
        .into_iter()
        .find(|change| change.committed_seq == seq)
        .expect("change at committed sequence")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_rows_project_the_commit_that_created_each_retained_fact() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-attribution",
        "http-attribution",
    ))
    .await;
    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let root = harness
        .client
        .stat_path(&path("/"), &Default::default())
        .await
        .expect("stat root");
    assert_eq!(root.created_by, ActorRef::loonfs_system());
    assert!(root.created_at_ms > 0);
    let root_attributes = root.attributes.expect("root attributes");
    assert_eq!(root_attributes.attributes_updated_by, None);
    assert_eq!(root_attributes.attributes_updated_at_ms, None);

    let creator = actor("creator");
    let create = harness
        .client
        .put_file_bytes(
            &path("/implicit/parent/report.txt"),
            b"v1",
            &PutFileOptions::new(creator.clone()),
        )
        .await
        .expect("create file and implicit parents");
    let create_change = change_at(&harness, create.committed_seq).await;
    let created = harness
        .client
        .stat_path(&path("/implicit/parent/report.txt"), &Default::default())
        .await
        .expect("stat created file");
    assert_eq!(created.created_by, creator);
    assert_eq!(created.kind.revision_actor(), Some(&creator));
    assert_eq!(created.created_at_ms, create_change.committed_at_ms);
    for parent_path in ["/implicit", "/implicit/parent"] {
        let parent = harness
            .client
            .stat_path(&path(parent_path), &Default::default())
            .await
            .expect("stat implicit parent");
        assert_eq!(parent.created_by, creator);
        assert_eq!(parent.created_at_ms, create_change.committed_at_ms);
    }

    let replacer = actor("replacer");
    harness
        .client
        .put_file_bytes(
            &path("/implicit/parent/report.txt"),
            b"v2",
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(replacer.clone())
            },
        )
        .await
        .expect("replace file");
    let replaced = harness
        .client
        .stat_path(&path("/implicit/parent/report.txt"), &Default::default())
        .await
        .expect("stat replaced file");
    assert_eq!(replaced.created_by, creator);
    assert_eq!(replaced.created_at_ms, created.created_at_ms);
    assert_eq!(replaced.kind.revision_actor(), Some(&replacer));

    let restorer = actor("restorer");
    harness
        .client
        .restore_file_revision(
            &path("/implicit/parent/report.txt"),
            RevisionNo(1),
            &RestoreRevisionOptions::new(restorer.clone()),
        )
        .await
        .expect("restore first revision");
    let revisions = harness
        .client
        .list_file_revisions_page(&path("/implicit/parent/report.txt"), Some(10), None)
        .await
        .expect("list revisions");
    assert_eq!(
        revisions
            .revisions
            .iter()
            .find(|revision| revision.revision_no == RevisionNo(1))
            .expect("historical revision")
            .actor,
        creator
    );
    assert_eq!(revisions.revisions[0].actor, restorer);

    let copier = actor("copier");
    let copy = harness
        .client
        .copy_path(
            &path("/implicit/parent/report.txt"),
            &path("/copy.txt"),
            &CopyOptions::new(copier.clone()),
        )
        .await
        .expect("copy file");
    let copy_change = change_at(&harness, copy.committed_seq).await;
    let copied = harness
        .client
        .stat_path(&path("/copy.txt"), &Default::default())
        .await
        .expect("stat copy");
    assert_eq!(copied.created_by, copier);
    assert_eq!(copied.kind.revision_actor(), Some(&copier));
    assert_eq!(copied.created_at_ms, copy_change.committed_at_ms);

    let before_move = copied.clone();
    harness
        .client
        .move_path(
            &path("/copy.txt"),
            &path("/moved.txt"),
            &MoveOptions::new(actor("mover")),
        )
        .await
        .expect("move file");
    let after_move = harness
        .client
        .stat_path(&path("/moved.txt"), &Default::default())
        .await
        .expect("stat moved file");
    assert_eq!(after_move.created_by, before_move.created_by);
    assert_eq!(after_move.created_at_ms, before_move.created_at_ms);
    assert_eq!(after_move.kind, before_move.kind);

    let bare_attributes = after_move.attributes.expect("synthetic attributes");
    assert_eq!(bare_attributes.attributes_updated_by, None);
    assert_eq!(bare_attributes.attributes_updated_at_ms, None);
    let updater = actor("updater");
    let update = harness
        .client
        .update_attributes(
            &path("/moved.txt"),
            &UpdateAttributesOptions {
                set: BTreeMap::from([(attribute_key("owner"), attribute_text("platform"))]),
                ..UpdateAttributesOptions::new(updater.clone())
            },
        )
        .await
        .expect("update attributes");
    let update_change = change_at(&harness, update.committed_seq).await;
    let updated = harness
        .client
        .stat_path(&path("/moved.txt"), &Default::default())
        .await
        .expect("stat attributes")
        .attributes
        .expect("attributes projection");
    assert_eq!(updated.attributes_updated_by, Some(updater));
    assert_eq!(
        updated.attributes_updated_at_ms,
        Some(update_change.committed_at_ms)
    );

    let deleter = actor("deleter");
    let delete = harness
        .client
        .delete_path(&path("/moved.txt"), &DeleteOptions::new(deleter.clone()))
        .await
        .expect("delete file");
    let delete_change = change_at(&harness, delete.committed_seq).await;
    let trash = harness
        .client
        .list_trash_page(&namespace, Some(10), None)
        .await
        .expect("list trash");
    assert_eq!(trash.entries.len(), 1);
    let deleted = &trash.entries[0];
    assert_eq!(deleted.deleted_at_seq, delete.committed_seq);
    assert_eq!(deleted.deleted_at_ms, delete_change.committed_at_ms);
    assert_eq!(deleted.deleted_by, deleter);

    let undelete_actor = actor("undelete-restorer");
    let undelete = harness
        .client
        .undelete(
            &namespace,
            deleted.root_inode_id,
            deleted.deleted_at_seq,
            None,
            &UndeleteOptions::new(undelete_actor.clone()),
        )
        .await
        .expect("undelete file");
    assert_eq!(
        change_at(&harness, undelete.committed_seq).await.actor,
        undelete_actor
    );
    assert!(harness
        .client
        .list_trash_page(&namespace, Some(10), None)
        .await
        .expect("list trash after undelete")
        .entries
        .is_empty());

    harness.server.abort();
}
