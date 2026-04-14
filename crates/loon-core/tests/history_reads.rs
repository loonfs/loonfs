use loon_api::{NamespaceId, RevisionNo};
use loon_core::{
    bootstrap_namespace, delete_path_non_recursive, list_file_revisions_by_inode,
    list_file_revisions_by_path, move_path, put_file_bytes, read_file_bytes_at_revision,
    read_file_bytes_by_inode_at_revision, resolve_path, MutationContext, PutFileBehavior,
};
use loon_objectstore::fs::LocalFsStore;
use tempfile::tempdir;

#[test]
fn path_history_follows_current_inode_after_rename() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace = NamespaceId::from("demo");

    bootstrap_namespace(&store, &namespace, &context(), false).expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace,
        "/docs/history.txt",
        b"v1\n",
        PutFileBehavior::CreateOnly,
        &context(),
        None,
    )
    .expect("create");
    put_file_bytes(
        &store,
        &namespace,
        "/docs/history.txt",
        b"v2\n",
        PutFileBehavior::ReplaceExisting,
        &context(),
        None,
    )
    .expect("replace");
    move_path(
        &store,
        &namespace,
        "/docs/history.txt",
        "/docs/renamed.txt",
        &context(),
        None,
    )
    .expect("rename");

    let revisions =
        list_file_revisions_by_path(&store, &namespace, "/docs/renamed.txt", None, None)
            .expect("list revisions");
    assert_eq!(
        revisions.current_absolute_path.as_deref(),
        Some("/docs/renamed.txt")
    );
    assert!(revisions.currently_visible);
    assert_eq!(revisions.revisions.len(), 2);
    assert_eq!(revisions.revisions[0].revision_no, RevisionNo(2));
    assert_eq!(revisions.revisions[1].revision_no, RevisionNo(1));

    let first_revision =
        read_file_bytes_at_revision(&store, &namespace, "/docs/renamed.txt", RevisionNo(1))
            .expect("read revision");
    assert_eq!(first_revision.bytes, b"v1\n");
}

#[test]
fn inode_history_survives_delete_and_old_bytes_remain_readable() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace = NamespaceId::from("demo");

    bootstrap_namespace(&store, &namespace, &context(), false).expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace,
        "/docs/history.txt",
        b"v1\n",
        PutFileBehavior::CreateOnly,
        &context(),
        None,
    )
    .expect("create");
    put_file_bytes(
        &store,
        &namespace,
        "/docs/history.txt",
        b"v2\n",
        PutFileBehavior::ReplaceExisting,
        &context(),
        None,
    )
    .expect("replace");
    let inode_id = resolve_path(&store, &namespace, "/docs/history.txt")
        .expect("stat")
        .inode_id;
    delete_path_non_recursive(&store, &namespace, "/docs/history.txt", &context(), None)
        .expect("delete");

    let revisions = list_file_revisions_by_inode(&store, &namespace, inode_id, None, None)
        .expect("list inode revisions");
    assert_eq!(revisions.inode_id, inode_id);
    assert!(!revisions.currently_visible);
    assert!(revisions.current_absolute_path.is_none());
    assert_eq!(revisions.revisions.len(), 2);
    assert_eq!(revisions.revisions[0].size_bytes, 3);

    let first_revision =
        read_file_bytes_by_inode_at_revision(&store, &namespace, inode_id, RevisionNo(1))
            .expect("read inode revision");
    assert_eq!(first_revision.bytes, b"v1\n");
}

fn context() -> MutationContext {
    MutationContext {
        writer_id: "history-tests".to_owned(),
        writer_version: "history-tests/0.1.0".to_owned(),
        now_ms: 1,
        lease_duration_ms: 1_000,
    }
}
