use loon_api::{InodeId, NamespaceId};
use loon_core::{
    begin_read_session, bootstrap_namespace, close_read_session, list_read_session_children,
    read_read_session_file, resolve_path, write_file_bytes, CoreError, MutationContext,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::read_session;
use loon_objectstore::ObjectStore;
use tempfile::tempdir;

#[test]
fn read_session_pins_listing_and_file_bytes_at_session_start() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha-v1",
        &context,
        Some("seed-alpha-v1"),
    )
    .expect("seed alpha");

    let session =
        begin_read_session(&store, &namespace_id, "/docs", &context).expect("begin read session");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha-v2",
        &context,
        Some("seed-alpha-v2"),
    )
    .expect("replace alpha");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/beta.txt",
        b"beta-v1",
        &context,
        Some("seed-beta-v1"),
    )
    .expect("seed beta");

    let entries = list_read_session_children(
        &store,
        &namespace_id,
        &session.session_id,
        session.root.inode_id,
    )
    .expect("list pinned children");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].absolute_path, "/docs/alpha.txt");

    let pinned_read = read_read_session_file(
        &store,
        &namespace_id,
        &session.session_id,
        entries[0].inode_id,
    )
    .expect("read pinned alpha");
    assert_eq!(pinned_read.bytes, b"alpha-v1");

    let live_beta = resolve_path(&store, &namespace_id, "/docs/beta.txt").expect("live beta");
    assert_eq!(live_beta.display_name, "beta.txt");
}

#[test]
fn read_session_rejects_inode_ids_outside_the_pinned_subtree() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/inside.txt",
        b"inside",
        &context,
        Some("seed-inside"),
    )
    .expect("seed inside");
    write_file_bytes(
        &store,
        &namespace_id,
        "/other/outside.txt",
        b"outside",
        &context,
        Some("seed-outside"),
    )
    .expect("seed outside");

    let session =
        begin_read_session(&store, &namespace_id, "/docs", &context).expect("begin read session");
    let outside = resolve_path(&store, &namespace_id, "/other/outside.txt").expect("outside stat");

    let error =
        read_read_session_file(&store, &namespace_id, &session.session_id, outside.inode_id)
            .expect_err("outside inode should be rejected");
    assert!(matches!(
        error,
        CoreError::InvalidReadSessionTarget {
            session_id,
            inode_id,
            root_inode_id,
        } if session_id == session.session_id
            && inode_id == outside.inode_id
            && root_inode_id == session.root.inode_id
    ));
}

#[test]
fn close_read_session_removes_the_control_object_and_is_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id();

    bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha-v1",
        &context,
        Some("seed-alpha-v1"),
    )
    .expect("seed alpha");

    let session =
        begin_read_session(&store, &namespace_id, "/docs", &context).expect("begin read session");
    let session_key = read_session(namespace_id.as_str(), &session.session_id);
    assert!(
        store.head(&session_key).expect("session head").is_some(),
        "session object should exist before close"
    );

    close_read_session(&store, &namespace_id, &session.session_id).expect("close session");
    assert!(
        store.head(&session_key).expect("session head").is_none(),
        "session object should be removed after close"
    );

    close_read_session(&store, &namespace_id, &session.session_id)
        .expect("repeated close stays cleanup-safe");

    let error = list_read_session_children(&store, &namespace_id, &session.session_id, InodeId(2))
        .expect_err("closed session should not be readable");
    assert!(matches!(
        error,
        CoreError::ReadSessionNotFound { session_id } if session_id == session.session_id
    ));
}

fn mutation_context() -> MutationContext {
    MutationContext {
        writer_id: "writer-a".to_owned(),
        writer_version: "writer-a/0.1.0".to_owned(),
        now_ms: 1_000,
        lease_duration_ms: 60_000,
    }
}

fn namespace_id() -> NamespaceId {
    NamespaceId::from("demo")
}
