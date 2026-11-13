//! The pool that every manifest records.

use super::*;

#[tokio::test]
async fn a_create_generates_a_pool_and_every_fork_records_its_source_pool() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = test_context();
    let [root, other, fork, grandchild] = ["root", "other", "fork", "grandchild"]
        .map(|name| NamespaceId::parse(name).expect("namespace"));
    let mut created = Vec::new();
    for namespace_id in [&root, &other] {
        let metadata = crate::test_support::NamespaceFixture::new(&store, namespace_id, &setup)
            .create()
            .await
            .expect("create the namespace");
        created.push(metadata.pool_id);
    }
    assert_ne!(created[0], created[1], "each create generates its own pool");
    let pool_id = created[0].clone();
    for (source, target) in [(&root, &fork), (&fork, &grandchild)] {
        let fixture = crate::test_support::NamespaceFixture::new(&store, source, &setup);
        let forked = fixture
            .engine()
            .fork_namespace(
                target,
                &loonfs_test_support::test_actor(),
                crate::test_support::folded_manifest(&fixture.namespace)
                    .await
                    .expect("fold the WAL tail"),
            )
            .await
            .expect("fork");
        assert_eq!(forked.pool_id, pool_id, "the fork `{target}`");
    }
    // A successor of the first manifest keeps the value.
    write_test_file(&store, &grandchild, "/file", "write", &setup).await;
    crate::test_support::namespace_store(&store, &grandchild)
        .fold_wal()
        .await
        .expect("fold");
    for namespace_id in [&root, &fork, &grandchild] {
        let namespace = crate::test_support::namespace_store(&store, namespace_id);
        let current = namespace
            .load_current_manifest()
            .await
            .expect("current manifest");
        assert_eq!(current.payload().pool_id, pool_id, "`{namespace_id}`");
        assert_eq!(
            namespace.load_metadata().await.expect("status").pool_id,
            pool_id
        );
    }
}

/// Checks that a process that has a manifest refuses a successor that
/// changes the pool when it probes above that manifest.
#[tokio::test]
async fn a_successor_that_changes_the_pool_is_refused() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = test_context();
    let root = NamespaceId::parse("root").expect("namespace");
    crate::test_support::NamespaceFixture::new(&store, &root, &setup)
        .create()
        .await
        .expect("create the root");
    let first = crate::test_support::namespace_store(&store, &root)
        .load_current_manifest()
        .await
        .expect("the first manifest");
    write_namespace_manifest(
        &store,
        NamespaceManifestPayload {
            pool_id: loonfs_types::PoolId::generate(),
            ..first
                .payload()
                .successor(first.payload().runs.clone())
                .expect("successor")
        },
    )
    .await
    .expect("write the successor");
    let error = crate::test_support::namespace_store(&store, &root)
        .find_newest_manifest_above(&first)
        .await
        .expect_err("the successor changes the pool");
    assert!(error.to_string().contains("pool_id"), "{error}");
}
