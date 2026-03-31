use loon_client::state_db::{
    ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow, RemoteFileStateRow, SqliteStateDb,
    SyncAnchorRow,
};
use loon_client::upload::upload_small_file_from_path;
use loon_macos::{
    FileProviderBridge, FileProviderSpikeConfig, ProviderItemId, ProviderMaterializationState,
};
use loon_objectstore::fs::LocalFsStore;
use loon_ops::{OpsClientConfig, OpsConfig, OpsObjectStoreSpec, OpsSection, OpsServerConfig};
use loon_testkit::tempdir::TestDir;
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use std::collections::BTreeSet;
use std::fs;

#[test]
fn file_provider_bridge_lists_root_namespaces_deterministically() {
    let temp_dir = TestDir::new("macos-provider-root");
    let config = spike_config(
        temp_dir.path().join("client.sqlite3"),
        temp_dir.path().join("mirror"),
        temp_dir.path().join("objectstore"),
    );
    let bridge = FileProviderBridge::new(FileProviderSpikeConfig {
        ops_config: config,
        exposed_namespaces: BTreeSet::from([
            NamespaceId::from("gamma-namespace"),
            NamespaceId::from("alpha-namespace"),
            NamespaceId::from("beta-namespace"),
        ]),
    })
    .expect("create bridge");

    let root = bridge.list_root().expect("list provider root");
    let names = root
        .items
        .iter()
        .map(|item| item.display_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec!["alpha-namespace", "beta-namespace", "gamma-namespace"]
    );
}

#[test]
fn file_provider_bridge_lists_namespace_children_and_warns_for_unsupported_items() {
    let temp_dir = TestDir::new("macos-provider-listing");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    let object_store_root = temp_dir.path().join("objectstore");
    let namespace_id = NamespaceId::from("provider-ns");
    let namespace_root = mirror_root.join(namespace_id.as_str());
    fs::create_dir_all(&namespace_root).expect("create namespace root");
    fs::write(namespace_root.join("bound.txt"), "bound\n").expect("write bound file");
    fs::write(namespace_root.join("draft.txt"), "draft\n").expect("write local-only file");

    seed_projection_state(&db_path, &namespace_id);

    let bridge = FileProviderBridge::new(FileProviderSpikeConfig {
        ops_config: spike_config(db_path, mirror_root, object_store_root),
        exposed_namespaces: BTreeSet::from([namespace_id.clone()]),
    })
    .expect("create bridge");

    let listing = bridge
        .list_children(&ProviderItemId::NamespaceRoot {
            namespace_id: namespace_id.clone(),
        })
        .expect("list namespace children");

    let names = listing
        .items
        .iter()
        .map(|item| item.display_name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["bound.txt", "draft.txt", "remote.txt"]);

    let states = listing
        .items
        .iter()
        .map(|item| (item.display_name.clone(), item.materialization_state))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        states.get("bound.txt"),
        Some(&ProviderMaterializationState::Materialized)
    );
    assert_eq!(
        states.get("draft.txt"),
        Some(&ProviderMaterializationState::Materialized)
    );
    assert_eq!(
        states.get("remote.txt"),
        Some(&ProviderMaterializationState::Placeholder)
    );

    assert_eq!(listing.warnings.len(), 1);
    assert_eq!(listing.warnings[0].relative_path, "alias");
    assert_eq!(listing.warnings[0].reason, "unsupported_inode_kind");
}

#[test]
fn file_provider_bridge_materializes_placeholder_file() {
    let temp_dir = TestDir::new("macos-provider-materialize");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    let object_store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let namespace_id = NamespaceId::from("provider-ns");
    let store = LocalFsStore::new(&object_store_root).expect("open local fs store");
    let source_path = source_root.join("remote.txt");
    fs::write(&source_path, "finder hydration bytes\n").expect("write source file");
    let uploaded = upload_small_file_from_path(&store, &namespace_id, &source_path)
        .expect("upload source content");
    seed_remote_only_placeholder(
        &db_path,
        &namespace_id,
        InodeId(7),
        "remote.txt",
        &uploaded.file_digest_sha256,
        &uploaded.content_manifest_digest,
    );

    let bridge = FileProviderBridge::new(FileProviderSpikeConfig {
        ops_config: spike_config(db_path, mirror_root.clone(), object_store_root),
        exposed_namespaces: BTreeSet::from([namespace_id.clone()]),
    })
    .expect("create bridge");

    let snapshot = bridge
        .lookup_item(&ProviderItemId::BoundInode {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(7),
        })
        .expect("lookup item")
        .expect("placeholder item should exist");
    assert_eq!(
        snapshot.materialization_state,
        ProviderMaterializationState::Placeholder
    );

    let materialized = bridge
        .materialize_item(
            &ProviderItemId::BoundInode {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(7),
            },
            1_700_000_000_000,
        )
        .expect("materialize provider item");

    assert_eq!(materialized.relative_path, "remote.txt");
    assert_eq!(
        fs::read_to_string(&materialized.absolute_path).expect("read hydrated file"),
        "finder hydration bytes\n"
    );
    assert_eq!(
        materialized.absolute_path,
        mirror_root.join(namespace_id.as_str()).join("remote.txt")
    );
}

fn spike_config(
    state_db_path: std::path::PathBuf,
    mirror_root: std::path::PathBuf,
    object_store_root: std::path::PathBuf,
) -> OpsConfig {
    OpsConfig {
        object_store: OpsObjectStoreSpec::LocalFs {
            root: object_store_root,
            key_prefix: None,
        },
        client: OpsClientConfig {
            state_db_path,
            mirror_root,
        },
        server: OpsServerConfig {
            writer_id: "writer-a".to_owned(),
            writer_version: "test".to_owned(),
            lease_duration_ms: 60_000,
        },
        ops: OpsSection::default(),
    }
}

fn seed_projection_state(db_path: &std::path::Path, namespace_id: &NamespaceId) {
    let mut db = SqliteStateDb::open(db_path).expect("open client db");
    db.planner_transaction("seed-provider-projection", |tx| {
        tx.upsert_remote_file(&remote_root(namespace_id))?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(1),
            InodeKind::Dir,
            None,
            "",
            false,
        ))?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(7),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(4),
            revision_no: RevisionNo(2),
            content_digest: Some("sha256:bound".to_owned()),
            content_manifest_digest: Some("sha256:manifest-bound".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "bound.txt".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(7),
            inode_kind: InodeKind::File,
            content_digest: Some("sha256:bound".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "bound.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_000_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(7),
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(4),
            revision_no: RevisionNo(2),
            content_digest: Some("sha256:bound".to_owned()),
            content_manifest_digest: Some("sha256:manifest-bound".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "bound.txt".to_owned(),
        })?;
        tx.upsert_local_only_file(&LocalOnlyFileStateRow {
            client_file_id: ClientFileId::from("tmp:provider-ns:00000000000000000001"),
            namespace_id: namespace_id.clone(),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(1)),
            display_name: "draft.txt".to_owned(),
            content_digest: Some("sha256:draft".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_001_000,
        })?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(8),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(5),
            revision_no: RevisionNo(1),
            content_digest: Some("sha256:remote".to_owned()),
            content_manifest_digest: Some("sha256:manifest-remote".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "remote.txt".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(8),
            InodeKind::File,
            Some(InodeId(1)),
            "remote.txt",
            false,
        ))?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(9),
            inode_kind: InodeKind::Symlink,
            observed_seq: ChangeSeq(6),
            revision_no: RevisionNo(0),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(1)),
            display_name: "alias".to_owned(),
            is_deleted: false,
        })?;
        Ok(())
    })
    .expect("seed projection state");
}

fn seed_remote_only_placeholder(
    db_path: &std::path::Path,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    display_name: &str,
    content_digest: &str,
    content_manifest_digest: &str,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client db");
    db.planner_transaction("seed-provider-placeholder", |tx| {
        tx.upsert_remote_file(&remote_root(namespace_id))?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            InodeId(1),
            InodeKind::Dir,
            None,
            "",
            false,
        ))?;
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(2),
            revision_no: RevisionNo(1),
            content_digest: Some(content_digest.to_owned()),
            content_manifest_digest: Some(content_manifest_digest.to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: display_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&local_placeholder(
            namespace_id,
            inode_id,
            InodeKind::File,
            Some(InodeId(1)),
            display_name,
            false,
        ))?;
        Ok(())
    })
    .expect("seed placeholder state");
}

fn remote_root(namespace_id: &NamespaceId) -> RemoteFileStateRow {
    RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: InodeId(1),
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(0),
        revision_no: RevisionNo(0),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: None,
        display_name: String::new(),
        is_deleted: false,
    }
}

fn local_placeholder(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    inode_kind: InodeKind,
    parent_inode_id: Option<InodeId>,
    display_name: &str,
    exists_on_disk: bool,
) -> LocalFileStateRow {
    LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id,
        inode_kind,
        content_digest: None,
        parent_inode_id,
        display_name: display_name.to_owned(),
        exists_on_disk,
        dirty: false,
        last_local_change_ms: 0,
    }
}
