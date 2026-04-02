use loon_client::state_db::{
    ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow, RemoteFileStateRow, SqliteStateDb,
    SyncAnchorRow,
};
use loon_client::upload::upload_small_file_from_path;
use loon_macos::{
    loon_file_provider_bridge_close, loon_file_provider_bridge_list_children,
    loon_file_provider_bridge_list_root, loon_file_provider_bridge_lookup_item,
    loon_file_provider_bridge_materialize_item, loon_file_provider_bridge_open,
    loon_file_provider_string_free, FileProviderBridge, FileProviderSpikeConfig, ProviderItemId,
    ProviderMaterializationState,
};
use loon_objectstore::fs::LocalFsStore;
use loon_ops::{OpsClientConfig, OpsConfig, OpsObjectStoreSpec, OpsSection, OpsServerConfig};
use loon_testkit::tempdir::TestDir;
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use std::collections::BTreeSet;
use std::ffi::{CStr, CString};
use std::fs;
use std::os::raw::c_char;

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

#[test]
fn provider_item_id_opaque_encoding_is_stable() {
    assert_eq!(ProviderItemId::Root.to_opaque_string(), "root");
    assert_eq!(
        ProviderItemId::NamespaceRoot {
            namespace_id: NamespaceId::from("ns-a"),
        }
        .to_opaque_string(),
        "ns:6e732d61"
    );
    assert_eq!(
        ProviderItemId::BoundInode {
            namespace_id: NamespaceId::from("ns-a"),
            inode_id: InodeId(7),
        }
        .to_opaque_string(),
        "inode:6e732d61:0000000000000007"
    );
    let local_only = ProviderItemId::LocalOnly {
        namespace_id: NamespaceId::from("ns-a"),
        client_file_id: ClientFileId::from("tmp:42"),
    };
    let opaque = local_only.to_opaque_string();
    assert_eq!(opaque, "local:6e732d61:746d703a3432");
    assert_eq!(
        ProviderItemId::from_opaque_str(&opaque).expect("decode opaque item id"),
        local_only
    );
}

#[test]
fn file_provider_ffi_round_trips_open_list_lookup_and_close() {
    let temp_dir = TestDir::new("macos-provider-ffi-listing");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    let object_store_root = temp_dir.path().join("objectstore");
    let config_path = temp_dir.path().join("loondb-provider.toml");
    let namespace_id = NamespaceId::from("provider-ns");
    let namespace_root = mirror_root.join(namespace_id.as_str());
    fs::create_dir_all(&namespace_root).expect("create namespace root");
    fs::write(namespace_root.join("bound.txt"), "bound\n").expect("write bound file");
    fs::write(namespace_root.join("draft.txt"), "draft\n").expect("write local-only file");
    seed_projection_state(&db_path, &namespace_id);
    write_ops_config_file(&config_path, &db_path, &mirror_root, &object_store_root);

    let open_response = call_ffi(
        loon_file_provider_bridge_open,
        &serde_json::json!({
            "ops_config_path": config_path.to_string_lossy(),
            "exposed_namespaces": [namespace_id.as_str()],
        })
        .to_string(),
    );
    assert_eq!(open_response["ok"], serde_json::Value::Bool(true));
    let bridge_handle = open_response["result"]["bridge_handle"]
        .as_u64()
        .expect("bridge handle should be present");

    let root_response = call_ffi(
        loon_file_provider_bridge_list_root,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
        })
        .to_string(),
    );
    assert_eq!(root_response["ok"], serde_json::Value::Bool(true));
    assert_eq!(
        root_response["result"]["items"][0]["display_name"],
        serde_json::Value::String(namespace_id.as_str().to_owned())
    );
    assert_eq!(
        root_response["result"]["items"][0]["parent_item_id"],
        serde_json::Value::String("root".to_owned())
    );

    let namespace_root_id = ProviderItemId::NamespaceRoot {
        namespace_id: namespace_id.clone(),
    }
    .to_opaque_string();
    let children_response = call_ffi(
        loon_file_provider_bridge_list_children,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
            "parent_item_id": namespace_root_id,
        })
        .to_string(),
    );
    assert_eq!(children_response["ok"], serde_json::Value::Bool(true));
    let child_names = children_response["result"]["items"]
        .as_array()
        .expect("children array")
        .iter()
        .map(|item| {
            item["display_name"]
                .as_str()
                .expect("display name")
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(child_names, vec!["bound.txt", "draft.txt", "remote.txt"]);
    assert_eq!(
        children_response["result"]["warnings"][0]["relative_path"],
        serde_json::Value::String("alias".to_owned())
    );
    assert_eq!(
        children_response["result"]["warnings"][0]["inode_kind"],
        serde_json::Value::String("symlink".to_owned())
    );

    let lookup_response = call_ffi(
        loon_file_provider_bridge_lookup_item,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
            "item_id": ProviderItemId::BoundInode {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(8),
            }
            .to_opaque_string(),
        })
        .to_string(),
    );
    assert_eq!(lookup_response["ok"], serde_json::Value::Bool(true));
    assert_eq!(
        lookup_response["result"]["item"]["materialization_state"],
        serde_json::Value::String("placeholder".to_owned())
    );
    assert_eq!(
        lookup_response["result"]["warnings"][0]["reason"],
        serde_json::Value::String("unsupported_inode_kind".to_owned())
    );

    let close_response = call_ffi(
        loon_file_provider_bridge_close,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
        })
        .to_string(),
    );
    assert_eq!(close_response["ok"], serde_json::Value::Bool(true));
    assert_eq!(
        close_response["result"]["closed"],
        serde_json::Value::Bool(true)
    );
}

#[test]
fn file_provider_ffi_materializes_placeholder_file() {
    let temp_dir = TestDir::new("macos-provider-ffi-materialize");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    let object_store_root = temp_dir.path().join("objectstore");
    let config_path = temp_dir.path().join("loondb-provider.toml");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let namespace_id = NamespaceId::from("provider-ns");
    let source_path = source_root.join("remote.txt");
    fs::write(&source_path, "finder hydration bytes\n").expect("write source file");
    let store = LocalFsStore::new(&object_store_root).expect("open local fs store");
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
    write_ops_config_file(&config_path, &db_path, &mirror_root, &object_store_root);

    let open_response = call_ffi(
        loon_file_provider_bridge_open,
        &serde_json::json!({
            "ops_config_path": config_path.to_string_lossy(),
            "exposed_namespaces": [namespace_id.as_str()],
        })
        .to_string(),
    );
    let bridge_handle = open_response["result"]["bridge_handle"]
        .as_u64()
        .expect("bridge handle should be present");

    let materialize_response = call_ffi(
        loon_file_provider_bridge_materialize_item,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
            "item_id": ProviderItemId::BoundInode {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(7),
            }
            .to_opaque_string(),
            "now_ms": 1_700_000_000_000u64,
        })
        .to_string(),
    );
    assert_eq!(materialize_response["ok"], serde_json::Value::Bool(true));
    assert_eq!(
        materialize_response["result"]["relative_path"],
        serde_json::Value::String("remote.txt".to_owned())
    );
    let absolute_path = materialize_response["result"]["absolute_path"]
        .as_str()
        .expect("absolute path");
    assert_eq!(
        fs::read_to_string(absolute_path).expect("read hydrated file"),
        "finder hydration bytes\n"
    );

    let close_response = call_ffi(
        loon_file_provider_bridge_close,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
        })
        .to_string(),
    );
    assert_eq!(close_response["ok"], serde_json::Value::Bool(true));
}

#[test]
fn file_provider_ffi_reports_invalid_json_and_unknown_item_id() {
    let temp_dir = TestDir::new("macos-provider-ffi-errors");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    let object_store_root = temp_dir.path().join("objectstore");
    let config_path = temp_dir.path().join("loondb-provider.toml");
    let namespace_id = NamespaceId::from("provider-ns");
    seed_projection_state(&db_path, &namespace_id);
    write_ops_config_file(&config_path, &db_path, &mirror_root, &object_store_root);

    let invalid_json_response = call_ffi(loon_file_provider_bridge_open, "{");
    assert_eq!(invalid_json_response["ok"], serde_json::Value::Bool(false));
    assert_eq!(
        invalid_json_response["error"]["code"],
        serde_json::Value::String("invalid_json_request".to_owned())
    );

    let open_response = call_ffi(
        loon_file_provider_bridge_open,
        &serde_json::json!({
            "ops_config_path": config_path.to_string_lossy(),
            "exposed_namespaces": [namespace_id.as_str()],
        })
        .to_string(),
    );
    let bridge_handle = open_response["result"]["bridge_handle"]
        .as_u64()
        .expect("bridge handle should be present");

    let unknown_item_response = call_ffi(
        loon_file_provider_bridge_lookup_item,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
            "item_id": "bogus-item-id",
        })
        .to_string(),
    );
    assert_eq!(unknown_item_response["ok"], serde_json::Value::Bool(false));
    assert_eq!(
        unknown_item_response["error"]["code"],
        serde_json::Value::String("invalid_item_id".to_owned())
    );

    let close_response = call_ffi(
        loon_file_provider_bridge_close,
        &serde_json::json!({
            "bridge_handle": bridge_handle,
        })
        .to_string(),
    );
    assert_eq!(close_response["ok"], serde_json::Value::Bool(true));
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

fn write_ops_config_file(
    config_path: &std::path::Path,
    state_db_path: &std::path::Path,
    mirror_root: &std::path::Path,
    object_store_root: &std::path::Path,
) {
    fs::write(
        config_path,
        format!(
            "\
[object_store]
kind = \"local-fs\"
root = \"{}\"

[client]
state_db_path = \"{}\"
mirror_root = \"{}\"

[server]
writer_id = \"writer-a\"
writer_version = \"test\"
lease_duration_ms = 60000
",
            object_store_root.display(),
            state_db_path.display(),
            mirror_root.display(),
        ),
    )
    .expect("write ops config");
}

fn call_ffi(
    function: extern "C" fn(*const c_char) -> *mut c_char,
    request_json: &str,
) -> serde_json::Value {
    let request_json = CString::new(request_json).expect("request JSON should not contain NUL");
    let response_ptr = function(request_json.as_ptr());
    ffi_response_json(response_ptr)
}

fn ffi_response_json(response_ptr: *mut c_char) -> serde_json::Value {
    assert!(
        !response_ptr.is_null(),
        "ffi response pointer should not be null"
    );
    // SAFETY: the response pointer comes from `loon_file_provider_*` and remains valid until it is
    // released through `loon_file_provider_string_free`.
    let response_json = unsafe { CStr::from_ptr(response_ptr) }
        .to_str()
        .expect("response should be valid UTF-8")
        .to_owned();
    // SAFETY: `response_ptr` was returned by a `loon_file_provider_*` FFI call and has not been
    // freed yet.
    unsafe { loon_file_provider_string_free(response_ptr) };
    serde_json::from_str(&response_json).expect("response should be valid JSON")
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
