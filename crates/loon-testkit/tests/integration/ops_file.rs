use loon_client::ops::file::{
    parse_authoritative_path_selector, run_file_command, FileCommand, FileCommandOutput,
};
use loon_client::ops::{
    OpsClientConfig, OpsConfig, OpsObjectStoreSpec, OpsSection, OpsServerConfig,
};
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_server::objectstore::keys::{blob, content_manifest};
use loon_server::objectstore::ConfiguredObjectStore;
use loon_server::ops::{
    bootstrap_namespace, list_authoritative_path, read_authoritative_file_bytes,
    resolve_authoritative_path,
};
use loon_testkit::tempdir::TestDir;
use loon_types::server::{
    AuthoritativeFileBytes, AuthoritativePathEntry, BootstrappedNamespace,
    NamespaceBootstrapParams, NamespaceStateSummary, ServerTransport,
};
use loon_types::{
    sha256_digest, ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse,
    ContentBlockDescriptor, ContentManifestEnvelope, ContentManifestPayload, NamespaceId,
    ObjectStore, ObservedRemoteInode,
};
use std::fs;
use std::path::{Path, PathBuf};

/// Minimal `ServerTransport` backed by a `ConfiguredObjectStore`, used only in tests.
struct TestTransport {
    store: ConfiguredObjectStore,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct TestTransportError(String);

impl ServerTransport for TestTransport {
    type Error = TestTransportError;

    fn execute_mutation(
        &self,
        _request: &ClientMutationRequest,
    ) -> Result<ClientMutationResponse, Self::Error> {
        Err(TestTransportError(
            "execute_mutation not used in file tests".into(),
        ))
    }

    fn load_namespace_state_summary(
        &self,
        _namespace_id: &NamespaceId,
    ) -> Result<NamespaceStateSummary, Self::Error> {
        Err(TestTransportError(
            "load_namespace_state_summary not used in file tests".into(),
        ))
    }

    fn load_remote_observations(
        &self,
        _namespace_id: &NamespaceId,
    ) -> Result<(ChangeSeq, Vec<ObservedRemoteInode>), Self::Error> {
        Err(TestTransportError(
            "load_remote_observations not used in file tests".into(),
        ))
    }

    fn bootstrap_namespace(
        &self,
        _namespace_id: &NamespaceId,
        _params: &NamespaceBootstrapParams,
    ) -> Result<BootstrappedNamespace, Self::Error> {
        Err(TestTransportError(
            "bootstrap_namespace not used in file tests".into(),
        ))
    }

    fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>, Self::Error> {
        list_authoritative_path(&self.store, namespace_id, absolute_path)
            .map_err(|e| TestTransportError(e.to_string()))
    }

    fn resolve_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry, Self::Error> {
        resolve_authoritative_path(&self.store, namespace_id, absolute_path)
            .map_err(|e| TestTransportError(e.to_string()))
    }

    fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes, Self::Error> {
        read_authoritative_file_bytes(&self.store, namespace_id, absolute_path)
            .map_err(|e| TestTransportError(e.to_string()))
    }
}

fn test_make_transport(config: &OpsConfig) -> anyhow::Result<TestTransport> {
    let store = test_open_store(config)?;
    Ok(TestTransport { store })
}

#[test]
fn parse_selector_normalizes_root_and_nested_paths() {
    let root = parse_authoritative_path_selector("demo:/").expect("parse root");
    assert_eq!(root.namespace_id.as_str(), "demo");
    assert_eq!(root.absolute_path, "/");

    let nested = parse_authoritative_path_selector("demo:/docs//report.txt").expect("parse nested");
    assert_eq!(nested.absolute_path, "/docs/report.txt");
}

#[test]
fn parse_selector_rejects_invalid_syntax() {
    assert!(parse_authoritative_path_selector("demo").is_err());
    assert!(parse_authoritative_path_selector("demo:docs").is_err());
    assert!(parse_authoritative_path_selector("demo:/docs/../secret").is_err());
}

#[test]
fn ls_stat_get_and_cat_use_authoritative_store_directly() {
    let temp_dir = TestDir::new("loon-ops-file-read");
    let config_path = write_local_fs_config(temp_dir.path());
    let namespace_id = NamespaceId::from("demo");
    seed_namespace_with_hello_file(&config_path, &namespace_id, b"hello from loon\n");

    let ls = run_file_command(
        FileCommand::Ls {
            config_path: config_path.clone(),
            selector: "demo:/".to_owned(),
        },
        &test_make_transport,
    )
    .expect("run ls");
    assert_eq!(ls, FileCommandOutput::Text("hello.txt\n".to_owned()));

    let stat = run_file_command(
        FileCommand::Stat {
            config_path: config_path.clone(),
            selector: "demo:/hello.txt".to_owned(),
        },
        &test_make_transport,
    )
    .expect("run stat");
    let stat_text = match stat {
        FileCommandOutput::Text(text) => text,
        other => panic!("expected text stat output, got {other:?}"),
    };
    assert!(stat_text.contains("absolute_path: /hello.txt"));
    assert!(stat_text.contains("inode_kind: file"));

    let download_dir = temp_dir.path().join("downloads");
    fs::create_dir_all(&download_dir).expect("create download dir");
    let get = run_file_command(
        FileCommand::Get {
            config_path: config_path.clone(),
            selector: "demo:/hello.txt".to_owned(),
            local_path: download_dir.clone(),
        },
        &test_make_transport,
    )
    .expect("run get");
    let get_text = match get {
        FileCommandOutput::Text(text) => text,
        other => panic!("expected text get output, got {other:?}"),
    };
    assert!(get_text.contains("size_bytes: 16"));
    assert_eq!(
        fs::read(download_dir.join("hello.txt")).expect("read downloaded file"),
        b"hello from loon\n"
    );

    let cat = run_file_command(
        FileCommand::Cat {
            config_path,
            selector: "demo:/hello.txt".to_owned(),
        },
        &test_make_transport,
    )
    .expect("run cat");
    assert_eq!(cat, FileCommandOutput::Bytes(b"hello from loon\n".to_vec()));
}

#[test]
fn get_fails_closed_for_existing_target_and_missing_parent() {
    let temp_dir = TestDir::new("loon-ops-file-get-errors");
    let config_path = write_local_fs_config(temp_dir.path());
    let namespace_id = NamespaceId::from("demo");
    seed_namespace_with_hello_file(&config_path, &namespace_id, b"hello from loon\n");

    let existing_target = temp_dir.path().join("existing.txt");
    fs::write(&existing_target, b"present").expect("seed existing target");
    let existing_error = run_file_command(
        FileCommand::Get {
            config_path: config_path.clone(),
            selector: "demo:/hello.txt".to_owned(),
            local_path: existing_target,
        },
        &test_make_transport,
    )
    .expect_err("existing target should fail");
    assert!(existing_error.to_string().contains("already exists"));

    let missing_parent = temp_dir.path().join("missing/download.txt");
    let missing_parent_error = run_file_command(
        FileCommand::Get {
            config_path,
            selector: "demo:/hello.txt".to_owned(),
            local_path: missing_parent,
        },
        &test_make_transport,
    )
    .expect_err("missing parent should fail");
    assert!(missing_parent_error
        .to_string()
        .contains("parent directory is missing"));
}

fn write_local_fs_config(root: &Path) -> PathBuf {
    let object_store_root = root.join("object-store");
    let state_db_path = root.join("client.sqlite3");
    let mirror_root = root.join("mirror");
    fs::create_dir_all(&object_store_root).expect("create object store root");
    fs::create_dir_all(&mirror_root).expect("create mirror root");
    let config = OpsConfig {
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
            writer_version: "loon-ops-test".to_owned(),
            lease_duration_ms: 60_000,
        },
        ops: OpsSection {
            now_ms: Some(1_000),
            max_steps: None,
        },
    };
    let config_path = root.join("loondb-demo.local.toml");
    fs::write(
        &config_path,
        toml::to_string_pretty(&config).expect("serialize config"),
    )
    .expect("write config");
    config_path
}

fn seed_namespace_with_hello_file(config_path: &Path, namespace_id: &NamespaceId, bytes: &[u8]) {
    let config = OpsConfig::load(config_path).expect("load config");
    let store = test_open_store(&config).expect("open store");
    bootstrap_namespace(
        &store,
        namespace_id,
        &NamespaceBootstrapParams {
            holder_id: config.server.writer_id.clone(),
            writer_version: config.server.writer_version.clone(),
            now_ms: config.ops.now_ms.expect("configured now_ms"),
            lease_duration_ms: config.server.lease_duration_ms,
            allow_existing: false,
        },
    )
    .expect("bootstrap namespace");

    let file_digest_sha256 = sha256_digest(bytes);
    let block_digest = sha256_digest(bytes);
    store
        .put_if_absent(&blob(namespace_id.as_str(), &block_digest), bytes)
        .expect("write content block");
    let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
        namespace_id: namespace_id.clone(),
        file_size_bytes: bytes.len() as u64,
        file_digest_sha256,
        block_size_bytes: bytes.len() as u64,
        blocks: vec![ContentBlockDescriptor {
            content_digest_sha256: block_digest,
            plaintext_size_bytes: bytes.len() as u64,
        }],
    })
    .expect("build manifest");
    let manifest_bytes = serde_json::to_vec(&manifest).expect("serialize manifest");
    let manifest_digest = sha256_digest(&manifest_bytes);
    store
        .put_if_absent(
            &content_manifest(namespace_id.as_str(), &manifest_digest),
            &manifest_bytes,
        )
        .expect("write manifest");

    execute_client_mutation(
        &store,
        &ClientMutationRequest {
            namespace_id: namespace_id.clone(),
            client_request_id: "create-file".to_owned(),
            op: ClientMutationOp::CreateFile {
                parent_inode_id: loon_types::InodeId(1),
                display_name: "hello.txt".to_owned(),
                content_manifest_digest: manifest_digest,
            },
        },
        &ClientMutationExecutionParams {
            writer_id: config.server.writer_id,
            writer_version: config.server.writer_version,
            now_ms: 2_000,
            lease_duration_ms: config.server.lease_duration_ms,
        },
    )
    .expect("create authoritative file");
}

fn test_open_store(config: &OpsConfig) -> anyhow::Result<ConfiguredObjectStore> {
    match &config.object_store {
        OpsObjectStoreSpec::LocalFs { root, key_prefix } => {
            ConfiguredObjectStore::local_fs(root, key_prefix.as_deref()).map_err(Into::into)
        }
        _ => anyhow::bail!("test only supports local-fs stores"),
    }
}
