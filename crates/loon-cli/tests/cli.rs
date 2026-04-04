use loon_api::{AuthoritativePathEntry, NamespaceSummary};
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loon_server::{app, ServerConfig, StoreConfig};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_commands_match_pretty_server_responses() {
    let harness = start_harness().await;
    harness
        .client
        .create_namespace("demo")
        .expect("create namespace");
    let target = NamespacePath::parse("demo:/docs/hello.txt").expect("target");
    harness
        .client
        .write_file_bytes(&target, b"hello from cli\n")
        .expect("write file");

    let namespace_output = run_loon(
        harness.client_config_path(),
        &["namespace", "list", "--json"],
    );
    assert_success(&namespace_output);
    let expected_namespaces: Vec<NamespaceSummary> =
        harness.client.list_namespaces().expect("list namespaces");
    assert_eq!(
        stdout_string(&namespace_output),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&expected_namespaces).expect("render namespaces json")
        )
    );

    let list_output = run_loon(
        harness.client_config_path(),
        &["file", "ls", "demo:/docs", "--json"],
    );
    assert_success(&list_output);
    let expected_entries: Vec<AuthoritativePathEntry> = harness
        .client
        .list_path(&NamespacePath::parse("demo:/docs").expect("docs path"))
        .expect("list docs");
    assert_eq!(
        stdout_string(&list_output),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&expected_entries).expect("render entries json")
        )
    );

    let stat_output = run_loon(
        harness.client_config_path(),
        &["file", "stat", "demo:/docs/hello.txt", "--json"],
    );
    assert_success(&stat_output);
    let expected_entry = harness.client.stat_path(&target).expect("stat file");
    assert_eq!(
        stdout_string(&stat_output),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&expected_entry).expect("render stat json")
        )
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_commands_succeed_end_to_end() {
    let harness = start_harness().await;
    let upload_path = harness.temp_dir.path().join("upload.txt");
    let download_path = harness.temp_dir.path().join("downloaded.txt");
    let payload = b"hello from cli\n";
    fs::write(&upload_path, payload).expect("write upload file");

    assert_success(&run_loon(
        harness.client_config_path(),
        &["namespace", "create", "demo"],
    ));
    assert_success(&run_loon(
        harness.client_config_path(),
        &[
            "file",
            "put",
            upload_path.to_str().expect("upload path"),
            "demo:/docs/hello.txt",
        ],
    ));
    assert_success(&run_loon(
        harness.client_config_path(),
        &[
            "file",
            "mv",
            "demo:/docs/hello.txt",
            "demo:/docs/renamed.txt",
        ],
    ));

    let cat_output = run_loon(
        harness.client_config_path(),
        &["file", "cat", "demo:/docs/renamed.txt"],
    );
    assert_success(&cat_output);
    assert_eq!(cat_output.stdout, payload);

    assert_success(&run_loon(
        harness.client_config_path(),
        &[
            "file",
            "get",
            "demo:/docs/renamed.txt",
            download_path.to_str().expect("download path"),
        ],
    ));
    assert_eq!(
        fs::read(&download_path).expect("read downloaded file"),
        payload
    );

    assert_success(&run_loon(
        harness.client_config_path(),
        &["file", "rm", "demo:/docs/renamed.txt"],
    ));

    let stat_result = harness
        .client
        .stat_path(&NamespacePath::parse("demo:/docs/renamed.txt").expect("renamed path"));
    match stat_result {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
        other => panic!("expected path_not_found after rm, got {other:?}"),
    }

    harness.server.abort();
}

#[test]
fn invalid_client_config_fails_before_request_execution() {
    let config_path = write_temp_file(
        "invalid-client.toml",
        r#"
server_url = "ftp://example.com"
auth_token = "dev-token"
"#,
    );

    let output = run_loon(&config_path, &["namespace", "list"]);

    assert_failure(&output);
    assert!(
        stderr_string(&output).contains("invalid `server_url`"),
        "stderr did not contain invalid server_url message: {}",
        stderr_string(&output)
    );
}

#[test]
fn invalid_namespace_path_fails_locally() {
    let config_path = write_temp_file(
        "valid-client.toml",
        r#"
server_url = "http://127.0.0.1:65535"
auth_token = "dev-token"
"#,
    );

    let output = run_loon(
        &config_path,
        &["file", "stat", "not-a-namespace-path", "--json"],
    );

    assert_failure(&output);
    assert!(
        stderr_string(&output).contains("invalid namespace path"),
        "stderr did not contain invalid namespace path message: {}",
        stderr_string(&output)
    );
}

struct TestHarness {
    temp_dir: TempDir,
    client: Client,
    client_config_path: PathBuf,
    server: tokio::task::JoinHandle<()>,
}

impl TestHarness {
    fn client_config_path(&self) -> &Path {
        &self.client_config_path
    }
}

async fn start_harness() -> TestHarness {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let config = ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".to_owned()),
        writer_id: "loond-cli-test".to_owned(),
        writer_version: "loond-cli-test/0.1.0".to_owned(),
        lease_duration_ms: 60_000,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: Some("cli-tests".to_owned()),
        },
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app(config).expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let client_config_path = temp_dir.path().join("client.toml");
    fs::write(
        &client_config_path,
        format!(
            "server_url = \"http://{}\"\nauth_token = \"test-token\"\n",
            addr
        ),
    )
    .expect("write client config");

    TestHarness {
        client: Client::new(ClientConfig {
            server_url: format!("http://{}", addr),
            auth_token: Some("test-token".to_owned()),
        }),
        client_config_path,
        server,
        temp_dir,
    }
}

fn run_loon(config_path: &Path, args: &[&str]) -> Output {
    Command::new(loon_binary_path())
        .arg("--config")
        .arg(config_path)
        .args(args)
        .output()
        .expect("run loon")
}

fn loon_binary_path() -> PathBuf {
    if let Some(path) = env::var_os("CARGO_BIN_EXE_loon") {
        return PathBuf::from(path);
    }

    let current_exe = env::current_exe().expect("current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|path| path.parent())
        .expect("target debug dir");
    let candidate = debug_dir.join(if cfg!(windows) { "loon.exe" } else { "loon" });
    assert!(
        candidate.exists(),
        "loon binary not found at {}",
        candidate.display()
    );
    candidate
}

fn write_temp_file(name: &str, contents: &str) -> PathBuf {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join(name);
    fs::write(&path, contents).expect("write temp file");
    let _ = temp_dir.keep();
    path
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "expected success, status={:?}, stdout={}, stderr={}",
        output.status.code(),
        stdout_string(output),
        stderr_string(output)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "expected failure, stdout={}, stderr={}",
        stdout_string(output),
        stderr_string(output)
    );
}

fn stdout_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
