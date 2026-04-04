use loon_api::{ApiError, AuthoritativePathEntry, NamespaceSummary};
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loon_core::{bootstrap_namespace, MutationContext};
use loon_server::{app, ServerConfig, StoreConfig};
use serde_json::json;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_read_commands_match_pretty_server_responses() {
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
    assert_pretty_stdout(&namespace_output, &expected_namespaces);

    let list_output = run_loon(
        harness.client_config_path(),
        &["file", "ls", "demo:/docs", "--json"],
    );
    assert_success(&list_output);
    let expected_entries: Vec<AuthoritativePathEntry> = harness
        .client
        .list_path(&NamespacePath::parse("demo:/docs").expect("docs path"))
        .expect("list docs");
    assert_pretty_stdout(&list_output, &expected_entries);

    let stat_output = run_loon(
        harness.client_config_path(),
        &["file", "stat", "demo:/docs/hello.txt", "--json"],
    );
    assert_success(&stat_output);
    let expected_entry = harness.client.stat_path(&target).expect("stat file");
    assert_pretty_stdout(&stat_output, &expected_entry);

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_mutation_commands_emit_stable_payloads() {
    let harness = start_harness().await;
    let upload_path = harness.temp_dir.path().join("upload.txt");
    let download_path = harness.temp_dir.path().join("downloaded.txt");
    let payload = b"hello from cli\n";
    fs::write(&upload_path, payload).expect("write upload file");

    let create_output = run_loon(
        harness.client_config_path(),
        &["namespace", "create", "demo", "--json"],
    );
    assert_success(&create_output);
    assert_pretty_stdout(
        &create_output,
        &NamespaceSummary {
            name: "demo".into(),
        },
    );

    let put_output = run_loon(
        harness.client_config_path(),
        &[
            "file",
            "put",
            upload_path.to_str().expect("upload path"),
            "demo:/docs/hello.txt",
            "--json",
        ],
    );
    assert_success(&put_output);
    assert_pretty_stdout(
        &put_output,
        &json!({
            "target": "demo:/docs/hello.txt",
            "committed_seq": 1u64,
        }),
    );

    let get_output = run_loon(
        harness.client_config_path(),
        &[
            "file",
            "get",
            "demo:/docs/hello.txt",
            download_path.to_str().expect("download path"),
            "--json",
        ],
    );
    assert_success(&get_output);
    assert_pretty_stdout(
        &get_output,
        &json!({
            "target": "demo:/docs/hello.txt",
            "destination": download_path.display().to_string(),
            "bytes_written": payload.len() as u64,
        }),
    );
    assert_eq!(
        fs::read(&download_path).expect("read downloaded file"),
        payload
    );

    let move_output = run_loon(
        harness.client_config_path(),
        &[
            "file",
            "mv",
            "demo:/docs/hello.txt",
            "demo:/docs/renamed.txt",
            "--json",
        ],
    );
    assert_success(&move_output);
    assert_pretty_stdout(
        &move_output,
        &json!({
            "from": "demo:/docs/hello.txt",
            "to": "demo:/docs/renamed.txt",
            "committed_seq": 2u64,
        }),
    );

    let rm_output = run_loon(
        harness.client_config_path(),
        &["file", "rm", "demo:/docs/renamed.txt", "--json"],
    );
    assert_success(&rm_output);
    assert_pretty_stdout(
        &rm_output,
        &json!({
            "target": "demo:/docs/renamed.txt",
            "committed_seq": 3u64,
        }),
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_commands_and_raw_cat_succeed_end_to_end() {
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

    let cat_json_output = run_loon(
        harness.client_config_path(),
        &["file", "cat", "demo:/docs/renamed.txt", "--json"],
    );
    assert_failure(&cat_json_output);

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
fn invalid_client_config_json_errors_use_invalid_config() {
    let config_path = write_temp_file(
        "invalid-client.toml",
        r#"
server_url = "ftp://example.com"
auth_token = "dev-token"
"#,
    );

    let output = run_loon(&config_path, &["namespace", "list", "--json"]);

    assert_failure(&output);
    assert_json_stderr(
        &output,
        &ApiError {
            code: "invalid_config".to_owned(),
            message: "invalid `server_url`: scheme must be http or https, got `ftp`".to_owned(),
        },
    );
}

#[test]
fn invalid_namespace_path_json_errors_use_invalid_target() {
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
    assert_json_stderr(
        &output,
        &ApiError {
            code: "invalid_target".to_owned(),
            message: "invalid namespace path `not-a-namespace-path`".to_owned(),
        },
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_io_failures_render_as_io_error_in_json_mode() {
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

    let blocked_parent = harness.temp_dir.path().join("blocked-parent");
    fs::write(&blocked_parent, b"not a directory").expect("write blocked parent");
    let blocked_destination = blocked_parent.join("out.txt");

    let output = run_loon(
        harness.client_config_path(),
        &[
            "file",
            "get",
            "demo:/docs/hello.txt",
            blocked_destination.to_str().expect("blocked destination"),
            "--json",
        ],
    );

    assert_failure(&output);
    let error = parse_json_stderr(&output);
    assert_eq!(error.code, "io_error");
    assert!(
        error.message.starts_with("i/o error:"),
        "expected io_error message, got {}",
        error.message
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_remote_path_not_found_error_is_preserved() {
    let harness = start_harness().await;
    harness
        .client
        .create_namespace("demo")
        .expect("create namespace");

    let expected = match harness
        .client
        .stat_path(&NamespacePath::parse("demo:/missing.txt").expect("missing path"))
    {
        Err(ClientError::Api { code, message, .. }) => ApiError { code, message },
        other => panic!("expected direct api error, got {other:?}"),
    };

    let output = run_loon(
        harness.client_config_path(),
        &["file", "stat", "demo:/missing.txt", "--json"],
    );

    assert_failure(&output);
    assert_json_stderr(&output, &expected);

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_remote_lease_conflict_error_is_preserved() {
    let harness = start_harness_with_seeded_namespace("other-writer", "server-writer").await;
    let upload_path = harness.temp_dir.path().join("blocked-upload.txt");
    fs::write(&upload_path, b"blocked\n").expect("write upload file");
    let target = NamespacePath::parse("demo:/docs/blocked.txt").expect("target");

    let expected = match harness.client.write_file_bytes(&target, b"blocked\n") {
        Err(ClientError::Api { code, message, .. }) => ApiError { code, message },
        other => panic!("expected direct api error, got {other:?}"),
    };

    let output = run_loon(
        harness.client_config_path(),
        &[
            "file",
            "put",
            upload_path.to_str().expect("upload path"),
            "demo:/docs/blocked.txt",
            "--json",
        ],
    );

    assert_failure(&output);
    assert_json_stderr(&output, &expected);

    harness.server.abort();
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
    let config = test_config(
        temp_dir.path().join("store"),
        "loond-cli-test",
        "cli-tests",
        60_000,
    );
    start_harness_with_config(temp_dir, config).await
}

async fn start_harness_with_seeded_namespace(
    seed_writer_id: &str,
    server_writer_id: &str,
) -> TestHarness {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config = test_config(
        temp_dir.path().join("store"),
        server_writer_id,
        "cli-tests",
        60_000,
    );
    let store = config.object_store().expect("construct object store");
    bootstrap_namespace(&store, &"demo".into(), &context(seed_writer_id), false)
        .expect("bootstrap namespace");
    start_harness_with_config(temp_dir, config).await
}

async fn start_harness_with_config(temp_dir: TempDir, config: ServerConfig) -> TestHarness {
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

fn test_config(
    store_root: PathBuf,
    writer_id: &str,
    key_prefix: &str,
    lease_duration_ms: u64,
) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".to_owned()),
        writer_id: writer_id.to_owned(),
        writer_version: format!("{writer_id}/0.1.0"),
        lease_duration_ms,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: Some(key_prefix.to_owned()),
        },
    }
}

fn context(writer_id: &str) -> MutationContext {
    MutationContext {
        writer_id: writer_id.to_owned(),
        writer_version: format!("{writer_id}/0.1.0"),
        now_ms: now_ms(),
        lease_duration_ms: 60_000,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
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
        "expected loon binary at {}",
        candidate.display()
    );
    candidate
}

fn write_temp_file(name: &str, contents: &str) -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("loondb-cli-test-{}-{}", std::process::id(), name));
    fs::write(&path, contents).expect("write temp file");
    path
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "expected success, got status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        stdout_string(output),
        stderr_string(output)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "expected failure, got success\nstdout:\n{}\nstderr:\n{}",
        stdout_string(output),
        stderr_string(output)
    );
}

fn assert_pretty_stdout<T>(output: &Output, expected: &T)
where
    T: serde::Serialize,
{
    assert_eq!(
        stdout_string(output),
        format!(
            "{}\n",
            serde_json::to_string_pretty(expected).expect("render expected json")
        )
    );
}

fn assert_json_stderr(output: &Output, expected: &ApiError) {
    assert_eq!(parse_json_stderr(output), *expected);
}

fn parse_json_stderr(output: &Output) -> ApiError {
    serde_json::from_slice(&output.stderr).expect("parse stderr json")
}

fn stdout_string(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout utf8")
}

fn stderr_string(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr utf8")
}
