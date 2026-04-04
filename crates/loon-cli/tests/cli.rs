use serde_json::Value;
use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn unsupported_config_version_is_rejected() {
    let harness = Harness::new();
    fs::write(
        harness.config_path(),
        r#"
config_version = 2

[profiles.local]
mode = "local"

[profiles.local.local]
server_config_path = "/tmp/loond.toml"
"#,
    )
    .expect("write config");

    let output = harness.run(&["--json", "profile", "list"]);
    assert_failure(&output);
    assert_eq!(json_error(&output)["code"], "invalid_config");
    assert!(json_error(&output)["message"]
        .as_str()
        .unwrap()
        .contains("unsupported `config_version`"));
}

#[test]
fn profile_add_list_show_use_remove_and_config_show_work() {
    let harness = Harness::new();
    let local_config = harness.write_server_config("local", "profile-add-local");

    let add_local = harness.run(&[
        "--json",
        "profile",
        "add",
        "local",
        "local",
        "--server-config",
        local_config.to_str().unwrap(),
    ]);
    assert_success(&add_local);
    assert_eq!(json_data(&add_local)["name"], "local");
    assert_eq!(json_data(&add_local)["active"], true);

    let external =
        harness.start_external_server(harness.write_server_config("remote", "profile-add-remote"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "add",
        "remote",
        "remote",
        "--server-url",
        &external.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);
    assert_eq!(json_data(&add_remote)["name"], "remote");
    assert_eq!(json_data(&add_remote)["active"], false);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let list_data = json_data(&list);
    let profiles = list_data["profiles"].as_array().unwrap();
    assert_eq!(profiles.len(), 2);
    assert_eq!(profiles[0]["name"], "local");
    assert_eq!(profiles[0]["active"], true);
    assert_eq!(profiles[1]["name"], "remote");
    assert_eq!(profiles[1]["active"], false);

    let show = harness.run(&["config", "show"]);
    assert_success(&show);
    let stdout = stdout_string(&show);
    assert!(stdout.contains("mode = \"remote\""));
    assert!(stdout.contains("REDACTED"));
    assert!(!stdout.contains("test-token"));

    let use_remote = harness.run(&["--json", "profile", "use", "remote"]);
    assert_success(&use_remote);
    assert_eq!(json_data(&use_remote)["name"], "remote");
    assert_eq!(json_data(&use_remote)["active"], true);

    let show_remote = harness.run(&["--json", "profile", "show"]);
    assert_success(&show_remote);
    assert_eq!(json_data(&show_remote)["name"], "remote");
    assert_eq!(json_data(&show_remote)["mode"], "remote");

    let remove_local = harness.run(&["--json", "profile", "remove", "local"]);
    assert_success(&remove_local);
    assert_eq!(json_data(&remove_local)["name"], "local");
}

#[test]
fn no_input_rejects_missing_profile_fields_and_keeps_stdout_empty() {
    let harness = Harness::new();
    let output = harness.run(&["--json", "--no-input", "profile", "add", "local", "local"]);

    assert_failure(&output);
    assert!(output.stdout.is_empty());
    assert_eq!(
        json_error(&output)["code"],
        "non_interactive_input_required"
    );
}

#[test]
fn profile_add_local_accepts_server_config_relative_to_shell_cwd() {
    let harness = Harness::new();
    let nested_dir = harness.temp_dir.path().join("configs");
    fs::create_dir_all(&nested_dir).expect("nested configs dir");
    let config_path = nested_dir.join("loon.local.toml");
    let server_config_source = harness.write_server_config("cwd-relative", "cwd-relative");
    let server_config = nested_dir.join("cwd-relative.loond.toml");
    fs::copy(&server_config_source, &server_config).expect("copy server config into configs dir");
    let relative_server_config = PathBuf::from("configs").join("cwd-relative.loond.toml");

    let output = Command::new(loon_binary_path())
        .current_dir(harness.temp_dir.path())
        .arg("--config")
        .arg(&config_path)
        .arg("--json")
        .arg("profile")
        .arg("add")
        .arg("local")
        .arg("local")
        .arg("--server-config")
        .arg(&relative_server_config)
        .output()
        .expect("run loon with cwd-relative config path");

    assert_success(&output);
    let stored = fs::read_to_string(&config_path).expect("read stored config");
    assert!(stored.contains(&server_config.display().to_string()));
}

#[test]
fn local_up_status_down_and_stale_cleanup_work() {
    let harness = Harness::new();
    let local_config = harness.write_server_config("alpha", "local-up-status");
    harness.add_local_profile("alpha", &local_config);

    let up = harness.run(&["--json", "--profile", "alpha", "local", "up"]);
    assert_success(&up);
    let pid = json_data(&up)["pid"].as_u64().unwrap() as u32;

    let status = harness.run(&["--json", "--profile", "alpha", "local", "status"]);
    assert_success(&status);
    assert_eq!(json_data(&status)["status"], "running");

    let double_up = harness.run(&["--json", "--profile", "alpha", "local", "up"]);
    assert_failure(&double_up);
    assert_eq!(
        json_error(&double_up)["code"],
        "local_server_already_running"
    );

    terminate_pid(pid);
    wait_for_stale(&harness, "alpha");

    let stale = harness.run(&["--json", "--profile", "alpha", "local", "status"]);
    assert_success(&stale);
    assert_eq!(json_data(&stale)["status"], "stale");

    let up_again = harness.run(&["--json", "--profile", "alpha", "local", "up"]);
    assert_success(&up_again);
    assert_eq!(json_data(&up_again)["status"], "running");

    let down = harness.run(&["--json", "--profile", "alpha", "local", "down"]);
    assert_success(&down);
    assert_eq!(json_data(&down)["status"], "stopped");
}

#[test]
fn managed_local_http_filesystem_flow_works_end_to_end() {
    let harness = Harness::new();
    let local_config = harness.write_server_config("local", "managed-local-flow");
    harness.add_local_profile("local", &local_config);
    assert_success(&harness.run(&["--profile", "local", "local", "up"]));

    let upload_path = harness.temp_dir.path().join("upload.txt");
    let download_path = harness.temp_dir.path().join("downloaded.txt");
    fs::write(&upload_path, b"hello over managed loond\n").expect("upload payload");

    assert_success(&harness.run(&["namespace", "create", "demo"]));

    let put = harness.run(&[
        "--json",
        "filesystem",
        "put",
        "demo",
        upload_path.to_str().unwrap(),
        "/docs/hello.txt",
    ]);
    assert_success(&put);
    assert_eq!(json_data(&put)["target"], "demo:/docs/hello.txt");

    let put_conflict = harness.run(&[
        "--json",
        "filesystem",
        "put",
        "demo",
        upload_path.to_str().unwrap(),
        "/docs/hello.txt",
    ]);
    assert_failure(&put_conflict);
    assert_eq!(json_error(&put_conflict)["code"], "path_conflict");

    let put_force = harness.run(&[
        "--json",
        "filesystem",
        "put",
        "demo",
        upload_path.to_str().unwrap(),
        "/docs/hello.txt",
        "--force",
    ]);
    assert_success(&put_force);

    let cp = harness.run(&[
        "--json",
        "filesystem",
        "cp",
        "demo",
        "/docs/hello.txt",
        "/docs/copy.txt",
    ]);
    assert_success(&cp);

    let source = harness.run(&["--json", "filesystem", "stat", "demo", "/docs/hello.txt"]);
    let copy = harness.run(&["--json", "filesystem", "stat", "demo", "/docs/copy.txt"]);
    assert_success(&source);
    assert_success(&copy);
    assert_ne!(json_data(&source)["inode_id"], json_data(&copy)["inode_id"]);
    assert_eq!(
        json_data(&source)["content_manifest_digest"],
        json_data(&copy)["content_manifest_digest"]
    );

    let cat = harness.run(&["filesystem", "cat", "demo", "/docs/hello.txt"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"hello over managed loond\n");
    assert!(cat.stderr.is_empty());

    let get_stdout = harness.run(&["filesystem", "get", "demo", "/docs/hello.txt", "-"]);
    assert_success(&get_stdout);
    assert_eq!(get_stdout.stdout, b"hello over managed loond\n");

    let get_file = harness.run(&[
        "--json",
        "filesystem",
        "get",
        "demo",
        "/docs/hello.txt",
        download_path.to_str().unwrap(),
    ]);
    assert_success(&get_file);
    assert_eq!(
        fs::read(&download_path).expect("downloaded bytes"),
        b"hello over managed loond\n"
    );

    let mv = harness.run(&[
        "--json",
        "filesystem",
        "mv",
        "demo",
        "/docs/copy.txt",
        "/docs/final.txt",
    ]);
    assert_success(&mv);

    let rm_dir = harness.run(&["--json", "filesystem", "rm", "demo", "/docs"]);
    assert_failure(&rm_dir);
    assert_eq!(json_error(&rm_dir)["code"], "path_conflict");

    let rm = harness.run(&["--json", "filesystem", "rm", "demo", "/docs/final.txt"]);
    assert_success(&rm);

    let down = harness.run(&["--profile", "local", "local", "down"]);
    assert_success(&down);
}

#[test]
fn external_remote_profile_executes_through_http() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "remote-exec"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "add",
        "remote",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let create = harness.run(&[
        "--json",
        "--profile",
        "remote",
        "namespace",
        "create",
        "demo",
    ]);
    assert_success(&create);

    let list = harness.run(&["--json", "--profile", "remote", "namespace", "list"]);
    assert_success(&list);
    let list_data = json_data(&list);
    let namespaces = list_data["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0]["name"], "demo");
}

struct Harness {
    temp_dir: TempDir,
    config_path: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        Self {
            config_path: temp_dir.path().join("config.toml"),
            temp_dir,
        }
    }

    fn config_path(&self) -> &Path {
        &self.config_path
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(loon_binary_path())
            .arg("--config")
            .arg(&self.config_path)
            .args(args)
            .output()
            .expect("run loon")
    }

    fn add_local_profile(&self, name: &str, server_config_path: &Path) {
        let output = self.run(&[
            "--json",
            "profile",
            "add",
            "local",
            name,
            "--server-config",
            server_config_path.to_str().unwrap(),
        ]);
        assert_success(&output);
    }

    fn write_server_config(&self, name: &str, key_prefix: &str) -> PathBuf {
        let bind = format!("127.0.0.1:{}", available_port());
        let path = self.temp_dir.path().join(format!("{name}.loond.toml"));
        let store_root = self.temp_dir.path().join(format!("{name}-store"));
        let contents = format!(
            r#"
bind = "{bind}"
auth_token = "test-token"
writer_id = "{name}"
writer_version = "{name}/0.1.0"
lease_duration_ms = 200

[store]
kind = "local-fs"
root = "{}"
key_prefix = "{key_prefix}"
"#,
            store_root.display()
        );
        fs::write(&path, contents).expect("write server config");
        path
    }

    fn start_external_server(&self, server_config_path: PathBuf) -> ExternalServer {
        let child = Command::new(loond_binary_path())
            .arg("--config")
            .arg(&server_config_path)
            .spawn()
            .expect("spawn loond");
        let server_url = server_url_from_config(&server_config_path);
        wait_for_healthz(&server_url);
        ExternalServer { child, server_url }
    }
}

struct ExternalServer {
    child: Child,
    server_url: String,
}

impl Drop for ExternalServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

fn loond_binary_path() -> PathBuf {
    if let Some(path) = env::var_os("CARGO_BIN_EXE_loond") {
        return PathBuf::from(path);
    }

    let current_exe = env::current_exe().expect("current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|path| path.parent())
        .expect("target debug dir");
    let candidate = debug_dir.join(if cfg!(windows) { "loond.exe" } else { "loond" });
    assert!(
        candidate.exists(),
        "expected loond binary at {}",
        candidate.display()
    );
    candidate
}

fn server_url_from_config(path: &Path) -> String {
    let config = fs::read_to_string(path).expect("read server config");
    let bind = config
        .lines()
        .find_map(|line| line.trim().strip_prefix("bind = "))
        .expect("bind line")
        .trim_matches('"')
        .to_owned();
    format!("http://{bind}")
}

fn wait_for_healthz(server_url: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if ureq::get(&format!("{server_url}/healthz")).call().is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for {server_url}/healthz");
}

fn wait_for_stale(harness: &Harness, profile: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let output = harness.run(&["--json", "--profile", profile, "local", "status"]);
        assert_success(&output);
        if json_data(&output)["status"] == "stale" {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for stale local runtime");
}

fn terminate_pid(pid: u32) {
    #[cfg(unix)]
    {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .expect("kill process");
        assert!(status.success(), "failed to terminate pid {pid}");
    }
    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .status()
            .expect("taskkill process");
        assert!(status.success(), "failed to terminate pid {pid}");
    }
}

fn available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "expected success, got {:?}\nstdout:\n{}\nstderr:\n{}",
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

fn stdout_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn parse_json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("parse json")
}

fn json_data(output: &Output) -> Value {
    parse_json(&output.stdout)["data"].clone()
}

fn json_error(output: &Output) -> Value {
    parse_json(&output.stderr)["error"].clone()
}
