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
fn profile_add_list_show_remove_work() {
    let harness = Harness::new();

    let add_local = harness.run(&[
        "--json",
        "profile",
        "add",
        "local-fs",
        "default",
        "--root",
        harness.store_root("default").to_str().unwrap(),
    ]);
    assert_success(&add_local);
    assert_eq!(json_data(&add_local)["mode"], "local");

    let external =
        harness.start_external_server(harness.write_server_config("remote", "profile-add-remote"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "add",
        "remote",
        "prod",
        "--server-url",
        &external.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);
    assert_eq!(json_data(&add_remote)["mode"], "remote");

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let list_data = json_data(&list);
    let profiles = list_data["profiles"].as_array().unwrap();
    assert_eq!(profiles.len(), 2);
    assert_eq!(profiles[0]["name"], "default");
    assert_eq!(profiles[0]["mode"], "local");
    assert_eq!(profiles[1]["name"], "prod");
    assert_eq!(profiles[1]["mode"], "remote");

    let show = harness.run(&["config", "show"]);
    assert_success(&show);
    let stdout = stdout_string(&show);
    assert!(stdout.contains("mode = \"remote\""));
    assert!(stdout.contains("REDACTED"));
    assert!(!stdout.contains("test-token"));

    let show_default = harness.run(&["--json", "profile", "show"]);
    assert_success(&show_default);
    assert_eq!(json_data(&show_default)["mode"], "local");

    let remove_default = harness.run(&["--json", "profile", "remove", "default"]);
    assert_success(&remove_default);
    assert_eq!(json_data(&remove_default)["name"], "default");

    let list_after_remove = harness.run(&["--json", "profile", "list"]);
    assert_success(&list_after_remove);
    assert!(json_data(&list_after_remove)["default_profile"].is_null());

    let show_without_default = harness.run(&["--json", "profile", "show"]);
    assert_failure(&show_without_default);
    assert_eq!(
        json_error(&show_without_default)["code"],
        "no_default_profile"
    );
}

#[test]
fn local_profile_filesystem_flow_works_end_to_end() {
    let harness = Harness::new();
    harness.add_local_profile("default");

    let upload_path = harness.temp_dir.path().join("upload.txt");
    let download_path = harness.temp_dir.path().join("downloaded.txt");
    fs::write(&upload_path, b"hello from direct core\n").expect("upload payload");

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
    assert_eq!(cat.stdout, b"hello from direct core\n");
    assert!(cat.stderr.is_empty());

    let get_stdout = harness.run(&["filesystem", "get", "demo", "/docs/hello.txt", "-"]);
    assert_success(&get_stdout);
    assert_eq!(get_stdout.stdout, b"hello from direct core\n");

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
        b"hello from direct core\n"
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
}

#[test]
fn init_creates_local_profile_and_sets_default() {
    let harness = Harness::new();

    let init = harness.run(&[
        "--json",
        "init",
        "mystore",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("mystore").to_str().unwrap(),
    ]);
    assert_success(&init);
    assert_eq!(json_data(&init)["mode"], "local");

    let show = harness.run(&["--json", "profile", "show"]);
    assert_success(&show);
    assert_eq!(json_data(&show)["mode"], "local");

    assert_success(&harness.run(&["namespace", "create", "demo"]));
    let list = harness.run(&["--json", "namespace", "list"]);
    assert_success(&list);
    let namespaces = json_data(&list)["namespaces"]
        .as_array()
        .unwrap()
        .to_owned();
    assert_eq!(namespaces.len(), 1);
}

#[test]
fn removing_last_profile_leaves_empty_config() {
    let harness = Harness::new();
    harness.add_local_profile("default");

    let remove = harness.run(&["--json", "--no-input", "profile", "remove", "default"]);
    assert_success(&remove);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let data = json_data(&list);
    assert!(data["default_profile"].is_null());
    assert_eq!(data["profiles"].as_array().unwrap().len(), 0);

    let show_config = harness.run(&["config", "show"]);
    assert_success(&show_config);
    assert!(!stdout_string(&show_config).contains("default_profile"));

    let show = harness.run(&["--json", "profile", "show"]);
    assert_failure(&show);
    assert_eq!(json_error(&show)["code"], "no_default_profile");
}

#[test]
fn removing_default_profile_requires_explicit_reselection() {
    let harness = Harness::new();
    harness.add_local_profile("alpha");
    harness.add_local_profile("beta");

    let remove = harness.run(&["--json", "--no-input", "profile", "remove", "alpha"]);
    assert_success(&remove);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let data = json_data(&list);
    assert!(data["default_profile"].is_null());
    assert_eq!(data["profiles"].as_array().unwrap().len(), 1);

    let show = harness.run(&["--json", "profile", "show"]);
    assert_failure(&show);
    assert_eq!(json_error(&show)["code"], "no_default_profile");

    let namespace = harness.run(&["--json", "namespace", "list"]);
    assert_failure(&namespace);
    assert_eq!(json_error(&namespace)["code"], "no_default_profile");

    let filesystem = harness.run(&["--json", "filesystem", "ls", "demo"]);
    assert_failure(&filesystem);
    assert_eq!(json_error(&filesystem)["code"], "no_default_profile");

    let make_default = harness.run(&["--json", "profile", "make-default", "beta"]);
    assert_success(&make_default);

    let show_after = harness.run(&["--json", "profile", "show"]);
    assert_success(&show_after);
    assert_eq!(json_data(&show_after)["mode"], "local");
}

#[test]
fn profile_update_changes_fields() {
    let harness = Harness::new();
    harness.add_local_profile("default");

    let new_root = harness.store_root("updated");
    let update = harness.run(&[
        "--json",
        "profile",
        "update",
        "default",
        "--root",
        new_root.to_str().unwrap(),
    ]);
    assert_success(&update);

    let show = harness.run(&["--json", "profile", "show", "default"]);
    assert_success(&show);
    let store = &json_data(&show)["store"];
    assert_eq!(store["root"], new_root.to_str().unwrap());
}

#[test]
fn profile_make_default_switches_default() {
    let harness = Harness::new();
    harness.add_local_profile("alpha");
    harness.add_local_profile("beta");

    let make_default = harness.run(&["--json", "profile", "make-default", "beta"]);
    assert_success(&make_default);
    assert_eq!(json_data(&make_default)["name"], "beta");

    let show = harness.run(&["--json", "profile", "show"]);
    assert_success(&show);
}

#[test]
fn profile_make_default_rejects_missing_profile() {
    let harness = Harness::new();
    harness.add_local_profile("default");

    let result = harness.run(&["--json", "profile", "make-default", "nonexistent"]);
    assert_failure(&result);
    assert_eq!(json_error(&result)["code"], "profile_not_found");
}

#[test]
fn reserved_profile_names_are_rejected() {
    let harness = Harness::new();

    let init_reserved = harness.run(&[
        "--json",
        "init",
        "default_profile",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("default_profile").to_str().unwrap(),
    ]);
    assert_failure(&init_reserved);
    assert_eq!(json_error(&init_reserved)["code"], "invalid_input");

    let add_reserved = harness.run(&[
        "--json",
        "profile",
        "add",
        "local-fs",
        "config_version",
        "--root",
        harness.store_root("config_version").to_str().unwrap(),
    ]);
    assert_failure(&add_reserved);
    assert_eq!(json_error(&add_reserved)["code"], "invalid_input");
}

#[test]
fn init_rejects_existing_config_file() {
    let harness = Harness::new();
    harness.write_cli_config(&format!(
        r#"
config_version = 1
default_profile = "default"

[default]
mode = "local"

[default.store]
kind = "local-fs"
root = "{}"
"#,
        harness.store_root("default").display()
    ));
    let existing = fs::read_to_string(&harness.config_path).expect("read existing config");

    let init = harness.run(&[
        "--json",
        "init",
        "mystore",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("mystore").to_str().unwrap(),
    ]);
    assert_failure(&init);
    let error = json_error(&init);
    assert_eq!(error["code"], "config_already_exists");
    let message = error["message"].as_str().unwrap();
    assert!(message.contains("config file already exists"));
    assert!(message.contains("loon profile add"));
    assert!(message.contains("loon profile update"));
    assert!(message.contains("loon profile make-default"));
    assert_eq!(
        fs::read_to_string(&harness.config_path).expect("read unchanged config"),
        existing
    );
}

#[test]
fn reserved_profile_names_in_config_are_rejected() {
    let harness = Harness::new();
    harness.write_cli_config(format!(
        r#"
config_version = 1

[default_profile]
mode = "local"

[default_profile.store]
kind = "local-fs"
root = "{}"
"#,
        harness.store_root("default_profile").display()
    ));

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    assert_eq!(json_error(&list)["code"], "invalid_config");
}

#[test]
fn empty_default_profile_in_config_is_rejected() {
    let harness = Harness::new();
    harness.write_cli_config(
        r#"
config_version = 1
default_profile = ""
"#,
    );

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    let error = json_error(&list);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("default_profile"));
}

#[test]
fn whitespace_default_profile_in_config_is_rejected() {
    let harness = Harness::new();
    harness.write_cli_config(
        r#"
config_version = 1
default_profile = "   "
"#,
    );

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    let error = json_error(&list);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("default_profile"));
}

#[test]
fn invalid_store_field_messages_use_flattened_paths() {
    let harness = Harness::new();
    harness.write_cli_config(
        r#"
config_version = 1
default_profile = "default"

[default]
mode = "local"

[default.store]
kind = "local-fs"
root = ""
"#,
    );

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    let error = json_error(&list);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("default.store.root"));
}

#[test]
fn invalid_remote_urls_are_rejected() {
    let harness = Harness::new();

    let missing_host_http = harness.run(&[
        "--json",
        "profile",
        "add",
        "remote",
        "default",
        "--server-url",
        "http://",
    ]);
    assert_failure(&missing_host_http);
    assert_eq!(json_error(&missing_host_http)["code"], "invalid_config");
    assert!(json_error(&missing_host_http)["message"]
        .as_str()
        .unwrap()
        .contains("default.server_url"));

    let missing_host_https = harness.run(&[
        "--json",
        "profile",
        "add",
        "remote",
        "default",
        "--server-url",
        "https://",
    ]);
    assert_failure(&missing_host_https);
    assert_eq!(json_error(&missing_host_https)["code"], "invalid_config");
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
        "default",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let create = harness.run(&["--json", "namespace", "create", "demo"]);
    assert_success(&create);

    let list = harness.run(&["--json", "namespace", "list"]);
    assert_success(&list);
    let list_data = json_data(&list);
    let namespaces = list_data["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0]["name"], "demo");
}

struct Harness {
    temp_dir: TempDir,
    home_dir: PathBuf,
    config_path: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let home_dir = temp_dir.path().join("home");
        fs::create_dir_all(&home_dir).expect("create temp home");
        Self {
            config_path: home_dir.join(".loonfs").join("config.toml"),
            home_dir,
            temp_dir,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(loon_binary_path())
            .env("HOME", &self.home_dir)
            .args(args)
            .output()
            .expect("run loon")
    }

    fn store_root(&self, name: &str) -> PathBuf {
        self.temp_dir.path().join(format!("{name}-store"))
    }

    fn add_local_profile(&self, name: &str) {
        let output = self.run(&[
            "--json",
            "profile",
            "add",
            "local-fs",
            name,
            "--root",
            self.store_root(name).to_str().unwrap(),
        ]);
        assert_success(&output);
    }

    fn write_cli_config(&self, contents: impl AsRef<[u8]>) {
        fs::create_dir_all(self.config_path.parent().expect("config dir"))
            .expect("create config dir");
        fs::write(&self.config_path, contents).expect("write cli config");
    }

    fn write_server_config(&self, name: &str, key_prefix: &str) -> PathBuf {
        let bind = format!("127.0.0.1:{}", available_port());
        let path = self.temp_dir.path().join(format!("{name}.loond.toml"));
        let store_root = self.store_root(name);
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

#[test]
fn help_omits_config_flag() {
    let harness = Harness::new();
    let output = Command::new(loon_binary_path())
        .env("HOME", &harness.home_dir)
        .arg("--help")
        .output()
        .expect("run help");
    assert_success(&output);
    assert!(!stdout_string(&output).contains("--config"));
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
