//! Process harness and helpers shared by the CLI integration suites.

pub(super) use loonfs_test_support::http::raw_agent;
pub(super) use serde_json::Value;
pub(super) use std::env;
pub(super) use std::fs;
pub(super) use std::io::{Read, Write};
pub(super) use std::net::TcpListener;
pub(super) use std::path::{Path, PathBuf};
pub(super) use std::process::{Child, Command, Output, Stdio};
pub(super) use std::thread;
pub(super) use std::time::{Duration, Instant};
pub(super) use tempfile::TempDir;

/// A valid config with no profiles.
pub(super) const MINIMAL_CONFIG: &str = "config_version = 1\n";

/// A file small enough to upload in one request during recursive tests.
pub(super) const SMALL_TREE_FILE: &[u8] = b"small enough to hold";

/// The smallest payload used to exercise the streaming upload path.
pub(super) fn streaming_payload() -> Vec<u8> {
    let len = 8 * 1024 * 1024 + 1_024;
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

/// Reads a remote file back through the CLI and returns its bytes.
pub(super) fn download(harness: &Harness, remote_path: &str, name: &str) -> Vec<u8> {
    let local = harness.temp_dir.path().join(name);
    assert_success(&harness.run(&[
        "get",
        remote_path,
        local.to_str().expect("utf-8 path"),
        "--force",
    ]));
    fs::read(&local).expect("read downloaded file")
}

/// Events of one kind, in the order they were reported.
pub(super) fn events_of_kind(output: &Output, kind: &str) -> Vec<Value> {
    json_progress_events(output)
        .into_iter()
        .filter(|event| event["kind"] == kind)
        .collect()
}

/// A payload large enough to require several download chunks.
pub(super) fn multi_chunk_payload() -> Vec<u8> {
    let len = 3 * loonfs::CONTENT_READ_CHUNK_BYTES as usize + 1_024;
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

/// Returns the partial-data and metadata paths for an interrupted download.
pub(super) fn partial_paths(destination: &Path) -> (PathBuf, PathBuf) {
    let name = destination
        .file_name()
        .expect("destination file name")
        .to_str()
        .expect("utf-8 name");
    let parent = destination.parent().expect("destination parent");
    (
        parent.join(format!(".{name}.loonfs-partial")),
        parent.join(format!(".{name}.loonfs-partial.meta")),
    )
}

/// Creates the files left by a download interrupted after `held` bytes.
pub(super) fn leave_a_partial_download(
    harness: &Harness,
    remote_path: &str,
    destination: &Path,
    payload: &[u8],
    held: usize,
) {
    let stat = harness.run(&["--json", "stat", remote_path]);
    assert_success(&stat);
    let content_ref = json_data(&stat)["content_ref"].clone();
    let (partial, meta) = partial_paths(destination);
    fs::write(&partial, &payload[..held]).expect("write partial bytes");
    let note = serde_json::json!({
        "content_id": content_ref["content_id"],
        "size_bytes": content_ref["size_bytes"],
        "checksum": content_ref["checksum"],
        "revision_no": null,
    });
    fs::write(&meta, serde_json::to_vec(&note).expect("encode note")).expect("write note");
}

/// Finds the only content object with `size_bytes` under a store root.
///
/// Callers use a unique size so the match identifies one fixture object.
pub(super) fn content_object_path(store_root: &Path, size_bytes: u64) -> PathBuf {
    let objects = walkdir::WalkDir::new(store_root.join("content-stores"))
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .metadata()
                    .is_ok_and(|metadata| metadata.len() == size_bytes)
        })
        .map(|entry| entry.path().to_path_buf())
        .collect::<Vec<_>>();
    assert_eq!(
        objects.len(),
        1,
        "expected exactly one content object of {size_bytes} bytes, found {objects:?}"
    );
    objects.into_iter().next().expect("one content object")
}

/// Returns change-feed messages in commit order, omitting events without one.
pub(super) fn feed_messages(harness: &Harness) -> Vec<String> {
    let changes = harness.run(&["--json", "changes"]);
    assert_success(&changes);
    json_data(&changes)["changes"]
        .as_array()
        .expect("changes array")
        .iter()
        .filter_map(|row| row["message"].as_str().map(ToOwned::to_owned))
        .collect()
}

pub(super) fn backfilling_text_names_no_watermark(harness: &Harness) -> bool {
    let rendered = stdout_string(&harness.run(&["admin", "index", "status"]));
    rendered.contains("backfilling toward seq") && !rendered.contains("built through")
}

pub(super) struct Harness {
    pub(super) temp_dir: TempDir,
    pub(super) home_dir: PathBuf,
    pub(super) config_path: PathBuf,
}

impl Harness {
    pub(super) fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let home_dir = temp_dir.path().join("home");
        fs::create_dir_all(&home_dir).expect("create temp home");
        Self {
            config_path: home_dir.join(".loonfs").join("config.toml"),
            home_dir,
            temp_dir,
        }
    }

    pub(super) fn run(&self, args: &[&str]) -> Output {
        self.command().args(args).output().expect("run loonfs")
    }

    /// Runs the CLI with additional environment variables.
    pub(super) fn run_with_env<V: AsRef<std::ffi::OsStr>>(
        &self,
        variables: &[(&str, V)],
        args: &[&str],
    ) -> Output {
        let mut command = self.command();
        for (name, value) in variables {
            command.env(name, value);
        }
        command.args(args).output().expect("run loonfs")
    }

    /// Builds a command isolated from the developer's CLI configuration.
    pub(super) fn command(&self) -> Command {
        let mut command = Command::new(loon_binary_path());
        command
            .env("HOME", &self.home_dir)
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("LOONFS_CONFIG")
            .env_remove("LOONFS_PROFILE")
            .env_remove("LOONFS_NAMESPACE")
            .env_remove("LOONFS_ACTOR_KIND")
            .env_remove("LOONFS_ACTOR_ID");
        command
    }

    /// Runs a command printed by the CLI through a shell.
    ///
    /// This verifies that the printed quoting is valid. The helper replaces
    /// the `loonfs` executable with the test binary.
    pub(super) fn replay_in_shell(&self, command: &str) -> Output {
        let arguments = command
            .strip_prefix("loonfs ")
            .expect("a printed command invokes loonfs");
        let binary = loon_binary_path();
        let script = format!("'{}' {arguments}", binary.display());
        Command::new("sh")
            .arg("-c")
            .arg(&script)
            .env("HOME", &self.home_dir)
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("LOONFS_CONFIG")
            .env_remove("LOONFS_PROFILE")
            .env_remove("LOONFS_NAMESPACE")
            .env_remove("LOONFS_ACTOR_KIND")
            .env_remove("LOONFS_ACTOR_ID")
            .output()
            .expect("replay the printed command")
    }

    /// Runs the CLI with a payload on standard input.
    pub(super) fn run_with_stdin(&self, args: &[&str], stdin: &[u8]) -> Output {
        let mut child = self
            .command()
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn loonfs");
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(stdin)
            .expect("write stdin");
        child.wait_with_output().expect("run loonfs")
    }

    pub(super) fn store_root(&self, name: &str) -> PathBuf {
        self.temp_dir.path().join(format!("{name}-store"))
    }

    pub(super) fn add_embedded_profile(&self, name: &str) {
        let output = self.run(&[
            "--json",
            "profile",
            "create",
            "local",
            name,
            "--root",
            self.store_root(name).to_str().expect("utf-8 path"),
        ]);
        assert_success(&output);
    }

    pub(super) fn write_cli_config(&self, contents: impl AsRef<[u8]>) {
        fs::create_dir_all(self.config_path.parent().expect("config dir"))
            .expect("create config dir");
        fs::write(&self.config_path, contents).expect("write cli config");
    }

    pub(super) fn write_remote_listing_config(&self, server_url: &str) {
        self.write_cli_config(format!(
            r#"config_version = 1
default_profile = "remote"

[profiles.remote]
mode = "remote"
server_url = "{server_url}"
default_namespace = "demo"
auth_token = "test-token"
"#,
        ));
    }

    pub(super) fn write_server_config(&self, name: &str, key_prefix: &str) -> PathBuf {
        self.write_server_config_with(name, key_prefix, "")
    }

    /// A server config with `extra` appended, for tests that need a table
    /// the default deployment leaves out.
    pub(super) fn write_server_config_with(
        &self,
        name: &str,
        key_prefix: &str,
        extra: &str,
    ) -> PathBuf {
        let bind = format!("127.0.0.1:{}", available_port());
        let path = self
            .temp_dir
            .path()
            .join(format!("{name}.loonfs-server.toml"));
        let store_root = self.store_root(name);
        let contents = format!(
            r#"
bind = "{bind}"
auth_token = "test-token"
content_token_secret = "test-content-token-secret"
writer_id = "{name}"

[store]
kind = "local-fs"
root = "{}"
key_prefix = "{key_prefix}"
{extra}"#,
            store_root.display()
        );
        fs::write(&path, contents).expect("write server config");
        path
    }

    pub(super) fn start_external_server(&self, server_config_path: PathBuf) -> ExternalServer {
        for _ in 0..5 {
            let child = Command::new(loonfs_server_binary_path())
                .arg("--config")
                .arg(&server_config_path)
                .spawn()
                .expect("spawn loonfs-server");
            let server_url = server_url_from_config(&server_config_path);
            if wait_for_readiness(&server_url) {
                return ExternalServer { child, server_url };
            }

            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            rewrite_server_bind(&server_config_path, available_port());
        }

        panic!(
            "timed out waiting for external server from {}",
            server_config_path.display()
        );
    }
}

pub(super) fn json_response_server(responses: Vec<Value>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind JSON server");
    let address = listener.local_addr().expect("JSON server address");
    let server = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().expect("accept JSON request");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).expect("read JSON request");
                assert!(read > 0, "JSON request ended before its headers");
                request.extend_from_slice(&chunk[..read]);
            }

            let body = serde_json::to_vec(&response).expect("encode JSON response");
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .expect("write JSON response headers");
            stream.write_all(&body).expect("write JSON response");
        }
    });
    (format!("http://{address}"), server)
}

pub(super) struct ExternalServer {
    pub(super) child: Child,
    pub(super) server_url: String,
}

impl Drop for ExternalServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(super) fn loon_binary_path() -> PathBuf {
    if let Some(path) = env::var_os("CARGO_BIN_EXE_loonfs") {
        return PathBuf::from(path);
    }

    let current_exe = env::current_exe().expect("current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|path| path.parent())
        .expect("target debug dir");
    let candidate = debug_dir.join(if cfg!(windows) {
        "loonfs.exe"
    } else {
        "loonfs"
    });
    assert!(
        candidate.exists(),
        "expected loonfs binary at {}",
        candidate.display()
    );
    candidate
}

pub(super) fn loonfs_server_binary_path() -> PathBuf {
    if let Some(path) = env::var_os("CARGO_BIN_EXE_loonfs-server") {
        return PathBuf::from(path);
    }

    let current_exe = env::current_exe().expect("current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|path| path.parent())
        .expect("target debug dir");
    let candidate = debug_dir.join(if cfg!(windows) {
        "loonfs-server.exe"
    } else {
        "loonfs-server"
    });
    assert!(
        candidate.exists(),
        "expected loonfs-server binary at {}",
        candidate.display()
    );
    candidate
}

pub(super) fn server_url_from_config(path: &Path) -> String {
    let config = fs::read_to_string(path).expect("read server config");
    let bind = config
        .lines()
        .find_map(|line| line.trim().strip_prefix("bind = "))
        .expect("bind line")
        .trim_matches('"')
        .to_owned();
    format!("http://{bind}")
}

// Polls a spawned server binary for readiness; a wall-clock deadline is the
// point, so the timer methods the workspace otherwise disallows are scoped
// to this helper.
#[allow(clippy::disallowed_methods)]
pub(super) fn wait_for_readiness(server_url: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if raw_agent()
            .get(&format!("{server_url}/health"))
            .call()
            .is_ok()
        {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

pub(super) fn rewrite_server_bind(path: &Path, port: u16) {
    let config = fs::read_to_string(path).expect("read server config for bind rewrite");
    let bind = format!("127.0.0.1:{port}");
    let rewritten = config
        .lines()
        .map(|line| {
            if line.trim().starts_with("bind = ") {
                format!("bind = \"{bind}\"")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, rewritten).expect("rewrite server bind");
}

pub(super) fn available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr")
        .port()
}

pub(super) fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "expected success, got {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        stdout_string(output),
        stderr_string(output)
    );
}

pub(super) fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "expected failure, got success\nstdout:\n{}\nstderr:\n{}",
        stdout_string(output),
        stderr_string(output)
    );
}

pub(super) fn stdout_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The command a `rm` hint spells, taken from between its backticks.
pub(super) fn hinted_recovery_command(output: &Output) -> String {
    let text = stdout_string(output);
    let hint = text
        .split_once("recover with `")
        .and_then(|(_, rest)| rest.split_once('`'))
        .map(|(command, _)| command.to_owned());
    assert!(
        hint.is_some(),
        "expected a backtick-delimited recovery hint, got:\n{text}"
    );
    hint.expect("checked just above")
}

/// The `RECOVER` cell of the one trash row naming `display_name`.
pub(super) fn trash_recovery_command(output: &Output, display_name: &str) -> String {
    let table = stdout_string(output);
    let cell = table
        .lines()
        .find(|line| line.split('\t').nth(2) == Some(display_name))
        .and_then(|row| row.split('\t').nth(5))
        .map(ToOwned::to_owned);
    assert!(
        cell.is_some(),
        "expected a trash row for `{display_name}`, got:\n{table}"
    );
    cell.expect("checked just above")
}

pub(super) fn stderr_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

pub(super) fn parse_json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("parse json")
}

pub(super) fn json_data(output: &Output) -> Value {
    parse_json(&output.stdout)["data"].clone()
}

/// The failure envelope, which is the last document on standard error.
///
/// Under `--json` standard error carries a stream of JSON documents, not
/// one: a transfer reports progress there while it runs, and the envelope
/// comes last.
pub(super) fn json_error(output: &Output) -> Value {
    json_stderr_documents(output)
        .pop()
        .expect("a failure envelope on stderr")["error"]
        .clone()
}

/// Every JSON document standard error carried, in order.
pub(super) fn json_stderr_documents(output: &Output) -> Vec<Value> {
    serde_json::Deserializer::from_slice(&output.stderr)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse the json documents on stderr")
}

/// The progress events of one run, in the order they were reported.
pub(super) fn json_progress_events(output: &Output) -> Vec<Value> {
    json_stderr_documents(output)
        .into_iter()
        .filter(|document| document.get("error").is_none() && document.get("data").is_none())
        .collect()
}

pub(super) fn sorted_object_keys(value: &Value) -> Vec<String> {
    let mut keys: Vec<String> = value
        .as_object()
        .expect("json object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}
