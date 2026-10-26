//! The processes that a mount test runs: a server with a local store, the
//! CLI (as root, through `sudo -n` when the test isn't root), and the daemon
//! that the CLI starts.

use loonfs_client::{Client, ClientConfig};
use loonfs_types::SecretString;
use std::ffi::OsStr;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const AUTH_TOKEN: &str = "test-token";

/// How long a test waits for a condition that the mount reaches by itself.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// A server, a CLI configuration, and the directories of one mount test.
pub(crate) struct Harness {
    pub(crate) directory: TempDir,
    server: Child,
    server_url: String,
    sudo: bool,
}

impl Harness {
    /// Starts a server with a local store, and writes a CLI profile for it.
    pub(crate) fn start() -> Self {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let root = directory.path();
        for name in ["home/.config/loonfs", "mnt", "store"] {
            std::fs::create_dir_all(root.join(name)).expect("a test directory");
        }
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        let server_url = format!("http://127.0.0.1:{port}");
        std::fs::write(
            root.join("server.toml"),
            format!(
                "bind = \"127.0.0.1:{port}\"\nauth_token = \"{AUTH_TOKEN}\"\n\
                 content_token_secret = \"test-content-token-secret\"\nwriter_id = \"mount-test\"\n\n\
                 [store]\nkind = \"local_fs\"\nroot = \"{}\"\nkey_prefix = \"\"\n",
                root.join("store").display()
            ),
        )
        .expect("the server's config");
        std::fs::write(
            root.join("home/.config/loonfs/config.toml"),
            format!(
                "config_version = 1\ndefault_profile = \"remote\"\n\n[profiles.remote]\n\
                 mode = \"remote\"\nserver_url = \"{server_url}\"\nauth_token = \"{AUTH_TOKEN}\"\n"
            ),
        )
        .expect("the CLI's config");
        let server = Command::new(binary("loonfs-server"))
            .arg("--config")
            .arg(root.join("server.toml"))
            .stdout(std::fs::File::create(root.join("server.log")).expect("the server's log"))
            .stderr(std::fs::File::create(root.join("server.err")).expect("the server's log"))
            .spawn()
            .expect("the server starts");
        let harness = Self {
            directory,
            server,
            server_url,
            sudo: !is_root(),
        };
        let client = harness.client();
        let runtime = runtime();
        eventually("the server is healthy", || {
            runtime.block_on(client.get_health()).ok()
        });
        harness
    }

    /// Returns a client of the server.
    pub(crate) fn client(&self) -> Client {
        Client::new(ClientConfig {
            server_url: self.server_url.clone(),
            auth_token: Some(SecretString::new(AUTH_TOKEN)),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("a client")
    }

    /// Returns the directory that the test mounts on.
    pub(crate) fn mount_directory(&self) -> PathBuf {
        self.directory.path().join("mnt")
    }

    /// Returns the daemon's state directory.
    pub(crate) fn state_directory(&self) -> PathBuf {
        self.directory.path().join("state")
    }

    /// Runs the CLI as root, with the test's configuration and the test's
    /// state directory.
    pub(crate) fn loonfs<I, S>(&self, arguments: I) -> Output
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let home = self.directory.path().join("home");
        let mut command = if self.sudo {
            let mut command = Command::new("sudo");
            command.arg("-n").arg("env");
            command.arg(format!("HOME={}", home.display()));
            command.arg(binary("loonfs"));
            command
        } else {
            let mut command = Command::new(binary("loonfs"));
            command.env("HOME", &home);
            command
        };
        command
            .env_remove("XDG_RUNTIME_DIR")
            .env_remove("XDG_STATE_HOME")
            .env_remove("LOONFS_PROFILE")
            .env_remove("LOONFS_AUTH_TOKEN")
            .args(arguments)
            .arg("--state-dir")
            .arg(self.state_directory())
            .output()
            .expect("the CLI runs")
    }

    /// Runs the CLI and checks that it succeeded.
    pub(crate) fn loonfs_ok<I, S>(&self, arguments: I) -> String
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.loonfs(arguments);
        assert!(
            output.status.success(),
            "the CLI failed with {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("UTF-8 output")
    }

    /// Returns whether the kernel shows a mount at the test's directory.
    pub(crate) fn is_mounted(&self) -> bool {
        let table = std::fs::read_to_string("/proc/self/mountinfo").expect("the mount table");
        let directory = self.mount_directory();
        let directory = directory.to_str().expect("a UTF-8 directory");
        table
            .lines()
            .any(|line| line.split(' ').nth(4) == Some(directory))
    }
}

impl Drop for Harness {
    /// Leaves no mount, daemon, or server behind, whether or not the test
    /// passed.
    fn drop(&mut self) {
        if self.is_mounted() {
            let directory = self.mount_directory();
            let _ = self.loonfs([
                OsStr::new("unmount"),
                OsStr::new("--force"),
                directory.as_os_str(),
            ]);
        }
        let _ = self.loonfs(["daemon", "--stop"]);
        let _ = self.server.kill();
        let _ = self.server.wait();
        if self.sudo {
            // The daemon ran as root, so its state directory is root's.
            let _ = Command::new("sudo")
                .args(["-n", "rm", "-rf"])
                .arg(self.state_directory())
                .status();
        }
    }
}

/// Returns a runtime for the test's client calls.
pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

/// Returns the path of a binary that `cargo build` wrote next to the test
/// binary's directory.
fn binary(name: &str) -> PathBuf {
    let test_binary = std::env::current_exe().expect("the test binary");
    let path = test_binary
        .parent()
        .and_then(Path::parent)
        .expect("the target directory")
        .join(name);
    assert!(
        path.exists(),
        "expected `{}`; run `cargo build -p loonfs-cli -p loonfs-server` first",
        path.display()
    );
    path
}

fn is_root() -> bool {
    let output = Command::new("id").arg("-u").output().expect("`id -u` runs");
    String::from_utf8_lossy(&output.stdout).trim() == "0"
}

/// Calls `check` until it returns a value, and fails the test after
/// [`WAIT`].
#[allow(
    clippy::disallowed_methods,
    reason = "the test waits on the wall clock for a mount that follows its feed"
)]
pub(crate) fn eventually<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}
