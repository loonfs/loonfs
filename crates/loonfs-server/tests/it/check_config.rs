//! `loonfs-server --check-config` tests, driven through the real binary.
//!
//! The flag exists so an operator can validate a config before a deployment
//! starts, so these tests run the binary the operator runs.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Writes a server config that binds `bind` and stores under `store_root`.
fn write_config(dir: &Path, bind: &str, store_root: &Path) -> PathBuf {
    let path = dir.join("loonfs-server.toml");
    let contents = format!(
        r#"
bind = "{bind}"
auth_token = "check-config-token"
content_token_secret = "check-config-secret"
writer_id = "check-config-writer"

[store]
kind = "local-fs"
root = "{}"
"#,
        store_root.display()
    );
    std::fs::write(&path, contents).expect("write server config");
    path
}

fn check_config(config_path: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_loonfs-server"))
        .arg("--config")
        .arg(config_path)
        .arg("--check-config")
        .output()
        .expect("run loonfs-server --check-config")
}

#[test]
fn check_config_accepts_a_valid_config_and_names_what_it_validated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_root = dir.path().join("store");
    let config_path = write_config(dir.path(), "127.0.0.1:9400", &store_root);

    let output = check_config(&config_path);

    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(
        output.status.success(),
        "expected success, got {:?}: {stdout}",
        output.status
    );
    assert_eq!(
        stdout.trim(),
        "config ok: bind 127.0.0.1:9400, store local-fs"
    );
    // The documented caveat: constructing a local-fs store creates its root.
    assert!(store_root.is_dir(), "local-fs check creates the store root");
}

#[test]
fn check_config_reports_an_invalid_config_and_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("loonfs-server.toml");
    std::fs::write(
        &config_path,
        r#"
bind = "not-an-address"
auth_token = "check-config-token"
content_token_secret = "check-config-secret"
writer_id = "check-config-writer"

[store]
kind = "local-fs"
root = "/tmp/loonfs-check-config-unused"
"#,
    )
    .expect("write server config");

    let output = check_config(&config_path);

    assert!(!output.status.success(), "expected a non-zero exit");
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("bind"),
        "expected the config error to name the field, got: {stderr}"
    );
}

/// Holding the configured port during the check proves the check never binds
/// it: a server that bound would fail with the address already in use.
#[test]
fn check_config_does_not_bind_the_port() {
    let dir = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let bind = listener.local_addr().expect("local addr").to_string();
    let config_path = write_config(dir.path(), &bind, &dir.path().join("store"));

    let output = check_config(&config_path);

    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        output.status.success(),
        "the check must succeed while the port is taken: {stdout}{stderr}"
    );
    assert_eq!(
        stdout.trim(),
        format!("config ok: bind {bind}, store local-fs")
    );
    drop(listener);
}
