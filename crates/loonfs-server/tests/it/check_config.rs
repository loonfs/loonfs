//! `loonfs-server` startup-check tests, driven through the real binary.
//!
//! The flags exist so an operator can validate a config before a deployment
//! starts, so these tests run the binary the operator runs.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Writes a server config that binds `bind` and stores under `store_root`.
fn write_config(dir: &Path, bind: &str, store_root: &Path) -> PathBuf {
    write_config_with(dir, bind, store_root, "")
}

/// [`write_config`] with `extra` written verbatim between the top-level
/// fields and the `[store]` table, for the optional tables a test needs.
fn write_config_with(dir: &Path, bind: &str, store_root: &Path, extra: &str) -> PathBuf {
    let path = dir.join("loonfs-server.toml");
    let contents = format!(
        r#"
bind = "{bind}"
auth_token = "check-config-token"
content_token_secret = "check-config-secret"
writer_id = "check-config-writer"
{extra}
[store]
kind = "local-fs"
root = "{}"
"#,
        store_root.display()
    );
    std::fs::write(&path, contents).expect("write server config");
    path
}

/// A `[tls]` table naming the two files.
fn tls_table(cert_path: &Path, key_path: &Path) -> String {
    format!(
        "\n[tls]\ncert_path = \"{}\"\nkey_path = \"{}\"\n",
        cert_path.display(),
        key_path.display()
    )
}

/// A `[local_cache]` table at `path`, sized at the smallest disk tier the
/// config accepts so the check allocates as little as it can.
fn local_cache_table(path: &Path) -> String {
    format!(
        "\n[local_cache]\npath = \"{}\"\nmemory_bytes = 4194304\ndisk_bytes = 100663296\n",
        path.display()
    )
}

/// Generates a self-signed identity in `dir` and returns its certificate and
/// key paths.
fn write_tls_identity(dir: &Path) -> (PathBuf, PathBuf) {
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate self-signed identity");
    std::fs::write(&cert_path, identity.cert.pem()).expect("write certificate");
    std::fs::write(&key_path, identity.signing_key.serialize_pem()).expect("write private key");
    (cert_path, key_path)
}

fn run_server(config_path: &Path, flags: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loonfs-server"));
    command.arg("--config").arg(config_path).args(flags);
    command.output().expect("run loonfs-server")
}

#[tokio::test]
async fn probe_store_reports_every_check_for_a_working_local_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_root = dir.path().join("store");
    let config_path = write_config(dir.path(), "127.0.0.1:9400", &store_root);

    let config = loonfs_server::load_server_config(&config_path).expect("load config");
    let report = loonfs_server::probe_store(&config)
        .await
        .expect("probe store through library boundary");
    let output = run_server(&config_path, &["--probe-store"]);

    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        output.status.success(),
        "expected success, got {:?}: {stdout}{stderr}",
        output.status
    );
    let lines: Vec<&str> = stdout.lines().collect();
    let expected: Vec<String> = report
        .checks
        .iter()
        .map(|check| check.check_line())
        .collect();
    assert_eq!(
        lines,
        expected.iter().map(String::as_str).collect::<Vec<_>>(),
        "{stdout}"
    );
    assert!(!stdout.contains("check-config-token"), "{stdout}");
    assert!(!stdout.contains("check-config-secret"), "{stdout}");
    assert!(!stderr.contains("check-config-token"), "{stderr}");
    assert!(!stderr.contains("check-config-secret"), "{stderr}");
}

#[test]
fn probe_store_fails_when_the_local_store_root_cannot_be_created() {
    let dir = tempfile::tempdir().expect("tempdir");
    let occupied = dir.path().join("store");
    std::fs::write(&occupied, b"a file, not a directory").expect("occupy the store path");
    let config_path = write_config(dir.path(), "127.0.0.1:9400", &occupied);

    let output = run_server(&config_path, &["--probe-store"]);

    assert!(!output.status.success(), "expected a non-zero exit");
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("store"), "{stderr}");
    assert!(!stdout.contains("check-config-token"), "{stdout}");
    assert!(!stdout.contains("check-config-secret"), "{stdout}");
    assert!(!stderr.contains("check-config-token"), "{stderr}");
    assert!(!stderr.contains("check-config-secret"), "{stderr}");
}

#[tokio::test]
async fn probe_store_and_check_config_may_be_combined() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(dir.path(), "127.0.0.1:9400", &dir.path().join("store"));

    let config = loonfs_server::load_server_config(&config_path).expect("load config");
    let report = loonfs_server::probe_store(&config)
        .await
        .expect("probe store through library boundary");
    let output = run_server(&config_path, &["--probe-store", "--check-config"]);

    assert!(
        output.status.success(),
        "combined checks failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], config.check_summary());
    assert_eq!(lines.len(), report.checks.len() + 1);
}

#[test]
fn check_config_accepts_a_valid_config_and_names_what_it_validated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_root = dir.path().join("store");
    let config_path = write_config(dir.path(), "127.0.0.1:9400", &store_root);

    let output = run_server(&config_path, &["--check-config"]);

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

    let output = run_server(&config_path, &["--check-config"]);

    assert!(!output.status.success(), "expected a non-zero exit");
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("bind"),
        "expected the config error to name the field, got: {stderr}"
    );
}

/// The check loads the TLS identity, so an identity a start could not use
/// fails the check too. Field validation passes both configs below: each one
/// names two non-empty paths, which is all the config file can say about
/// them.
#[test]
fn check_config_reports_a_tls_identity_it_cannot_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_root = dir.path().join("store");
    let (cert_path, key_path) = write_tls_identity(dir.path());

    // A certificate path that names no file.
    let missing_cert = dir.path().join("absent.crt");
    let config_path = write_config_with(
        dir.path(),
        "127.0.0.1:9400",
        &store_root,
        &tls_table(&missing_cert, &key_path),
    );

    let output = run_server(&config_path, &["--check-config"]);

    assert!(
        !output.status.success(),
        "a certificate file that is not there must fail the check"
    );
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("absent.crt"),
        "the error must name the file it could not read, got: {stderr}"
    );

    // A key file that is there and is not the PEM it is configured as.
    let not_pem = dir.path().join("not-pem.key");
    std::fs::write(&not_pem, b"this file holds no private key\n")
        .expect("write a key that is not PEM");
    let config_path = write_config_with(
        dir.path(),
        "127.0.0.1:9400",
        &store_root,
        &tls_table(&cert_path, &not_pem),
    );

    let output = run_server(&config_path, &["--check-config"]);

    assert!(
        !output.status.success(),
        "a key file that is not PEM must fail the check"
    );
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("not-pem.key"),
        "the error must name the file it could not parse, got: {stderr}"
    );
}

/// The check opens the local block cache, so a directory a start could not
/// own fails the check too. A plain file where the directory belongs is one
/// way to be unable to own it, and it fails whichever user runs the test.
#[test]
fn check_config_reports_a_local_cache_it_cannot_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let occupied = dir.path().join("cache");
    std::fs::write(&occupied, b"a file, not a directory").expect("occupy the cache path");
    let config_path = write_config_with(
        dir.path(),
        "127.0.0.1:9400",
        &dir.path().join("store"),
        &local_cache_table(&occupied),
    );

    let output = run_server(&config_path, &["--check-config"]);

    assert!(
        !output.status.success(),
        "a cache directory the server cannot open must fail the check"
    );
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("local_cache.path"),
        "the error must name the field it came from, got: {stderr}"
    );
    assert!(
        stderr.contains("cache"),
        "the error must name the directory, got: {stderr}"
    );
}

/// A configured identity and a configured cache both pass, and the check
/// leaves the cache directory the way the start that follows it needs to
/// find it.
///
/// The second run is what proves the release: it calls the same open a start
/// calls, so a lock the first run still held would fail it.
#[test]
fn check_config_opens_the_local_cache_and_leaves_it_openable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_root = dir.path().join("store");
    let (cert_path, key_path) = write_tls_identity(dir.path());
    let cache_root = dir.path().join("cache");
    let config_path = write_config_with(
        dir.path(),
        "127.0.0.1:9400",
        &store_root,
        &format!(
            "{}{}",
            tls_table(&cert_path, &key_path),
            local_cache_table(&cache_root)
        ),
    );

    let output = run_server(&config_path, &["--check-config"]);

    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(
        output.status.success(),
        "expected success, got {:?}: {stdout}{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        stdout.trim(),
        "config ok: bind 127.0.0.1:9400, store local-fs"
    );
    assert!(
        cache_root.is_dir(),
        "the check opens the cache, and opening it builds the directory"
    );

    let second = run_server(&config_path, &["--check-config"]);

    assert!(
        second.status.success(),
        "the checked directory must be one the next open can take: {}",
        String::from_utf8_lossy(&second.stderr)
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

    let output = run_server(&config_path, &["--check-config"]);

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
