use loon_ops::OpsCommand;
use loon_testkit::tempdir::TestDir;
use loon_types::NamespaceId;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
fn version_subcommand_prints_version() {
    let output = run_loon(["version"]);
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stderr).expect("utf-8 stderr"), "");
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        format!("loon {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn completion_generation_mentions_active_ops_subcommands() {
    let output = run_loon(["completion", "bash"]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("bootstrap-namespace"));
    assert!(stdout.contains("sync-until-idle"));
}

#[test]
fn manpages_generation_writes_root_and_ops_pages() {
    let temp_dir = TestDir::new("loon-cli-manpages");
    let output_dir = temp_dir.path().join("man");
    let output = run_loon(["manpages", output_dir.to_str().expect("utf-8 path")]);
    assert!(output.status.success());
    assert!(output_dir.join("loon.1").is_file());
    assert!(output_dir.join("loon-ops.1").is_file());
    assert!(output_dir.join("loon-ops-smoke.1").is_file());
}

#[test]
fn ops_smoke_stdout_matches_loon_ops_exactly() {
    let cli_temp_dir = TestDir::new("loon-cli-smoke-cli");
    let ops_temp_dir = TestDir::new("loon-cli-smoke-ops");
    let cli_config_path = write_demo_local_fs_config(cli_temp_dir.path());
    let ops_config_path = write_demo_local_fs_config(ops_temp_dir.path());
    let namespace_id = NamespaceId::from("demo");

    let output = run_loon([
        "ops",
        "smoke",
        "--config",
        cli_config_path.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        loon_ops::run_command(OpsCommand::Smoke {
            config_path: ops_config_path,
            namespace_id: namespace_id.clone(),
        })
        .expect("run loon_ops smoke")
    );
}

#[test]
fn bootstrap_namespace_stdout_matches_loon_ops_exactly() {
    let cli_temp_dir = TestDir::new("loon-cli-bootstrap-cli");
    let ops_temp_dir = TestDir::new("loon-cli-bootstrap-ops");
    let cli_config_path = write_demo_local_fs_config(cli_temp_dir.path());
    let ops_config_path = write_demo_local_fs_config(ops_temp_dir.path());
    let namespace_id = NamespaceId::from("demo");

    let output = run_loon([
        "ops",
        "bootstrap-namespace",
        "--config",
        cli_config_path.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        loon_ops::run_command(OpsCommand::BootstrapNamespace {
            config_path: ops_config_path,
            namespace_id,
            allow_existing: false,
        })
        .expect("run loon_ops bootstrap")
    );
}

#[test]
fn parse_failure_writes_to_stderr_and_exits_non_zero() {
    let output = run_loon([
        "ops",
        "observe-local",
        "--config",
        "demo.toml",
        "--namespace",
        "demo",
    ]);
    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8(output.stderr)
        .expect("utf-8 stderr")
        .contains("--path"));
}

#[test]
fn runtime_failure_writes_to_stderr_and_exits_non_zero() {
    let output = run_loon([
        "ops",
        "show-client-state",
        "--config",
        "missing.toml",
        "--namespace",
        "demo",
    ]);
    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8(output.stderr)
        .expect("utf-8 stderr")
        .contains("read ops config missing.toml"));
}

fn run_loon<I, S>(args: I) -> std::process::Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    Command::new(env!("CARGO_BIN_EXE_loon"))
        .args(args)
        .output()
        .expect("run loon cli")
}

fn write_demo_local_fs_config(base_dir: &Path) -> PathBuf {
    let object_store_root = base_dir.join("object-store");
    let mirror_root = base_dir.join("mirror");
    let state_db_path = base_dir.join("client.sqlite3");
    let config_path = base_dir.join("loondb-demo.toml");
    fs::create_dir_all(&object_store_root).expect("create object store root");
    fs::create_dir_all(&mirror_root).expect("create mirror root");
    fs::write(
        &config_path,
        format!(
            r#"[object_store]
kind = "local-fs"
root = "{}"

[client]
state_db_path = "{}"
mirror_root = "{}"

[server]
writer_id = "loon-cli-test"
writer_version = "v1"
lease_duration_ms = 60000

[ops]
now_ms = 1700000000000
"#,
            object_store_root.display(),
            state_db_path.display(),
            mirror_root.display(),
        ),
    )
    .expect("write ops config");
    config_path
}
