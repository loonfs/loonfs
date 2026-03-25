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
fn help_ops_mentions_bootstrap_namespace() {
    let output = run_loon(["help", "ops"]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("bootstrap-namespace"));
    assert!(stdout.contains("Use `bootstrap-namespace`"));
}

#[test]
fn help_bootstrap_namespace_shows_required_flags() {
    let output = run_loon(["help", "ops", "bootstrap-namespace"]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("--config"));
    assert!(stdout.contains("--namespace"));
    assert!(stdout.contains("--allow-existing"));
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
    let bootstrap_manpage = fs::read_to_string(output_dir.join("loon-ops-bootstrap-namespace.1"))
        .expect("read bootstrap manpage");
    assert!(bootstrap_manpage.contains("bootstrap-namespace"));
}

#[test]
fn config_path_uses_local_default_resolution() {
    let temp_dir = TestDir::new("loon-cli-config-path");
    write_demo_local_fs_config_with_name(temp_dir.path(), "loondb-demo.local.toml");
    let output = run_loon_in_dir(temp_dir.path(), ["config", "path"]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        "loondb-demo.local.toml\n"
    );
}

#[test]
fn config_path_prefers_loon_config_env() {
    let temp_dir = TestDir::new("loon-cli-config-env");
    let env_config_path = write_demo_local_fs_config_with_name(temp_dir.path(), "env-demo.toml");
    write_demo_local_fs_config_with_name(temp_dir.path(), "loondb-demo.local.toml");
    let output = run_loon_in_dir_with_env(
        temp_dir.path(),
        [("LOON_CONFIG", env_config_path.to_str().expect("utf-8 path"))],
        ["config", "path"],
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        format!("{}\n", env_config_path.display())
    );
}

#[test]
fn config_path_missing_points_at_example_template() {
    let temp_dir = TestDir::new("loon-cli-config-missing");
    let output = run_loon_in_dir(temp_dir.path(), ["config", "path"]);
    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8(output.stderr)
        .expect("utf-8 stderr")
        .contains("configs/loondb-demo.local-fs.example.toml"));
}

#[test]
fn config_show_prints_normalized_toml() {
    let temp_dir = TestDir::new("loon-cli-config-show");
    let config_path = write_demo_local_fs_config(temp_dir.path());
    let output = run_loon([
        "config",
        "show",
        "--config",
        config_path.to_str().expect("utf-8 path"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("[object_store]"));
    assert!(stdout.contains("kind = \"local-fs\""));
    assert!(stdout.contains("[ops]"));
}

#[test]
fn config_validate_reports_valid_status() {
    let temp_dir = TestDir::new("loon-cli-config-validate");
    let config_path = write_demo_local_fs_config(temp_dir.path());
    let output = run_loon([
        "config",
        "validate",
        "--config",
        config_path.to_str().expect("utf-8 path"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("command=config/validate"));
    assert!(stdout.contains("status=valid"));
}

#[test]
fn config_validate_invalid_config_fails_cleanly() {
    let temp_dir = TestDir::new("loon-cli-config-invalid");
    let config_path = temp_dir.path().join("broken.toml");
    fs::write(&config_path, "[object_store\nkind = \"local-fs\"\n").expect("write invalid config");
    let output = run_loon([
        "config",
        "validate",
        "--config",
        config_path.to_str().expect("utf-8 path"),
    ]);
    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8(output.stderr)
        .expect("utf-8 stderr")
        .contains("parse ops config"));
}

#[test]
fn doctor_reports_success_for_valid_local_fs_config() {
    let temp_dir = TestDir::new("loon-cli-doctor-valid");
    let config_path = write_demo_local_fs_config(temp_dir.path());
    let output = run_loon([
        "doctor",
        "--config",
        config_path.to_str().expect("utf-8 path"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("command=doctor"));
    assert!(stdout.contains("object_store_kind=local-fs"));
    assert!(stdout.contains("client_state_db=missing"));
    assert!(stdout.contains("status=ok"));
}

#[test]
fn doctor_invalid_config_fails_cleanly() {
    let temp_dir = TestDir::new("loon-cli-doctor-invalid");
    let config_path = temp_dir.path().join("broken.toml");
    fs::write(&config_path, "[object_store\nkind = \"local-fs\"\n").expect("write invalid config");
    let output = run_loon([
        "doctor",
        "--config",
        config_path.to_str().expect("utf-8 path"),
    ]);
    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("command=doctor"));
    assert!(stderr.contains("config_parse=error"));
    assert!(stderr.contains("status=failed"));
}

#[test]
fn doctor_reports_missing_paths_cleanly() {
    let temp_dir = TestDir::new("loon-cli-doctor-missing-paths");
    let config_path = write_demo_local_fs_config_with_paths(
        temp_dir.path(),
        temp_dir.path().join("object-store"),
        temp_dir.path().join("missing-parent/client.sqlite3"),
        temp_dir.path().join("missing-mirror"),
        "loondb-demo.toml",
        false,
    );
    let output = run_loon([
        "doctor",
        "--config",
        config_path.to_str().expect("utf-8 path"),
    ]);
    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("client_state_db_parent=missing"));
    assert!(stderr.contains("mirror_root=missing"));
    assert!(stderr.contains("status=failed"));
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
    run_loon_command(Command::new(env!("CARGO_BIN_EXE_loon")).args(args))
}

fn run_loon_in_dir<I, S>(dir: &Path, args: I) -> std::process::Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    run_loon_command(
        Command::new(env!("CARGO_BIN_EXE_loon"))
            .current_dir(dir)
            .args(args),
    )
}

fn run_loon_in_dir_with_env<I, S, K, V>(
    dir: &Path,
    envs: impl IntoIterator<Item = (K, V)>,
    args: I,
) -> std::process::Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
    K: AsRef<std::ffi::OsStr>,
    V: AsRef<std::ffi::OsStr>,
{
    let mut command = Command::new(env!("CARGO_BIN_EXE_loon"));
    command.current_dir(dir).args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    run_loon_command(&mut command)
}

fn run_loon_command(command: &mut Command) -> std::process::Output {
    command.output().expect("run loon cli")
}

fn write_demo_local_fs_config(base_dir: &Path) -> PathBuf {
    write_demo_local_fs_config_with_name(base_dir, "loondb-demo.toml")
}

fn write_demo_local_fs_config_with_name(base_dir: &Path, name: &str) -> PathBuf {
    write_demo_local_fs_config_with_paths(
        base_dir,
        base_dir.join("object-store"),
        base_dir.join("client.sqlite3"),
        base_dir.join("mirror"),
        name,
        true,
    )
}

fn write_demo_local_fs_config_with_paths(
    base_dir: &Path,
    object_store_root: PathBuf,
    state_db_path: PathBuf,
    mirror_root: PathBuf,
    name: &str,
    create_mirror_root: bool,
) -> PathBuf {
    let config_path = base_dir.join(name);
    fs::create_dir_all(&object_store_root).expect("create object store root");
    if create_mirror_root {
        fs::create_dir_all(&mirror_root).expect("create mirror root");
    }
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
