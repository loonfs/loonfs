use loon_objectstore::ObjectStore;
use loon_ops::OpsCommand;
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_server::ops::{bootstrap_namespace, NamespaceBootstrapParams};
use loon_testkit::tempdir::TestDir;
use loon_types::{
    sha256_digest, ClientMutationOp, ClientMutationRequest, ContentBlockDescriptor,
    ContentManifestEnvelope, ContentManifestPayload, NamespaceId,
};
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
fn help_file_mentions_authoritative_subcommands() {
    let output = run_loon(["help", "file"]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("ls"));
    assert!(stdout.contains("stat"));
    assert!(stdout.contains("get"));
    assert!(stdout.contains("cat"));
    assert!(stdout.contains("put"));
    assert!(stdout.contains("cp"));
    assert!(stdout.contains("mkdir"));
    assert!(stdout.contains("rm"));
    assert!(stdout.contains("mv"));
}

#[test]
fn file_ls_and_cat_read_authoritative_state_directly() {
    let temp_dir = TestDir::new("loon-cli-file-read");
    let config_path = write_demo_local_fs_config(temp_dir.path());
    let namespace_id = NamespaceId::from("demo");
    seed_authoritative_hello_file(&config_path, &namespace_id, b"hello from loon\n");

    let ls = run_loon([
        "file",
        "ls",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/",
    ]);
    assert!(ls.status.success());
    assert_eq!(
        String::from_utf8(ls.stdout).expect("utf-8 stdout"),
        "hello.txt\n"
    );

    let cat = run_loon([
        "file",
        "cat",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/hello.txt",
    ]);
    assert!(cat.status.success());
    assert_eq!(cat.stdout, b"hello from loon\n");
    assert!(cat.stderr.is_empty());
}

#[test]
fn file_get_writes_download_without_touching_existing_target() {
    let temp_dir = TestDir::new("loon-cli-file-get");
    let config_path = write_demo_local_fs_config(temp_dir.path());
    let namespace_id = NamespaceId::from("demo");
    seed_authoritative_hello_file(&config_path, &namespace_id, b"hello from loon\n");
    let download_dir = temp_dir.path().join("downloads");
    fs::create_dir_all(&download_dir).expect("create downloads dir");

    let get = run_loon([
        "file",
        "get",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/hello.txt",
        download_dir.to_str().expect("utf-8 path"),
    ]);
    assert!(get.status.success());
    assert_eq!(
        fs::read(download_dir.join("hello.txt")).expect("read downloaded file"),
        b"hello from loon\n"
    );

    let second_get = run_loon([
        "file",
        "get",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/hello.txt",
        download_dir.to_str().expect("utf-8 path"),
    ]);
    assert!(!second_get.status.success());
    assert!(String::from_utf8(second_get.stderr)
        .expect("utf-8 stderr")
        .contains("already exists"));
}

#[test]
fn file_put_replace_cp_mkdir_rm_and_mv_run_as_authoritative_commands() {
    let temp_dir = TestDir::new("loon-cli-file-write");
    let config_path = write_demo_local_fs_config(temp_dir.path());
    let namespace_id = NamespaceId::from("demo");
    bootstrap_empty_namespace(&config_path, &namespace_id);

    let mkdir = run_loon([
        "file",
        "mkdir",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/docs",
    ]);
    assert!(mkdir.status.success());
    assert!(String::from_utf8(mkdir.stdout)
        .expect("utf-8 stdout")
        .contains("absolute_path: /docs"));

    let local_file = temp_dir.path().join("hello.txt");
    fs::write(&local_file, b"hello write path\n").expect("write local source");
    let put = run_loon([
        "file",
        "put",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        local_file.to_str().expect("utf-8 path"),
        "demo:/docs/hello.txt",
    ]);
    assert!(put.status.success());
    let put_stdout = String::from_utf8(put.stdout).expect("utf-8 stdout");
    assert!(put_stdout.contains("destination: demo:/docs/hello.txt"));
    assert!(put_stdout.contains("replace: false"));

    let replacement_file = temp_dir.path().join("hello-v2.txt");
    fs::write(&replacement_file, b"hello replaced path\n").expect("write replacement source");
    let replace = run_loon([
        "file",
        "put",
        "--replace",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        replacement_file.to_str().expect("utf-8 path"),
        "demo:/docs/hello.txt",
    ]);
    assert!(replace.status.success());
    let replace_stdout = String::from_utf8(replace.stdout).expect("utf-8 stdout");
    assert!(replace_stdout.contains("replace: true"));
    assert!(replace_stdout.contains("destination: demo:/docs/hello.txt"));
    assert!(replace_stdout.contains("revision_no: 2"));

    let cp = run_loon([
        "file",
        "cp",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/docs/hello.txt",
        "demo:/docs/hello-copy.txt",
    ]);
    assert!(cp.status.success());
    let cp_stdout = String::from_utf8(cp.stdout).expect("utf-8 stdout");
    assert!(cp_stdout.contains("from: demo:/docs/hello.txt"));
    assert!(cp_stdout.contains("to: demo:/docs/hello-copy.txt"));
    assert!(cp_stdout.contains("absolute_path: /docs/hello-copy.txt"));

    let mv = run_loon([
        "file",
        "mv",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/docs/hello.txt",
        "demo:/docs/archive.txt",
    ]);
    assert!(mv.status.success());
    assert!(String::from_utf8(mv.stdout)
        .expect("utf-8 stdout")
        .contains("to: demo:/docs/archive.txt"));

    let ls = run_loon([
        "file",
        "ls",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/docs",
    ]);
    assert!(ls.status.success());
    assert_eq!(
        String::from_utf8(ls.stdout).expect("utf-8 stdout"),
        "archive.txt\nhello-copy.txt\n"
    );

    let rm = run_loon([
        "file",
        "rm",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "demo:/docs/archive.txt",
    ]);
    assert!(rm.status.success());
    assert!(String::from_utf8(rm.stdout)
        .expect("utf-8 stdout")
        .contains("deleted_path: /docs/archive.txt"));
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
fn import_then_sync_once_materializes_root_directory() {
    let temp_dir = TestDir::new("loon-cli-root-materialization");
    let config_path = write_demo_local_fs_config(temp_dir.path());
    let namespace_id = NamespaceId::from("demo");

    let bootstrap = run_loon([
        "ops",
        "bootstrap-namespace",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(bootstrap.status.success());

    let import = run_loon([
        "ops",
        "import-remote-observations",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(import.status.success());

    let imported_state = run_loon([
        "ops",
        "show-client-state",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(imported_state.status.success());
    let imported_stdout = String::from_utf8(imported_state.stdout).expect("utf-8 stdout");
    assert!(imported_stdout.contains("decision: materialize_remote_dir"));
    assert!(imported_stdout.contains("exists_on_disk: false"));

    let sync = run_loon([
        "ops",
        "sync-once",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(sync.status.success());

    let final_state = run_loon([
        "ops",
        "show-client-state",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(final_state.status.success());
    let final_stdout = String::from_utf8(final_state.stdout).expect("utf-8 stdout");
    assert!(final_stdout.contains("exists_on_disk: true"));
    assert!(final_stdout.contains("synced_seq: 0"));
    assert!(final_stdout.contains("planned_actions: []"));
}

#[test]
fn expired_same_writer_lease_is_reacquired_on_sync_once() {
    let temp_dir = TestDir::new("loon-cli-expired-same-writer");
    let object_store_root = temp_dir.path().join("object-store");
    let state_db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    let bootstrap_config = write_demo_local_fs_config_with_spec(
        temp_dir.path(),
        object_store_root.clone(),
        state_db_path.clone(),
        mirror_root.clone(),
        "bootstrap.toml",
        true,
        "writer-a",
        1_000,
    );
    let run_config = write_demo_local_fs_config_with_spec(
        temp_dir.path(),
        object_store_root,
        state_db_path,
        mirror_root.clone(),
        "run.toml",
        true,
        "writer-a",
        70_000,
    );
    let namespace_id = NamespaceId::from("demo");

    assert!(run_loon([
        "ops",
        "bootstrap-namespace",
        "--config",
        bootstrap_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());
    assert!(run_loon([
        "ops",
        "import-remote-observations",
        "--config",
        bootstrap_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());
    assert!(run_loon([
        "ops",
        "sync-once",
        "--config",
        bootstrap_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());

    fs::write(mirror_root.join("hello.txt"), b"hello from writer a\n").expect("write local file");
    assert!(run_loon([
        "ops",
        "observe-local",
        "--config",
        run_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
        "--path",
        mirror_root.join("hello.txt").to_str().expect("utf-8 path"),
    ])
    .status
    .success());

    let sync = run_loon([
        "ops",
        "sync-once",
        "--config",
        run_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(sync.status.success());

    let namespace_state = run_loon([
        "ops",
        "show-namespace-state",
        "--config",
        run_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(namespace_state.status.success());
    let stdout = String::from_utf8(namespace_state.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("active_fence_token: 1"));
    assert!(stdout.contains("holder_id: writer-a"));
    assert!(stdout.contains("lease_expires_at_ms: 130000"));
}

#[test]
fn writer_b_can_take_over_after_expiry_and_writer_a_is_then_rejected() {
    let temp_dir = TestDir::new("loon-cli-writer-takeover");
    let object_store_root = temp_dir.path().join("object-store");
    let writer_a_db = temp_dir.path().join("writer-a.sqlite3");
    let writer_a_mirror = temp_dir.path().join("mirror-a");
    let writer_b_db = temp_dir.path().join("writer-b.sqlite3");
    let writer_b_mirror = temp_dir.path().join("mirror-b");
    let bootstrap_config = write_demo_local_fs_config_with_spec(
        temp_dir.path(),
        object_store_root.clone(),
        writer_a_db.clone(),
        writer_a_mirror.clone(),
        "bootstrap-a.toml",
        true,
        "writer-a",
        1_000,
    );
    let writer_a_run = write_demo_local_fs_config_with_spec(
        temp_dir.path(),
        object_store_root.clone(),
        writer_a_db,
        writer_a_mirror.clone(),
        "run-a.toml",
        true,
        "writer-a",
        70_000,
    );
    let writer_b_run = write_demo_local_fs_config_with_spec(
        temp_dir.path(),
        object_store_root,
        writer_b_db,
        writer_b_mirror.clone(),
        "run-b.toml",
        true,
        "writer-b",
        70_000,
    );
    let namespace_id = NamespaceId::from("demo");

    assert!(run_loon([
        "ops",
        "bootstrap-namespace",
        "--config",
        bootstrap_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());
    assert!(run_loon([
        "ops",
        "import-remote-observations",
        "--config",
        bootstrap_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());
    assert!(run_loon([
        "ops",
        "sync-once",
        "--config",
        bootstrap_config.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());

    assert!(run_loon([
        "ops",
        "import-remote-observations",
        "--config",
        writer_b_run.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());
    assert!(run_loon([
        "ops",
        "sync-once",
        "--config",
        writer_b_run.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());

    fs::write(writer_b_mirror.join("hello.txt"), b"writer b takeover\n").expect("write b file");
    assert!(run_loon([
        "ops",
        "observe-local",
        "--config",
        writer_b_run.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
        "--path",
        writer_b_mirror
            .join("hello.txt")
            .to_str()
            .expect("utf-8 path"),
    ])
    .status
    .success());
    assert!(run_loon([
        "ops",
        "sync-once",
        "--config",
        writer_b_run.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ])
    .status
    .success());

    fs::write(writer_a_mirror.join("stale.txt"), b"writer a stale\n").expect("write a file");
    assert!(run_loon([
        "ops",
        "observe-local",
        "--config",
        writer_a_run.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
        "--path",
        writer_a_mirror
            .join("stale.txt")
            .to_str()
            .expect("utf-8 path"),
    ])
    .status
    .success());

    let stale_sync = run_loon([
        "ops",
        "sync-once",
        "--config",
        writer_a_run.to_str().expect("utf-8 path"),
        "--namespace",
        namespace_id.as_str(),
    ]);
    assert!(!stale_sync.status.success());
    let stderr = String::from_utf8(stale_sync.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("writer-b"));
    assert!(stderr.contains("held by another active writer"));
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
    write_demo_local_fs_config_with_spec(
        base_dir,
        base_dir.join("object-store"),
        base_dir.join("client.sqlite3"),
        base_dir.join("mirror"),
        name,
        true,
        "loon-cli-test",
        1_700_000_000_000,
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
    write_demo_local_fs_config_with_spec(
        base_dir,
        object_store_root,
        state_db_path,
        mirror_root,
        name,
        create_mirror_root,
        "loon-cli-test",
        1_700_000_000_000,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_demo_local_fs_config_with_spec(
    base_dir: &Path,
    object_store_root: PathBuf,
    state_db_path: PathBuf,
    mirror_root: PathBuf,
    name: &str,
    create_mirror_root: bool,
    writer_id: &str,
    now_ms: u64,
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
writer_id = "{writer_id}"
writer_version = "v1"
lease_duration_ms = 60000

[ops]
now_ms = {now_ms}
"#,
            object_store_root.display(),
            state_db_path.display(),
            mirror_root.display(),
        ),
    )
    .expect("write ops config");
    config_path
}

fn seed_authoritative_hello_file(config_path: &Path, namespace_id: &NamespaceId, bytes: &[u8]) {
    let config = loon_ops::OpsConfig::load(config_path).expect("load config");
    let store = config.open_store().expect("open store");
    bootstrap_empty_namespace(config_path, namespace_id);

    let file_digest_sha256 = sha256_digest(bytes);
    let block_digest = sha256_digest(bytes);
    store
        .put_if_absent(
            &loon_objectstore::keys::blob(namespace_id.as_str(), &block_digest),
            bytes,
        )
        .expect("write content block");
    let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
        namespace_id: namespace_id.clone(),
        file_size_bytes: bytes.len() as u64,
        file_digest_sha256,
        block_size_bytes: bytes.len() as u64,
        blocks: vec![ContentBlockDescriptor {
            content_digest_sha256: block_digest,
            plaintext_size_bytes: bytes.len() as u64,
        }],
    })
    .expect("build manifest");
    let manifest_bytes = serde_json::to_vec(&manifest).expect("serialize manifest");
    let manifest_digest = sha256_digest(&manifest_bytes);
    store
        .put_if_absent(
            &loon_objectstore::keys::content_manifest(namespace_id.as_str(), &manifest_digest),
            &manifest_bytes,
        )
        .expect("write manifest");

    execute_client_mutation(
        &store,
        &ClientMutationRequest {
            namespace_id: namespace_id.clone(),
            client_request_id: "create-file".to_owned(),
            op: ClientMutationOp::CreateFile {
                parent_inode_id: loon_types::InodeId(1),
                display_name: "hello.txt".to_owned(),
                content_manifest_digest: manifest_digest,
            },
        },
        &ClientMutationExecutionParams {
            writer_id: config.server.writer_id,
            writer_version: config.server.writer_version,
            now_ms: 2_000,
            lease_duration_ms: config.server.lease_duration_ms,
        },
    )
    .expect("create authoritative file");
}

fn bootstrap_empty_namespace(config_path: &Path, namespace_id: &NamespaceId) {
    let config = loon_ops::OpsConfig::load(config_path).expect("load config");
    let store = config.open_store().expect("open store");
    bootstrap_namespace(
        &store,
        namespace_id,
        &NamespaceBootstrapParams {
            holder_id: config.server.writer_id.clone(),
            writer_version: config.server.writer_version.clone(),
            now_ms: config.ops.now_ms.expect("configured now_ms"),
            lease_duration_ms: config.server.lease_duration_ms,
            allow_existing: false,
        },
    )
    .expect("bootstrap namespace");
}
