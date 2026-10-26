use super::common::*;

/// Runs a mount command with a state directory of the test's own, and
/// without `XDG_RUNTIME_DIR`, so it can't reach a daemon that the developer
/// runs.
fn run_mount_command(harness: &Harness, args: &[&str]) -> Output {
    harness
        .command()
        .env_remove("XDG_RUNTIME_DIR")
        .args(args)
        .arg("--state-dir")
        .arg(harness.temp_dir.path().join("state"))
        .output()
        .expect("run loonfs")
}

/// Checks the mount commands that need no daemon: `status` reports no
/// mounts, `unmount` of a directory without a mount succeeds, and a
/// writable mount is refused before anything starts.
#[cfg(target_os = "linux")]
#[test]
fn mount_commands_without_a_daemon_report_no_mounts_and_refuse_writable_mounts() {
    let harness = Harness::new();
    let directory = harness.temp_dir.path().join("mnt");
    fs::create_dir(&directory).expect("create the mount directory");
    let directory = directory.to_str().expect("utf-8 path");

    let status = run_mount_command(&harness, &["status"]);
    assert_success(&status);
    assert_eq!(stdout_string(&status).trim(), "no mounts");

    let unmounted = run_mount_command(&harness, &["unmount", directory]);
    assert_success(&unmounted);
    assert!(stdout_string(&unmounted).contains("has no mount"));

    for args in [vec!["status", directory], vec!["refresh", directory]] {
        let output = run_mount_command(&harness, &args);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert!(stderr_string(&output).contains("has no mount"), "{args:?}");
    }

    let writable = run_mount_command(&harness, &["--json", "mount", "demo", directory]);
    assert_eq!(writable.status.code(), Some(1));
    let error = json_error(&writable);
    assert_eq!(error["code"], "not_supported");
    assert_eq!(error["param"], "--ro");
    assert!(
        error["message"]
            .as_str()
            .expect("message")
            .contains("writable mounts aren't available yet"),
        "{error}"
    );
    assert!(
        !harness.temp_dir.path().join("state").exists(),
        "a refused mount starts no daemon"
    );
}

/// Checks that every mount command fails at once on a platform without
/// FUSE mounts.
#[cfg(not(target_os = "linux"))]
#[test]
fn mount_commands_fail_at_once_without_linux() {
    let harness = Harness::new();
    for args in [
        vec!["mount", "demo", ".", "--ro"],
        vec!["unmount", "."],
        vec!["status"],
        vec!["refresh", "."],
    ] {
        let output = run_mount_command(&harness, &args);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert!(
            stderr_string(&output).contains("mounts are available on Linux only"),
            "{args:?}"
        );
    }
}
