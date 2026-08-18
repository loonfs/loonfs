use std::process::{Command, Output, Stdio};

#[test]
fn completion_generates_for_zsh_and_bash() {
    for shell in ["zsh", "bash"] {
        let output = loonfs()
            .args(["completion", "--shell", shell])
            .output()
            .expect("run completion generation");

        assert!(output.status.success(), "{output:?}");
        assert!(!output.stdout.is_empty());
        assert!(stdout(&output).contains("loonfs"));
    }
}

#[test]
fn completion_detects_the_shell_from_the_environment() {
    let output = loonfs()
        .arg("completion")
        .env("SHELL", "/bin/zsh")
        .output()
        .expect("run completion generation");

    assert!(output.status.success(), "{output:?}");
    assert!(!output.stdout.is_empty());
}

#[test]
fn completion_without_a_shell_fails_cleanly() {
    let output = loonfs()
        .arg("completion")
        .env_remove("SHELL")
        .output()
        .expect("run completion generation");

    assert!(!output.status.success(), "{output:?}");
    let stderr = stderr(&output);
    assert!(stderr.contains("--shell"), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
}

#[test]
fn completion_does_not_panic_when_stdout_closes() {
    let mut child = loonfs()
        .args(["completion", "--shell", "zsh"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn completion generation");
    drop(child.stdout.take().expect("piped stdout"));

    let output = child
        .wait_with_output()
        .expect("wait for completion generation");
    let stderr = stderr(&output);
    assert_ne!(output.status.code(), Some(101), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
}

#[test]
fn completion_rejects_json_output() {
    let output = loonfs()
        .args(["--json", "completion", "--shell", "zsh"])
        .output()
        .expect("run completion generation");

    assert!(!output.status.success(), "{output:?}");
    let stderr = stderr(&output);
    assert!(stderr.contains("--json"), "{stderr}");
    assert!(stderr.contains("not supported"), "{stderr}");
}

fn loonfs() -> Command {
    Command::new(env!("CARGO_BIN_EXE_loonfs"))
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
