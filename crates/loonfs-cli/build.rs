//! Embeds the git commit and commit date so `loonfs version` distinguishes
//! builds that share a crate version (pre-release binaries move much faster
//! than the version number). Builds without git (release tarballs) fall
//! back to "unknown".

use std::process::Command;

fn main() {
    // Ask Git for its real paths: in a linked worktree `.git` is a file,
    // not the directory `../../.git`. Detached checkouts only need HEAD;
    // branch checkouts also watch the loose ref that moves on each commit.
    watch_git_path("HEAD");
    watch_git_path("packed-refs");
    if let Some(head_ref) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watch_git_path(&head_ref);
    }
    let commit = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let commit_date =
        git(&["show", "-s", "--format=%cs", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=LOON_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=LOON_GIT_COMMIT_DATE={commit_date}");
}

fn watch_git_path(path: &str) {
    if let Some(path) = git(&["rev-parse", "--git-path", path]) {
        println!("cargo:rerun-if-changed={path}");
    }
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim().to_owned();
    (!value.is_empty()).then_some(value)
}
