//! Embeds available source metadata so `loonfs version` distinguishes builds
//! that share a crate version (pre-release binaries move much faster than the
//! version number).

use serde_json::Value;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

struct GitMetadata {
    commit: String,
    commit_date: Option<String>,
}

fn main() {
    println!("cargo:rerun-if-env-changed=LOONFS_BUILD_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=LOONFS_BUILD_GIT_COMMIT_DATE");

    let metadata = explicit_metadata()
        .or_else(packaged_metadata)
        .or_else(checkout_metadata);
    let version = env::var("CARGO_PKG_VERSION").expect("Cargo sets CARGO_PKG_VERSION");
    let long_version = match &metadata {
        Some(metadata) => match &metadata.commit_date {
            Some(date) => format!("{version} ({} {date})", metadata.commit),
            None => format!("{version} ({})", metadata.commit),
        },
        None => version,
    };

    if let Some(metadata) = metadata {
        println!("cargo:rustc-env=LOONFS_GIT_COMMIT={}", metadata.commit);
        if let Some(date) = metadata.commit_date {
            println!("cargo:rustc-env=LOONFS_GIT_COMMIT_DATE={date}");
        }
    }
    println!("cargo:rustc-env=LOONFS_LONG_VERSION={long_version}");
}

fn explicit_metadata() -> Option<GitMetadata> {
    let commit = env_value("LOONFS_BUILD_GIT_COMMIT").and_then(|value| short_commit(&value))?;
    let commit_date = env_value("LOONFS_BUILD_GIT_COMMIT_DATE");
    Some(GitMetadata {
        commit,
        commit_date,
    })
}

/// Cargo puts this file in every packaged crate, including crates.io installs.
/// Prefer it to Git so a vendored crate never reports its consumer's checkout.
fn packaged_metadata() -> Option<GitMetadata> {
    let path = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR")?).join(".cargo_vcs_info.json");
    if !path.is_file() {
        return None;
    }
    println!("cargo:rerun-if-changed={}", path.display());
    let document: Value = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
    let commit = short_commit(document.pointer("/git/sha1")?.as_str()?)?;
    Some(GitMetadata {
        commit,
        // Cargo records the source commit but not its date.
        commit_date: None,
    })
}

fn checkout_metadata() -> Option<GitMetadata> {
    // Ask Git for its real paths: in a linked worktree `.git` is a file,
    // not the directory `../../.git`. Detached checkouts only need HEAD;
    // branch checkouts also watch the loose ref that moves on each commit.
    watch_git_path("HEAD");
    watch_git_path("packed-refs");
    if let Some(head_ref) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watch_git_path(&head_ref);
    }
    Some(GitMetadata {
        commit: git(&["rev-parse", "--short=12", "HEAD"]).and_then(|value| short_commit(&value))?,
        commit_date: git(&["show", "-s", "--format=%cs", "HEAD"]),
    })
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

fn env_value(name: &str) -> Option<String> {
    let value = env::var(name).ok()?;
    let value = value.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn short_commit(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() < 12 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(value[..12].to_ascii_lowercase())
}
