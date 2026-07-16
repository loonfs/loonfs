//! Embeds the git commit and commit date so `loon version` distinguishes
//! builds that share a crate version (pre-release binaries move much faster
//! than the version number). Builds without git (release tarballs) fall
//! back to "unknown".

use std::process::Command;

fn main() {
    // HEAD moves on checkout/commit; refs cover branch fast-forwards.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
    let commit = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let commit_date =
        git(&["show", "-s", "--format=%cs", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=LOON_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=LOON_GIT_COMMIT_DATE={commit_date}");
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
