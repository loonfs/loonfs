//! The engine crate's dependency rule.

use std::collections::{HashMap, HashSet, VecDeque};

/// Checks that the engine doesn't depend on `loonfs-core` or `loonfs`,
/// directly or through another crate, so it uses nothing but the public API.
/// Dev dependencies don't count, because tests may use the server's crates.
#[test]
fn the_engine_depends_on_no_server_crate() {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--manifest-path",
        ])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .output()
        .expect("`cargo metadata` should run");
    assert!(
        output.status.success(),
        "`cargo metadata` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("`cargo metadata` prints JSON");
    let names: HashMap<&str, &str> = metadata["packages"]
        .as_array()
        .expect("a package list")
        .iter()
        .map(|package| {
            (
                package["id"].as_str().expect("a package ID"),
                package["name"].as_str().expect("a package name"),
            )
        })
        .collect();
    let nodes: HashMap<&str, &serde_json::Value> = metadata["resolve"]["nodes"]
        .as_array()
        .expect("a resolved graph")
        .iter()
        .map(|node| (node["id"].as_str().expect("a node ID"), node))
        .collect();
    let engine = names
        .iter()
        .find(|(_, name)| **name == "loonfs-mount")
        .map(|(id, _)| *id)
        .expect("the engine is a workspace package");

    let mut reachable = HashSet::new();
    let mut queue = VecDeque::from([engine]);
    while let Some(id) = queue.pop_front() {
        for dependency in nodes[id]["deps"].as_array().expect("a dependency list") {
            let built = dependency["dep_kinds"]
                .as_array()
                .expect("dependency kinds")
                .iter()
                .any(|kind| kind["kind"].as_str() != Some("dev"));
            let package = dependency["pkg"].as_str().expect("a dependency ID");
            if built && reachable.insert(names[package]) {
                queue.push_back(package);
            }
        }
    }
    assert!(reachable.contains("loonfs-client"), "{reachable:?}");
    for server_crate in ["loonfs-core", "loonfs"] {
        assert!(
            !reachable.contains(server_crate),
            "the engine depends on `{server_crate}`"
        );
    }
}
