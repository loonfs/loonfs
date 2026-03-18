use crate::scenario::{Scenario, ScenarioKind};
use anyhow::Result;
use std::path::{Path, PathBuf};

pub fn fixture_path(relative_path: &str) -> PathBuf {
    fixture_root().join(relative_path)
}

pub fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/scenarios")
}

pub fn load_fixture(relative_path: &str) -> Scenario {
    let path = fixture_path(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}

pub fn fixture_paths(kind: Option<ScenarioKind>) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let root = fixture_root();
    collect_fixture_paths(&root, kind, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_fixture_paths(
    root: &Path,
    kind: Option<ScenarioKind>,
    paths: &mut Vec<PathBuf>,
) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(kind) = kind {
                if root == fixture_root()
                    && path.file_name().and_then(|name| name.to_str()) != Some(kind.as_str())
                {
                    continue;
                }
            }
            collect_fixture_paths(&path, kind, paths)?;
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) == Some("yaml") {
            paths.push(path);
        }
    }
    Ok(())
}
