use anyhow::{anyhow, Result};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotKind {
    RenderCase,
    ReplaySeed,
    MinimizeCase,
}

impl SnapshotKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SnapshotKind::RenderCase => "render-case",
            SnapshotKind::ReplaySeed => "replay-seed",
            SnapshotKind::MinimizeCase => "minimize-case",
        }
    }
}

pub fn fixture_key_from_path(path: &Path) -> Result<String> {
    let scenario_root = fs::canonicalize(scenario_root())?;
    let scenario_path = fs::canonicalize(path)?;
    let relative = scenario_path.strip_prefix(&scenario_root).map_err(|_| {
        anyhow!(
            "snapshot output requires a scenario under {}",
            scenario_root.display()
        )
    })?;

    Ok(relative.to_string_lossy().replace('\\', "/"))
}

pub fn write_snapshot(
    fixture_key: &str,
    kind: SnapshotKind,
    seed_suffix: Option<u64>,
    contents: &str,
) -> Result<PathBuf> {
    let path = snapshot_path(fixture_key, kind, seed_suffix);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, contents)?;
    Ok(path)
}

pub fn snapshot_path(fixture_key: &str, kind: SnapshotKind, seed_suffix: Option<u64>) -> PathBuf {
    let fixture_path = Path::new(fixture_key);
    let mut output = snapshot_root().join(kind.as_str());
    if let Some(parent) = fixture_path.parent() {
        output = output.join(parent);
    }

    let stem = fixture_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("scenario");
    let file_name = match seed_suffix {
        Some(seed) => format!("{stem}.seed-{seed}.txt"),
        None => format!("{stem}.txt"),
    };

    output.join(file_name)
}

fn scenario_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/scenarios")
}

fn snapshot_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/snapshots")
}

#[cfg(test)]
mod tests {
    use super::{fixture_key_from_path, snapshot_path, SnapshotKind};
    use crate::fixtures::fixture_path;

    #[test]
    fn fixture_key_from_repo_scenario_is_stable() {
        let fixture = fixture_path("native/wal_tail_replay_advances_head.yaml");
        let key = fixture_key_from_path(&fixture).expect("fixture key");
        assert_eq!(key, "native/wal_tail_replay_advances_head.yaml");
    }

    #[test]
    fn snapshot_path_includes_command_family_and_seed_suffix() {
        let path = snapshot_path(
            "native/wal_tail_replay_advances_head.yaml",
            SnapshotKind::ReplaySeed,
            Some(4242),
        );
        assert!(
            path.ends_with(
                "tests/snapshots/replay-seed/native/wal_tail_replay_advances_head.seed-4242.txt"
            ),
            "unexpected snapshot path: {}",
            path.display()
        );
    }
}
