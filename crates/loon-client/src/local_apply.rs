use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
#[error("local apply failed during `{operation}` for `{path}`: {source}")]
pub(crate) struct LocalApplyError {
    pub(crate) operation: &'static str,
    pub(crate) path: String,
    #[source]
    pub(crate) source: io::Error,
}

pub(crate) fn apply_bytes_atomically(
    target_path: &Path,
    bytes: &[u8],
) -> Result<(), LocalApplyError> {
    let parent_dir = target_parent_dir(target_path);
    fs::create_dir_all(parent_dir)
        .map_err(|source| io_error("create_parent_dir", parent_dir, source))?;

    let stage_path = staging_path_for_target(target_path)?;
    let mut stage_file = File::create(&stage_path)
        .map_err(|source| io_error("create_stage_file", &stage_path, source))?;
    stage_file
        .write_all(bytes)
        .map_err(|source| io_error("write_stage_file", &stage_path, source))?;
    stage_file
        .sync_all()
        .map_err(|source| io_error("sync_stage_file", &stage_path, source))?;
    drop(stage_file);

    fs::rename(&stage_path, target_path)
        .map_err(|source| io_error("rename_stage_file", target_path, source))?;
    sync_parent_dir(parent_dir)
        .map_err(|source| io_error("sync_parent_dir", parent_dir, source))?;
    Ok(())
}

pub(crate) fn staging_path_for_target(target_path: &Path) -> Result<PathBuf, LocalApplyError> {
    let file_name = target_path.file_name().ok_or_else(|| {
        io_error(
            "derive_stage_path",
            target_path,
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "target path must include a file name",
            ),
        )
    })?;
    let stage_name = format!(".{}.loon-stage", file_name.to_string_lossy());
    Ok(target_parent_dir(target_path).join(stage_name))
}

fn target_parent_dir(target_path: &Path) -> &Path {
    target_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[cfg(unix)]
fn sync_parent_dir(parent_dir: &Path) -> io::Result<()> {
    File::open(parent_dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir(_parent_dir: &Path) -> io::Result<()> {
    Ok(())
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> LocalApplyError {
    LocalApplyError {
        operation,
        path: path.display().to_string(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_bytes_atomically, staging_path_for_target};
    use loon_testkit::tempdir::TestDir;
    use std::fs;
    use std::path::Path;

    #[test]
    fn apply_bytes_atomically_replaces_target_and_cleans_stage() {
        let temp_dir = TestDir::new("local-apply-atomic-replace");
        let target_path = temp_dir.path().join("nested/report.txt");
        fs::create_dir_all(target_path.parent().expect("parent dir")).expect("create parent dir");
        fs::write(&target_path, b"stale bytes").expect("seed target");

        let stage_path = staging_path_for_target(&target_path).expect("derive stage path");
        apply_bytes_atomically(&target_path, b"fresh bytes").expect("apply bytes atomically");

        assert_eq!(fs::read(&target_path).expect("read target"), b"fresh bytes");
        assert!(
            !stage_path.exists(),
            "stage path should be cleaned after rename"
        );
    }

    #[test]
    fn apply_bytes_atomically_overwrites_stale_stage_file() {
        let temp_dir = TestDir::new("local-apply-stale-stage");
        let target_path = temp_dir.path().join("report.txt");
        let stage_path = staging_path_for_target(&target_path).expect("derive stage path");
        fs::write(&stage_path, b"stale stage bytes").expect("seed stale stage file");

        apply_bytes_atomically(&target_path, b"replacement").expect("apply bytes atomically");

        assert_eq!(fs::read(&target_path).expect("read target"), b"replacement");
        assert!(
            !stage_path.exists(),
            "stage path should be replaced and removed"
        );
    }

    #[test]
    fn staging_path_stays_in_same_directory() {
        let target_path = Path::new("reports/report.txt");
        let stage_path = staging_path_for_target(target_path).expect("derive stage path");

        assert_eq!(
            stage_path,
            Path::new("reports/.report.txt.loon-stage"),
            "stage file should stay in the target directory"
        );
    }
}
