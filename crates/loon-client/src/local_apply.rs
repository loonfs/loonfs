use std::fs::{self, File, OpenOptions};
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

#[allow(dead_code)]
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

pub(crate) fn create_directory_durably(target_path: &Path) -> Result<(), LocalApplyError> {
    let parent_dir = target_parent_dir(target_path);
    fs::create_dir_all(target_path)
        .map_err(|source| io_error("create_target_dir", target_path, source))?;
    sync_dir(target_path).map_err(|source| io_error("sync_target_dir", target_path, source))?;
    sync_parent_dir(parent_dir)
        .map_err(|source| io_error("sync_parent_dir", parent_dir, source))?;
    Ok(())
}

pub(crate) fn rename_path_durably(
    current_path: &Path,
    target_path: &Path,
) -> Result<(), LocalApplyError> {
    let source_parent_dir = target_parent_dir(current_path);
    let target_parent_dir = target_parent_dir(target_path);
    if target_path.exists() {
        return Err(io_error(
            "rename_target_path",
            target_path,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "target path already exists",
            ),
        ));
    }

    fs::rename(current_path, target_path)
        .map_err(|source| io_error("rename_target_path", target_path, source))?;
    sync_parent_dir(target_parent_dir)
        .map_err(|source| io_error("sync_parent_dir", target_parent_dir, source))?;
    if source_parent_dir != target_parent_dir {
        sync_parent_dir(source_parent_dir)
            .map_err(|source| io_error("sync_parent_dir", source_parent_dir, source))?;
    }
    Ok(())
}

pub(crate) fn stage_file_size(target_path: &Path) -> Result<Option<u64>, LocalApplyError> {
    let stage_path = staging_path_for_target(target_path)?;
    match fs::metadata(&stage_path) {
        Ok(metadata) => Ok(Some(metadata.len())),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(io_error("stat_stage_file", &stage_path, source)),
    }
}

pub(crate) fn reset_stage_file(target_path: &Path) -> Result<(), LocalApplyError> {
    let parent_dir = target_parent_dir(target_path);
    fs::create_dir_all(parent_dir)
        .map_err(|source| io_error("create_parent_dir", parent_dir, source))?;

    let stage_path = staging_path_for_target(target_path)?;
    let stage_file = File::create(&stage_path)
        .map_err(|source| io_error("reset_stage_file", &stage_path, source))?;
    stage_file
        .sync_all()
        .map_err(|source| io_error("sync_stage_file", &stage_path, source))?;
    Ok(())
}

pub(crate) fn append_stage_bytes(target_path: &Path, bytes: &[u8]) -> Result<(), LocalApplyError> {
    let parent_dir = target_parent_dir(target_path);
    fs::create_dir_all(parent_dir)
        .map_err(|source| io_error("create_parent_dir", parent_dir, source))?;

    let stage_path = staging_path_for_target(target_path)?;
    let mut stage_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stage_path)
        .map_err(|source| io_error("open_stage_file", &stage_path, source))?;
    stage_file
        .write_all(bytes)
        .map_err(|source| io_error("write_stage_file", &stage_path, source))?;
    stage_file
        .sync_all()
        .map_err(|source| io_error("sync_stage_file", &stage_path, source))?;
    Ok(())
}

pub(crate) fn finalize_stage_file(target_path: &Path) -> Result<(), LocalApplyError> {
    let parent_dir = target_parent_dir(target_path);
    let stage_path = staging_path_for_target(target_path)?;
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

#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        append_stage_bytes, apply_bytes_atomically, create_directory_durably, finalize_stage_file,
        rename_path_durably, reset_stage_file, stage_file_size, staging_path_for_target,
    };
    use loon_testkit::tempdir::TestDir;
    use std::fs;
    use std::io;
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

    #[test]
    fn create_directory_durably_creates_target_directory() {
        let temp_dir = TestDir::new("local-apply-dir");
        let target_path = temp_dir.path().join("nested/incoming");

        create_directory_durably(&target_path).expect("create directory durably");

        assert!(target_path.is_dir(), "target directory should exist");
    }

    #[test]
    fn rename_path_durably_moves_file_within_same_directory() {
        let temp_dir = TestDir::new("local-apply-rename-same-parent");
        let current_path = temp_dir.path().join("docs/report.txt");
        let target_path = temp_dir.path().join("docs/report-renamed.txt");
        fs::create_dir_all(current_path.parent().expect("parent dir")).expect("create parent dir");
        fs::write(&current_path, "hello from loon").expect("seed current file");

        rename_path_durably(&current_path, &target_path).expect("rename path durably");

        assert!(!current_path.exists(), "current path should be removed");
        assert_eq!(
            fs::read_to_string(&target_path).expect("read target path"),
            "hello from loon"
        );
    }

    #[test]
    fn rename_path_durably_moves_file_across_directories() {
        let temp_dir = TestDir::new("local-apply-rename-cross-dir");
        let current_path = temp_dir.path().join("docs/report.txt");
        let target_path = temp_dir.path().join("archive/report.txt");
        fs::create_dir_all(current_path.parent().expect("source parent"))
            .expect("create source parent");
        fs::create_dir_all(target_path.parent().expect("target parent"))
            .expect("create target parent");
        fs::write(&current_path, "hello from loon").expect("seed current file");

        rename_path_durably(&current_path, &target_path).expect("rename path durably");

        assert!(!current_path.exists(), "current path should be removed");
        assert_eq!(
            fs::read_to_string(&target_path).expect("read moved target path"),
            "hello from loon"
        );
    }

    #[test]
    fn rename_path_durably_rejects_existing_destination() {
        let temp_dir = TestDir::new("local-apply-rename-destination-occupied");
        let current_path = temp_dir.path().join("docs/report.txt");
        let target_path = temp_dir.path().join("docs/report-renamed.txt");
        fs::create_dir_all(current_path.parent().expect("parent dir")).expect("create parent dir");
        fs::write(&current_path, "hello from loon").expect("seed current file");
        fs::write(&target_path, "occupied").expect("seed occupied destination");

        let error =
            rename_path_durably(&current_path, &target_path).expect_err("rename should fail");

        assert_eq!(error.operation, "rename_target_path");
        assert_eq!(error.path, target_path.display().to_string());
        assert_eq!(error.source.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            current_path.exists(),
            "current path should remain after failed rename"
        );
        assert_eq!(
            fs::read_to_string(&target_path).expect("read occupied target path"),
            "occupied"
        );
    }

    #[test]
    fn reset_and_append_stage_file_track_expected_size() {
        let temp_dir = TestDir::new("local-apply-stage-progress");
        let target_path = temp_dir.path().join("inbox/report.txt");

        reset_stage_file(&target_path).expect("reset stage file");
        assert_eq!(
            stage_file_size(&target_path).expect("stat stage file"),
            Some(0)
        );

        append_stage_bytes(&target_path, b"hello ").expect("append first block");
        append_stage_bytes(&target_path, b"loon").expect("append second block");

        let stage_path = staging_path_for_target(&target_path).expect("derive stage path");
        assert_eq!(
            fs::read(&stage_path).expect("read stage bytes"),
            b"hello loon"
        );
        assert_eq!(
            stage_file_size(&target_path).expect("stat stage file"),
            Some(10)
        );
    }

    #[test]
    fn finalize_stage_file_renames_stage_into_target() {
        let temp_dir = TestDir::new("local-apply-finalize-stage");
        let target_path = temp_dir.path().join("inbox/report.txt");

        reset_stage_file(&target_path).expect("reset stage file");
        append_stage_bytes(&target_path, b"hello from loon").expect("append bytes");
        let stage_path = staging_path_for_target(&target_path).expect("derive stage path");

        finalize_stage_file(&target_path).expect("finalize stage file");

        assert_eq!(
            fs::read_to_string(&target_path).expect("read finalized target"),
            "hello from loon"
        );
        assert!(
            !stage_path.exists(),
            "stage file should be removed by rename"
        );
    }
}
