#![forbid(unsafe_code)]

pub fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("create tempdir")
}
