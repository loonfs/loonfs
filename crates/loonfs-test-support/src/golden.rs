//! Byte-exact comparison against golden fixtures.

use std::path::Path;

/// Compares `actual` with the fixture `name` in `dir`.
///
/// With `UPDATE_GOLDEN` set, writes `actual` as the fixture first. A missing
/// or diverging fixture fails the test, and the message quotes `regenerate`,
/// the command that rewrites the fixtures.
#[allow(
    clippy::panic,
    reason = "a missing or diverging fixture fails the test that called this"
)]
pub fn assert_matches_golden(dir: &Path, regenerate: &str, name: &str, actual: &[u8]) {
    let path = dir.join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixture parent dir"))
            .expect("create golden dir");
        std::fs::write(&path, actual).expect("write golden fixture");
    }
    let expected = std::fs::read(&path).unwrap_or_else(|err| {
        panic!("read golden fixture `{name}` ({err}); run `{regenerate}` to generate it")
    });
    if expected != actual {
        let offset = expected
            .iter()
            .zip(actual.iter())
            .position(|(left, right)| left != right)
            .unwrap_or_else(|| expected.len().min(actual.len()));
        panic!(
            "golden fixture `{name}` diverged at byte {offset} \
             (expected {} bytes, actual {} bytes). The durable encoding changed: \
             while pre-release, regenerate the version-1 fixture with UPDATE_GOLDEN=1 \
             if the change is intentional.",
            expected.len(),
            actual.len(),
        );
    }
}
