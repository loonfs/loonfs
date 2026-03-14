use crate::scenario::Scenario;
use std::path::PathBuf;

pub fn fixture_path(relative_path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path)
}

pub fn load_fixture(relative_path: &str) -> Scenario {
    let path = fixture_path(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}
