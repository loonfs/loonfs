use super::*;

/// Checks that a `tree.db` without a schema, which a crash between creating
/// the file and writing its schema leaves, opens as no database and is
/// deleted, so the next mount admits again.
#[test]
fn a_database_without_a_schema_opens_as_none_and_is_deleted() {
    let directory = tempfile::tempdir().expect("a state directory");
    let path = directory.path().join("tree.db");
    drop(Connection::open(&path).expect("an empty database"));
    assert!(path.exists());
    assert!(Database::open(&path).expect("an open").is_none());
    assert!(!path.exists());
}
