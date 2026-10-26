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

/// Checks that `tree.db` uses write-ahead logging, and syncs at checkpoints
/// rather than at every commit.
#[test]
fn the_database_logs_ahead_and_syncs_at_checkpoints() {
    let directory = tempfile::tempdir().expect("a state directory");
    let database = Database::connect(&directory.path().join("tree.db")).expect("a database");
    let connection = database.connection();
    let journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .expect("the journal mode");
    let synchronous: i64 = connection
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .expect("the sync level");
    assert_eq!(journal_mode, "wal");
    assert_eq!(synchronous, 1, "NORMAL");
}
