use std::env;
use std::fs;

use datenbank_core::api::{DatabaseResult, ExecResult, TablePageStoreBuilder};
use datenbank_core::Database;

#[test]
fn test_basic_memory_integration() {
    let db = Database::memory();
    run_basic_test(db);
}

#[test]
fn test_basic_file_integration() {
    let temp_dir = env::temp_dir().join("file_integration_test");
    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::create_dir(&temp_dir);
    let db = Database::file(&temp_dir);
    run_basic_test(db);
    let _ = fs::remove_dir_all(&temp_dir);
}

fn exec_result(db: DatabaseResult) -> ExecResult {
    match db {
        DatabaseResult::Exec(er) => er,
        _ => panic!("expected DatabaseResult::ExecResult"),
    }
}

fn run_basic_test<B: TablePageStoreBuilder>(mut db: Database<B>) {
    let result = db
        .exec(
            r"CREATE TABLE testing {
    foo INT
    bar BOOL
    baz VARCHAR(16)
}",
        )
        .unwrap();
    assert_eq!(0, exec_result(result).rows_affected);

    // insert in order
    let result = db
        .exec("INSERT INTO testing (foo, bar, baz) VALUES (1, false, \"hello\")")
        .unwrap();
    assert_eq!(1, exec_result(result).rows_affected);

    // insert columns out of order
    let result = db
        .exec("INSERT INTO testing (baz, foo, bar) VALUES (\"world\", 2, true)")
        .unwrap();
    assert_eq!(1, exec_result(result).rows_affected);

    // insert multiple
    let result = db
        .exec("INSERT INTO testing (foo, bar, baz) VALUES (3, false, \"whats\"), (4, true, \"up\")")
        .unwrap();
    assert_eq!(2, exec_result(result).rows_affected);
}
