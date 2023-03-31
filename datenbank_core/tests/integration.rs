use std::env;
use std::fs;

use datenbank_core::api::{Column, DatabaseResult, ExecResult, QueryResult, TablePageStoreBuilder};
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

fn query_result(db: DatabaseResult) -> QueryResult {
    match db {
        DatabaseResult::Query(qr) => qr,
        _ => panic!("expected DatabaseResult::QueryResult"),
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

    let result = db.exec("SELECT * FROM testing").unwrap();
    assert_eq!(
        vec![
            vec![
                Column::Int(1),
                Column::Bool(false),
                Column::VarChar("hello".to_string()),
            ],
            vec![
                Column::Int(2),
                Column::Bool(true),
                Column::VarChar("world".to_string()),
            ],
            vec![
                Column::Int(3),
                Column::Bool(false),
                Column::VarChar("whats".to_string()),
            ],
            vec![
                Column::Int(4),
                Column::Bool(true),
                Column::VarChar("up".to_string()),
            ]
        ],
        query_result(result).values
    );

    let result = db.exec("SELECT foo, bar FROM testing").unwrap();
    assert_eq!(
        vec![
            vec![Column::Int(1), Column::Bool(false),],
            vec![Column::Int(2), Column::Bool(true),],
            vec![Column::Int(3), Column::Bool(false),],
            vec![Column::Int(4), Column::Bool(true),]
        ],
        query_result(result).values
    );
}
