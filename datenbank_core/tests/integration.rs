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

fn check_query<B: TablePageStoreBuilder>(db: &mut Database<B>, q: &str, exp: Vec<Vec<Column>>) {
    let result = db.exec(q).unwrap();
    assert_eq!(exp, query_result(result).values);
}

fn check_exec<B: TablePageStoreBuilder>(db: &mut Database<B>, q: &str, exp_count: usize) {
    let result = db.exec(q).unwrap();
    assert_eq!(exp_count, exec_result(result).rows_affected);
}

fn run_basic_test<B: TablePageStoreBuilder>(mut db: Database<B>) {
    check_exec(
        &mut db,
        r"CREATE TABLE testing {
    foo INT
    bar BOOL
    baz VARCHAR(16)
}",
        0,
    );

    // insert in order
    check_exec(
        &mut db,
        "INSERT INTO testing (foo, bar, baz) VALUES (1, false, \"hello\")",
        1,
    );

    // insert columns out of order
    check_exec(
        &mut db,
        "INSERT INTO testing (baz, foo, bar) VALUES (\"world\", 2, true)",
        1,
    );

    // insert multiple
    check_exec(
        &mut db,
        "INSERT INTO testing (foo, bar, baz) VALUES (3, false, \"whats\"), (4, true, \"up\")",
        2,
    );

    check_query(
        &mut db,
        "SELECT * FROM testing",
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
            ],
        ],
    );

    check_query(
        &mut db,
        "SELECT foo, bar FROM testing",
        vec![
            vec![Column::Int(1), Column::Bool(false)],
            vec![Column::Int(2), Column::Bool(true)],
            vec![Column::Int(3), Column::Bool(false)],
            vec![Column::Int(4), Column::Bool(true)],
        ],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE foo = 1",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
        ]],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE foo = 1",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
        ]],
    );

    check_query(
        &mut db,
        "SELECT baz FROM testing WHERE foo < 3",
        vec![
            vec![Column::VarChar("hello".to_string())],
            vec![Column::VarChar("world".to_string())],
        ],
    );

    check_query(
        &mut db,
        "SELECT foo FROM testing WHERE bar = false",
        vec![vec![Column::Int(1)], vec![Column::Int(3)]],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE foo = 1 AND bar = false AND baz = \"hello\"",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
        ]],
    );
}
