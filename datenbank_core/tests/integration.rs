use std::env;
use std::fs;

use datenbank_core::api::{
    BTreeError, Column, DatabaseResult, Error, ExecError, ExecResult, ParseError, QueryResult,
    TableError, TablePageStoreBuilder,
};
use datenbank_core::Database;

#[test]
fn test_basic_memory_no_pk_integration() {
    let db = Database::memory();
    run_basic_test(db, false);
}

#[test]
fn test_basic_memory_with_pk_integration() {
    let db = Database::memory();
    run_basic_test(db, true);
}

#[test]
fn test_basic_file_no_pk_integration() {
    let temp_dir = env::temp_dir().join("file_integration_test_no_pk");
    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::create_dir(&temp_dir);
    let db = Database::file(&temp_dir);
    run_basic_test(db, false);
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_basic_file_with_pk_integration() {
    let temp_dir = env::temp_dir().join("file_integration_test_with_pk");
    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::create_dir(&temp_dir);
    let db = Database::file(&temp_dir);
    run_basic_test(db, true);
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

fn check_exec_err<B: TablePageStoreBuilder>(db: &mut Database<B>, q: &str, exp: Error) {
    let result = db.exec(q);
    assert_eq!(Err(exp), result);
}

fn check_exec<B: TablePageStoreBuilder>(db: &mut Database<B>, q: &str, exp_count: usize) {
    let result = db.exec(q).unwrap();
    assert_eq!(exp_count, exec_result(result).rows_affected);
}

fn run_basic_test<B: TablePageStoreBuilder>(mut db: Database<B>, with_primary_key: bool) {
    let create_table = format!(
        r"CREATE TABLE testing {{
    foo INT
    bar BOOL
    baz VARCHAR(16)
{}}}",
        if with_primary_key {
            "    PRIMARY KEY (baz, foo)\n"
        } else {
            ""
        }
    );

    check_exec(&mut db, &create_table, 0);

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

    // insert duplicate row, should error
    check_exec_err(
        &mut db,
        "INSERT INTO testing (foo, bar, baz) VALUES (1, false, \"hello\")",
        Error::Exec(ExecError::Table(TableError::BTree(
            BTreeError::DuplicateEntry(if with_primary_key {
                vec![b'h', b'e', b'l', b'l', b'o', b'_', 0, 0, 0, 1]
            } else {
                vec![0, 0, 0, 1, b'_', 0, b'_', b'h', b'e', b'l', b'l', b'o']
            }),
        ))),
    );

    // The order of results will be different based on the primary key in a table scan because the
    // rows are in a different order.
    check_query(
        &mut db,
        "SELECT * FROM testing",
        if with_primary_key {
            vec![
                vec![
                    Column::Int(1),
                    Column::Bool(false),
                    Column::VarChar("hello".to_string()),
                ],
                vec![
                    Column::Int(4),
                    Column::Bool(true),
                    Column::VarChar("up".to_string()),
                ],
                vec![
                    Column::Int(3),
                    Column::Bool(false),
                    Column::VarChar("whats".to_string()),
                ],
                vec![
                    Column::Int(2),
                    Column::Bool(true),
                    Column::VarChar("world".to_string()),
                ],
            ]
        } else {
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
            ]
        },
    );

    check_query(
        &mut db,
        "SELECT foo, bar FROM testing",
        if with_primary_key {
            vec![
                vec![Column::Int(1), Column::Bool(false)],
                vec![Column::Int(4), Column::Bool(true)],
                vec![Column::Int(3), Column::Bool(false)],
                vec![Column::Int(2), Column::Bool(true)],
            ]
        } else {
            vec![
                vec![Column::Int(1), Column::Bool(false)],
                vec![Column::Int(2), Column::Bool(true)],
                vec![Column::Int(3), Column::Bool(false)],
                vec![Column::Int(4), Column::Bool(true)],
            ]
        },
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

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE baz = \"hello\" AND foo = 1",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
        ]],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE foo = 1 AND baz = \"hello\"",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
        ]],
    );

    // garbage in, error out
    check_exec_err(
        &mut db,
        "garbage",
        Error::Parse(ParseError {
            msg: "error Tag at: garbage".into(),
        }),
    )
}
