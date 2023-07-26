use std::env;
use std::fs;

use datenbank_core::api::{
    BTreeError, Column, DatabaseResult, Error, ExecError, ExecResult, ParseError, QueryResult,
    TableError, TablePageStoreManager,
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

fn check_query<M: TablePageStoreManager>(db: &mut Database<M>, q: &str, exp: Vec<Vec<Column>>) {
    let result = db.exec(q).unwrap();
    assert_eq!(exp, query_result(result).values);
}

fn check_exec_err<M: TablePageStoreManager>(db: &mut Database<M>, q: &str, exp: Error) {
    let result = db.exec(q);
    assert_eq!(Err(exp), result);
}

fn check_exec<M: TablePageStoreManager>(db: &mut Database<M>, q: &str, exp_count: usize) {
    let result = db.exec(q).unwrap();
    assert_eq!(exp_count, exec_result(result).rows_affected);
}

fn run_basic_test<M: TablePageStoreManager>(mut db: Database<M>, with_primary_key: bool) {
    let create_table = format!(
        r"CREATE TABLE testing {{
    foo INT
    bar BOOL
    baz VARCHAR(16)
    qux LONGBLOB(32)
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
        "INSERT INTO testing (foo, bar, baz, qux) VALUES (1, false, \"hello\", X'776f726c64')",
        1,
    );

    // insert columns out of order
    check_exec(
        &mut db,
        "INSERT INTO testing (baz, foo, qux, bar) VALUES (\"world\", 2, X'68656c6c6f', true)",
        1,
    );

    // insert multiple
    check_exec(
        &mut db,
        "INSERT INTO testing (foo, bar, baz, qux) VALUES (3, false, \"whats\", X'7570'), (4, true, \"up\", X'7768617473')",
        2,
    );

    // insert duplicate row, should error
    check_exec_err(
        &mut db,
        "INSERT INTO testing (foo, bar, baz, qux) VALUES (1, false, \"hello\", X'776f726c64')",
        Error::Exec(ExecError::Table(TableError::BTree(
            BTreeError::DuplicateEntry(if with_primary_key {
                vec![b'h', b'e', b'l', b'l', b'o', b'_', 0, 0, 0, 1]
            } else {
                vec![
                    0, 0, 0, 1, b'_', 0, b'_', b'h', b'e', b'l', b'l', b'o', b'_', b'w', b'o',
                    b'r', b'l', b'd',
                ]
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
                    Column::LongBlob(b"world".to_vec()),
                ],
                vec![
                    Column::Int(4),
                    Column::Bool(true),
                    Column::VarChar("up".to_string()),
                    Column::LongBlob(b"whats".to_vec()),
                ],
                vec![
                    Column::Int(3),
                    Column::Bool(false),
                    Column::VarChar("whats".to_string()),
                    Column::LongBlob(b"up".to_vec()),
                ],
                vec![
                    Column::Int(2),
                    Column::Bool(true),
                    Column::VarChar("world".to_string()),
                    Column::LongBlob(b"hello".to_vec()),
                ],
            ]
        } else {
            vec![
                vec![
                    Column::Int(1),
                    Column::Bool(false),
                    Column::VarChar("hello".to_string()),
                    Column::LongBlob(b"world".to_vec()),
                ],
                vec![
                    Column::Int(2),
                    Column::Bool(true),
                    Column::VarChar("world".to_string()),
                    Column::LongBlob(b"hello".to_vec()),
                ],
                vec![
                    Column::Int(3),
                    Column::Bool(false),
                    Column::VarChar("whats".to_string()),
                    Column::LongBlob(b"up".to_vec()),
                ],
                vec![
                    Column::Int(4),
                    Column::Bool(true),
                    Column::VarChar("up".to_string()),
                    Column::LongBlob(b"whats".to_vec()),
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
            Column::LongBlob(b"world".to_vec()),
        ]],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE foo = 1",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
            Column::LongBlob(b"world".to_vec()),
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
            Column::LongBlob(b"world".to_vec()),
        ]],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE baz = \"hello\" AND foo = 1",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
            Column::LongBlob(b"world".to_vec()),
        ]],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE foo = 1 AND baz = \"hello\"",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
            Column::LongBlob(b"world".to_vec()),
        ]],
    );

    check_query(
        &mut db,
        "SELECT * FROM testing WHERE foo = 1 AND baz = \"hello\" AND qux = X'776f726c64'",
        vec![vec![
            Column::Int(1),
            Column::Bool(false),
            Column::VarChar("hello".to_string()),
            Column::LongBlob(b"world".to_vec()),
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

#[test]
fn test_index_memory_integration() {
    let db = Database::memory();
    run_index_test(db);
}

#[test]
fn test_index_file_integration() {
    let temp_dir = env::temp_dir().join("index_file_integration_test");
    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::create_dir(&temp_dir);
    let db = Database::file(&temp_dir);
    run_index_test(db);
    let _ = fs::remove_dir_all(&temp_dir);
}

fn run_index_test<M: TablePageStoreManager>(mut db: Database<M>) {
    let create_table = r#"CREATE TABLE index_testing {
    foo INT
    bar BOOL
    baz VARCHAR(16)
    qux LONGBLOB(32)
    PRIMARY KEY (baz, foo)
    INDEX idx_bar_foo (bar, foo)
    INDEX idx_baz_bar (baz, bar)
}"#;
    check_exec(&mut db, &create_table, 0);

    check_exec(
        &mut db,
        r#"INSERT INTO index_testing (foo, bar, baz, qux) VALUES
(1, false, "hello", X'776f726c64'),
(2, true, "world", X'68656c6c6f'),
(3, false, "whats", X'7570'),
(4, true, "up", X'7768617473')"#,
        4,
    );

    // primary key
    check_query(
        &mut db,
        "SELECT qux FROM index_testing WHERE baz = \"hello\" AND foo = 1",
        vec![vec![Column::LongBlob(b"world".to_vec())]],
    );

    // idx_bar_foo
    check_query(
        &mut db,
        "SELECT qux FROM index_testing WHERE bar = true AND foo = 2",
        vec![vec![Column::LongBlob(b"hello".to_vec())]],
    );

    // idx_baz_bar
    check_query(
        &mut db,
        "SELECT qux FROM index_testing WHERE baz = \"whats\" AND bar = false",
        vec![vec![Column::LongBlob(b"up".to_vec())]],
    );
}

#[test]
fn test_big_table_memory_integration() {
    let db = Database::memory();
    run_big_table_test(db);
}

#[test]
fn test_big_table_file_integration() {
    let temp_dir = env::temp_dir().join("big_table_file_integration_test");
    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::create_dir(&temp_dir);
    let db = Database::file(&temp_dir);
    run_big_table_test(db);
    let _ = fs::remove_dir_all(&temp_dir);
}

fn run_big_table_test<M: TablePageStoreManager>(mut db: Database<M>) {
    let create_table = r#"CREATE TABLE big_testing {
    one INT
    two VARCHAR(512)
    three Bool
}"#;
    check_exec(&mut db, &create_table, 0);

    let big_num = 10_000;
    for i in 0..big_num {
        check_exec(
            &mut db,
            &format!(
                "INSERT INTO big_testing (one, two, three) VALUES ({}, \"{}\", {})",
                i as i32,
                (i % 10).to_string().repeat(100),
                i % 2 == 0
            ),
            1,
        );
    }

    let exp_rows = (0..big_num)
        .map(|i| {
            vec![
                Column::Int(i),
                Column::VarChar((i % 10).to_string().repeat(100)),
                Column::Bool(i % 2 == 0),
            ]
        })
        .collect();

    check_query(&mut db, "SELECT * FROM big_testing", exp_rows)
}
