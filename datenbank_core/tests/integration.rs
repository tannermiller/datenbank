use datenbank_core::Database;

#[test]
fn test_create_table() {
    let mut db = Database::memory();

    let result = db
        .exec(
            r"CREATE TABLE testing {
    foo INT
    bar BOOL
    baz VARCHAR(16)
}",
        )
        .unwrap();
    assert_eq!(0, result.rows_affected);

    // insert in order
    let result = db
        .exec("INSERT INTO testing (foo, bar, baz) VALUES (1, false, \"hello\")")
        .unwrap();
    assert_eq!(1, result.rows_affected);

    // insert columns out of order
    let result = db
        .exec("INSERT INTO testing (baz, foo, bar) VALUES (\"world\", 2, true)")
        .unwrap();
    assert_eq!(1, result.rows_affected);

    // insert multiple
    let result = db
        .exec("INSERT INTO testing (foo, bar, baz) VALUES (3, false, \"whats\"), (4, true, \"up\")")
        .unwrap();
    assert_eq!(2, result.rows_affected);
}
