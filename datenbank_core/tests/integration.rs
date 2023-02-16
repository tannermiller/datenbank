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
}
