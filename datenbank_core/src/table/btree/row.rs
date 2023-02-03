use std::io::Write;

use crate::schema::{size_of_packed_cols, Column, Schema};

// This holds a single row's worth of data.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    schema: Schema,
    data: Vec<u8>,
}

impl Row {
    pub fn from_columns(schema: Schema, cols: Vec<Column>) -> Option<Self> {
        if !schema.validate_columns(&cols) {
            return None;
        }

        let data = pack_row_data(cols);

        Some(Row { schema, data })
    }

    pub fn from_packed(schema: Schema, data: Vec<u8>) -> Option<Self> {
        if !schema.validate_packed(&data) {
            return None;
        }

        Some(Row { schema, data })
    }
}

fn pack_row_data(cols: Vec<Column>) -> Vec<u8> {
    let size = size_of_packed_cols(&cols);

    let mut row_data = Vec::with_capacity(size);

    for col in cols {
        match col {
            Column::VarChar(vc) => {
                // write len first
                row_data
                    .write_all(&(vc.len() as u16).to_be_bytes())
                    .expect("can't fail writing to vec");
                // and write the string data
                row_data
                    .write_all(vc.as_bytes())
                    .expect("can't fail writing to vec");
            }
            Column::Int(i) => {
                row_data
                    .write_all(&i.to_be_bytes())
                    .expect("can't fail writing to vec");
            }
            Column::Bool(b) => {
                let b = if b { 1 } else { 0 };
                row_data.write_all(&[b]).expect("can't fail writing to vec");
            }
        }
    }

    row_data
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::ColumnType;

    #[test]
    fn test_pack_row_data() {
        let cols = vec![
            Column::VarChar("0123456789".to_string()),
            Column::Int(7),
            Column::Bool(true),
        ];
        let bytes = pack_row_data(cols);
        assert_eq!(17, bytes.len());
        assert_eq!(
            vec![0, 10, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 0, 0, 7, 1],
            bytes
        );
    }

    #[test]
    fn test_row_from_columns() {
        fn check(schema: Schema, cols: Vec<Column>, expected_result: bool) {
            assert_eq!(expected_result, Row::from_columns(schema, cols).is_some());
        }

        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();
        let cols = vec![
            Column::Int(7),
            Column::Bool(true),
            Column::VarChar("0123456789".to_string()),
        ];
        check(schema, cols.clone(), true);

        // wrong order
        let schema = Schema::new(vec![
            ("bar".into(), ColumnType::Bool),
            ("foo".into(), ColumnType::Int),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();
        check(schema, cols.clone(), false);

        // extra col
        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
        ])
        .unwrap();
        check(schema, cols.clone(), false);

        // missing col
        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();
        let missing_cols = vec![Column::Int(7), Column::Bool(true)];
        check(schema, missing_cols, false);
    }

    #[test]
    fn test_row_from_packed() {
        fn check(schema: Schema, data: Vec<u8>, expected_result: bool) {
            assert_eq!(expected_result, Row::from_packed(schema, data).is_some());
        }

        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();

        // valid
        let data = vec![
            0, 0, 0, 10, 1, 0, 10, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57,
        ];
        check(schema.clone(), data, true);

        // empty
        let data = vec![];
        check(schema.clone(), data, false);

        // too short
        let data = vec![0, 0, 0, 10, 1, 0, 10, 48, 49, 50, 51, 52, 53];
        check(schema.clone(), data, false);

        // too long
        let data = vec![
            0, 0, 0, 10, 1, 0, 10, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 0, 0, 7,
        ];
        check(schema.clone(), data, false);
    }
}
