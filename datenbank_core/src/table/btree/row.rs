use std::cell::RefCell;
use std::io::Write;

use crate::schema::{size_of_packed_cols, Column, Schema};

// TODO: This needs to be in terms of RowCol
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

// This holds a single row's worth of data.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Row {
    schema: Schema,
    body: RefCell<RowBody>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RowBody {
    Packed(Vec<u8>),
    Unpacked(Vec<RowCol>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RowCol {
    Int(i32),
    Bool(bool),
    VarChar(RowVarChar),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RowVarChar {
    Expanded(String),
    Contracted {
        inline: String,
        next_page: Option<usize>,
    },
}

impl Row {
    pub(crate) fn from_columns(schema: Schema, cols: Vec<Column>) -> Option<Self> {
        // TODO: I already validated this at the table level in inser(), do I need to do this here?
        //       But this can probably be gotten to from multiple paths, so better to do this here.
        //if !schema.validate_columns(&cols) {
        //    return None;
        //}

        let row_cols = cols
            .into_iter()
            .map(|col| match col {
                Column::Int(i) => RowCol::Int(i),
                Column::Bool(b) => RowCol::Bool(b),
                Column::VarChar(s) => RowCol::VarChar(RowVarChar::Expanded(s)),
            })
            .collect();

        let body = RefCell::new(RowBody::Unpacked(row_cols));

        Some(Row { schema, body })
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        // TODO: so the problem here is that we need to split out the overflowed varchars and
        // return those up. But where do we actually handle storing those as we need to know their
        // ids before we encode them.

        todo!()
    }
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
}
