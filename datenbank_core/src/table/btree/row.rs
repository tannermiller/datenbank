use std::cell::RefCell;
use std::io::Write;

use super::cache::DataCache;
use super::Error;
use crate::pagestore::TablePageStore;
use crate::schema::{size_of_packed_cols, Column, Schema, MAX_INLINE_VAR_LEN_COL_SIZE};

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
pub(crate) struct RowVarChar {
    inline: String,
    next_page: Option<usize>,
}

impl Row {
    pub(crate) fn from_columns<S: TablePageStore>(
        schema: Schema,
        data_cache: &mut DataCache<S>,
        cols: Vec<Column>,
    ) -> Result<Self, Error> {
        let row_cols = cols
            .into_iter()
            .map(|col| match col {
                Column::Int(i) => Ok(RowCol::Int(i)),
                Column::Bool(b) => Ok(RowCol::Bool(b)),
                Column::VarChar(s) => {
                    let bs = s.into_bytes();
                    let (inline, next_page) = if bs.len() > MAX_INLINE_VAR_LEN_COL_SIZE {
                        let inline = String::from_utf8(&bs[..MAX_INLINE_VAR_LEN_COL_SIZE].to_vec())
                            .map_err(|e| Error::InvalidColumn(e.to_string()))?;

                        let data_pages = &bs[MAX_INLINE_VAR_LEN_COL_SIZE..]
                            .chunks(data_cache.page_size() - 8)
                            .map(|b| b.to_vec())
                            .collect();

                        // TODO: Need to separately allocate pages for each of those and then use
                        // those page_ids to write each one with the pointer to the next

                        todo!()
                    } else {
                        (bs, None)
                    };
                    let inline = String::from_utf8(inline)
                        .map_err(|e| Error::InvalidColumn(e.to_string()))?;
                    Ok(RowCol::VarChar(RowVarChar { inline, next_page }))
                }
            })
            .collect::<Result<Vec<RowCol>, Error>>()?;

        let body = RefCell::new(RowBody::Unpacked(row_cols));

        Ok(Row { schema, body })
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
