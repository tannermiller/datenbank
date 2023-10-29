use std::io::Write;
use std::rc::Rc;

use crate::cache::{Cache, Error as CacheError};
use crate::key;
use crate::pagestore::{PageID, TablePageStore};
use crate::schema::Error as SchemaError;
use crate::schema::{Column, Schema, MAX_INLINE_VAR_LEN_COL_SIZE};

pub(crate) mod encode;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("invalid column: {0}")]
    InvalidColumn(String),
    #[error("cache error")]
    Cache(#[from] CacheError),
    #[error("schema error")]
    Schema(#[from] SchemaError),
}

// This holds a single row's worth of data.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub body: Vec<RowCol>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RowCol {
    Int(i32),
    Bool(bool),
    VarChar(RowBytes),
    LongBlob(RowBytes),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowBytes {
    pub(crate) inline: Vec<u8>,
    pub(crate) next_page: Option<PageID>,
}

impl Row {
    pub(crate) fn to_columns<S: TablePageStore>(
        &self,
        data_cache: &mut Cache<S, Vec<u8>>,
        schema: &Schema,
        columns: &[Rc<String>],
    ) -> Result<Vec<Column>, Error> {
        let mut row_values = Vec::with_capacity(columns.len());

        for (col, (schema_col, _)) in self.body.iter().zip(schema.columns()) {
            if !columns.contains(schema_col) {
                continue;
            }

            let val = match col {
                RowCol::Int(i) => Column::Int(*i),
                RowCol::Bool(b) => Column::Bool(*b),
                RowCol::VarChar(vc) => {
                    let vc_data = load_full_row_bytes(data_cache, vc)?;

                    match String::from_utf8(vc_data) {
                        Ok(s) => Column::VarChar(s),
                        Err(e) => {
                            return Err(Error::InvalidColumn(e.to_string()));
                        }
                    }
                }
                RowCol::LongBlob(lb) => {
                    let lb_data = load_full_row_bytes(data_cache, lb)?;
                    Column::LongBlob(lb_data)
                }
            };
            row_values.push(val);
        }

        Ok(row_values)
    }

    pub(crate) fn key(&self, schema: &Schema) -> Vec<u8> {
        match schema.primary_key_indices() {
            Some(pk_indices) => {
                let key_parts: Vec<&RowCol> =
                    pk_indices.iter().map(|pki| &self.body[*pki]).collect();
                key::build(&key_parts)
            }
            None => key::build(&self.body),
        }
    }
}

fn load_full_row_bytes<S: TablePageStore>(
    data_cache: &mut Cache<S, Vec<u8>>,
    rb: &RowBytes,
) -> Result<Vec<u8>, Error> {
    let mut data = rb.inline.to_vec();

    if let Some(data_page_id) = rb.next_page {
        let mut data_page_id = data_page_id;
        loop {
            let rb_page = data_cache.get(data_page_id)?;

            if rb_page.len() < 4 {
                break;
            }

            let next_pointer =
                u32::from_be_bytes(rb_page[..4].try_into().map_err(
                    |e: std::array::TryFromSliceError| Error::InvalidColumn(e.to_string()),
                )?);

            data.extend(&rb_page[4..]);

            if next_pointer != 0 {
                data_page_id = next_pointer.into();
            } else {
                break;
            }
        }
    }

    Ok(data)
}

// ProcessedRow represents a row that has been split in order to fit into a Row, but may yet
// require some of the overflow data from variable length rows to be allocated and stored
// separately.
#[derive(Debug, PartialEq)]
pub(crate) struct ProcessedRow {
    columns: Vec<(RowCol, Option<Vec<Vec<u8>>>)>,
}

impl ProcessedRow {
    // Finalize the row by allocating and storing any overflow data pages from variable length
    // columns.
    pub(crate) fn finalize<S: TablePageStore>(
        self,
        cache: &mut Cache<S, Vec<u8>>,
    ) -> Result<Row, Error> {
        let ProcessedRow { columns } = self;

        let mut result_rows = Vec::with_capacity(columns.len());
        for (mut row, data_pages) in columns {
            let data_pages = match data_pages {
                Some(data_pages) => data_pages,
                None => {
                    result_rows.push(row);
                    continue;
                }
            };

            let page_ids = (0..data_pages.len())
                .map(|_| cache.allocate().map_err(Into::into))
                .collect::<Result<Vec<PageID>, Error>>()?;
            let first_next_page = page_ids[0];
            let mut next_page_ids: Vec<Option<PageID>> =
                page_ids.iter().skip(1).map(|pg_id| Some(*pg_id)).collect();
            next_page_ids.push(None);

            for ((page, page_id), next_page_id) in
                data_pages.into_iter().zip(page_ids).zip(next_page_ids)
            {
                let mut page_to_write = Vec::with_capacity(page.len() + 5);
                match next_page_id {
                    Some(page_id) => {
                        page_to_write.push(1);
                        page_to_write
                            .write_all(&page_id.to_be_bytes())
                            .expect("can't fail writing to vec");
                    }
                    None => {
                        page_to_write.push(0);
                        page_to_write
                            .write_all(&(page.len() as u32).to_be_bytes())
                            .expect("can't fail writing to vec");
                    }
                }

                page_to_write.extend(page);
                cache.put(page_id, page_to_write)?;
            }

            // VarChar and LongBlob are the only variable length values so this is fine
            match row {
                RowCol::VarChar(ref mut rb) | RowCol::LongBlob(ref mut rb) => {
                    rb.next_page = Some(first_next_page);
                }
                _ => unreachable!(),
            }

            result_rows.push(row);
        }

        Ok(Row { body: result_rows })
    }
}

pub(crate) fn process_columns(page_size: usize, cols: Vec<Column>) -> Result<ProcessedRow, Error> {
    let columns = cols
        .into_iter()
        .map(|col| match col {
            Column::Int(i) => Ok((RowCol::Int(i), None)),
            Column::Bool(b) => Ok((RowCol::Bool(b), None)),
            Column::VarChar(s) => {
                let (rb, data_pages) = full_bytes_to_row_bytes(page_size, s.into_bytes());
                Ok((RowCol::VarChar(rb), data_pages))
            }
            Column::LongBlob(lb) => {
                let (rb, data_pages) = full_bytes_to_row_bytes(page_size, lb);
                Ok((RowCol::LongBlob(rb), data_pages))
            }
        })
        .collect::<Result<Vec<(RowCol, Option<Vec<Vec<u8>>>)>, Error>>()?;

    Ok(ProcessedRow { columns })
}

fn full_bytes_to_row_bytes(page_size: usize, bs: Vec<u8>) -> (RowBytes, Option<Vec<Vec<u8>>>) {
    let (inline, data_pages) = if bs.len() > MAX_INLINE_VAR_LEN_COL_SIZE {
        let inline = bs[..MAX_INLINE_VAR_LEN_COL_SIZE].to_vec();

        // chunk out the remaining and assign page ids to them, see the below
        // comment for description of the 5 byte header.
        let data_pages: Vec<Vec<u8>> = bs[MAX_INLINE_VAR_LEN_COL_SIZE..]
            .chunks(page_size - 5)
            .map(|b| b.to_vec())
            .collect();

        (inline, Some(data_pages))
    } else {
        (bs, None)
    };

    (
        RowBytes {
            inline,
            next_page: None,
        },
        data_pages,
    )
}

// A Predicate is used to select rows to return during a scan;
pub trait Predicate<S: TablePageStore> {
    fn is_satisfied_by(
        &self,
        schema: &Schema,
        data_cache: &mut Cache<S, Vec<u8>>,
        row: &Row,
    ) -> Result<bool, Error>;
}

// AllRows is a Predicate that matches, and therefore returns, all rows in a table.
pub struct AllRows;

impl<S: TablePageStore> Predicate<S> for AllRows {
    fn is_satisfied_by(
        &self,
        _: &Schema,
        _: &mut Cache<S, Vec<u8>>,
        _: &Row,
    ) -> Result<bool, Error> {
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pagestore::{MemoryManager, TablePageStoreBuilder, TablePageStoreManager};
    use crate::schema::ColumnType;

    #[test]
    fn test_process_columns() {
        fn check(page_size: usize, cols: Vec<Column>, expected: Option<ProcessedRow>) {
            let result = process_columns(page_size, cols);
            match (result, expected) {
                (Ok(cols), Some(exp)) => assert_eq!(exp, cols),
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        let cols = vec![
            Column::Int(7),
            Column::Bool(true),
            Column::VarChar("0123456789".to_string()),
            Column::LongBlob(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ];
        check(
            64,
            cols.clone(),
            Some(ProcessedRow {
                columns: vec![
                    (RowCol::Int(7), None),
                    (RowCol::Bool(true), None),
                    (
                        RowCol::VarChar(RowBytes {
                            inline: b"0123456789".to_vec(),
                            next_page: None,
                        }),
                        None,
                    ),
                    (
                        RowCol::LongBlob(RowBytes {
                            inline: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                            next_page: None,
                        }),
                        None,
                    ),
                ],
            }),
        );

        let base_str = "1".repeat(MAX_INLINE_VAR_LEN_COL_SIZE);
        let mut var_char_value = base_str.clone();
        var_char_value.extend("0123456789".chars());
        let base_bytes = [1u8].repeat(MAX_INLINE_VAR_LEN_COL_SIZE);
        let mut long_blob_value = base_bytes.clone();
        long_blob_value.extend(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let cols = vec![
            Column::Int(7),
            Column::Bool(true),
            Column::VarChar(var_char_value),
            Column::LongBlob(long_blob_value),
        ];
        check(
            8, // absurdly low to verify paging is correct
            cols.clone(),
            Some(ProcessedRow {
                columns: vec![
                    (RowCol::Int(7), None),
                    (RowCol::Bool(true), None),
                    (
                        RowCol::VarChar(RowBytes {
                            inline: base_str.as_bytes().to_vec(),
                            next_page: None,
                        }),
                        Some(vec![
                            "012".as_bytes().to_vec(),
                            "345".as_bytes().to_vec(),
                            "678".as_bytes().to_vec(),
                            "9".as_bytes().to_vec(),
                        ]),
                    ),
                    (
                        RowCol::LongBlob(RowBytes {
                            inline: base_bytes.to_vec(),
                            next_page: None,
                        }),
                        Some(vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9]]),
                    ),
                ],
            }),
        );
    }

    #[test]
    fn test_processed_rows_finalize() {
        let mut store_builder = MemoryManager::new(64).builder("test").unwrap();
        let mut data_cache = Cache::new(store_builder.build().unwrap());
        let base_str = "1".repeat(MAX_INLINE_VAR_LEN_COL_SIZE);
        let base_blob = [1].repeat(MAX_INLINE_VAR_LEN_COL_SIZE);
        let pr = ProcessedRow {
            columns: vec![
                (RowCol::Int(7), None),
                (RowCol::Bool(true), None),
                (
                    RowCol::VarChar(RowBytes {
                        inline: base_str.as_bytes().to_vec(),
                        next_page: None,
                    }),
                    Some(vec![
                        "012".as_bytes().to_vec(),
                        "345".as_bytes().to_vec(),
                        "678".as_bytes().to_vec(),
                        "9".as_bytes().to_vec(),
                    ]),
                ),
                (
                    RowCol::LongBlob(RowBytes {
                        inline: base_blob.clone(),
                        next_page: None,
                    }),
                    Some(vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9]]),
                ),
            ],
        };

        let row = pr.finalize(&mut data_cache).unwrap();

        assert_eq!(
            Row {
                body: vec![
                    RowCol::Int(7),
                    RowCol::Bool(true),
                    RowCol::VarChar(RowBytes {
                        inline: base_str.as_bytes().to_vec(),
                        next_page: Some(1u32.into()),
                    }),
                    RowCol::LongBlob(RowBytes {
                        inline: base_blob,
                        next_page: Some(5u32.into()),
                    }),
                ],
            },
            row
        );

        // ensure that the 8 pages were allocated by seeing the next allocated one is 9
        let mut store = store_builder.build().unwrap();
        assert_eq!(PageID::from(9u32), store.allocate_new().unwrap());
        assert_eq!(
            &vec![1u8, 0, 0, 0, 2, 48, 49, 50],
            data_cache.get(1u32.into()).unwrap()
        );
        assert_eq!(
            &vec![1u8, 0, 0, 0, 3, 51, 52, 53],
            data_cache.get(2u32.into()).unwrap()
        );
        assert_eq!(
            &vec![1u8, 0, 0, 0, 4, 54, 55, 56],
            data_cache.get(3u32.into()).unwrap()
        );
        assert_eq!(
            &vec![0u8, 0, 0, 0, 1, 57],
            data_cache.get(4u32.into()).unwrap()
        );
    }

    #[test]
    fn test_key() {
        let row = Row {
            body: vec![
                RowCol::Int(7),
                RowCol::Bool(true),
                RowCol::VarChar(RowBytes {
                    inline: "Hello, World!".as_bytes().to_vec(),
                    next_page: None,
                }),
                RowCol::LongBlob(RowBytes {
                    inline: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                    next_page: None,
                }),
            ],
        };

        let schema_no_pk = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("baz".into(), ColumnType::VarChar(16)),
                ("qux".into(), ColumnType::LongBlob(16)),
            ],
            None,
            vec![],
        )
        .unwrap();

        let schema_with_pk = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("baz".into(), ColumnType::VarChar(16)),
                ("qux".into(), ColumnType::LongBlob(16)),
            ],
            Some(vec!["baz", "foo"]),
            vec![],
        )
        .unwrap();

        assert_eq!(
            vec![
                0, 0, 0, 7, b'_', 1, b'_', b'H', b'e', b'l', b'l', b'o', b',', b' ', b'W', b'o',
                b'r', b'l', b'd', b'!', b'_', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            ],
            row.key(&schema_no_pk)
        );
        assert_eq!(
            vec![
                b'H', b'e', b'l', b'l', b'o', b',', b' ', b'W', b'o', b'r', b'l', b'd', b'!', b'_',
                0, 0, 0, 7,
            ],
            row.key(&schema_with_pk)
        );
    }
}
