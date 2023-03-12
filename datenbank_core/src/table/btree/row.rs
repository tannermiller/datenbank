use std::cell::RefCell;
use std::io::Write;

use super::cache::Cache;
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
    pub(crate) schema: Schema,
    pub(crate) body: RefCell<Vec<RowCol>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RowCol {
    Int(i32),
    Bool(bool),
    VarChar(RowVarChar),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowVarChar {
    pub(crate) inline: String,
    pub(crate) next_page: Option<usize>,
}

impl Row {
    pub(crate) fn encode(&self) -> Vec<u8> {
        // TODO: so the problem here is that we need to split out the overflowed varchars and
        // return those up. But where do we actually handle storing those as we need to know their
        // ids before we encode them.

        todo!()
    }

    pub(crate) fn key(&self) -> String {
        // TODO: extract the row key, this will start with just all the columns, but this will be
        // more important when I impl primary keys and indexes as it will be different for those.
        todo!()
    }
}

// ProcessedRow represents a row that has been split in order to fit into a Row, but may yet
// require some of the overflow data from variable length rows to be allocated and stored
// separately.
#[derive(Debug, PartialEq)]
pub(crate) struct ProcessedRow {
    schema: Schema,
    columns: Vec<(RowCol, Option<Vec<Vec<u8>>>)>,
}

impl ProcessedRow {
    // Finalize the row by allocating and storing any overflow data pages from variable length
    // columns.
    pub(crate) fn finalize<S: TablePageStore>(
        self,
        cache: &mut Cache<S, Vec<u8>>,
    ) -> Result<Row, Error> {
        let ProcessedRow { schema, columns } = self;

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
                .map(|_| cache.allocate())
                .collect::<Result<Vec<usize>, Error>>()?;
            let first_next_page = page_ids[0];
            let mut next_page_ids: Vec<Option<usize>> =
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
                            .write_all(&(page_id as u32).to_be_bytes())
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

            // VarChar is our only variable length value so this is fine for now
            match row {
                RowCol::VarChar(ref mut rvc) => {
                    rvc.next_page = Some(first_next_page);
                }
                _ => unreachable!(),
            }

            result_rows.push(row);
        }

        Ok(Row {
            schema,
            body: RefCell::new(result_rows),
        })
    }
}

pub(crate) fn process_columns(
    schema: Schema,
    page_size: usize,
    cols: Vec<Column>,
) -> Result<ProcessedRow, Error> {
    let columns = cols
        .into_iter()
        .map(|col| match col {
            Column::Int(i) => Ok((RowCol::Int(i), None)),
            Column::Bool(b) => Ok((RowCol::Bool(b), None)),
            Column::VarChar(s) => {
                let bs = s.into_bytes();
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

                let inline =
                    String::from_utf8(inline).map_err(|e| Error::InvalidColumn(e.to_string()))?;
                Ok((
                    RowCol::VarChar(RowVarChar {
                        inline,
                        next_page: None,
                    }),
                    data_pages,
                ))
            }
        })
        .collect::<Result<Vec<(RowCol, Option<Vec<Vec<u8>>>)>, Error>>()?;

    Ok(ProcessedRow { schema, columns })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pagestore::Memory;
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
    fn test_process_columns() {
        fn check(
            schema: Schema,
            page_size: usize,
            cols: Vec<Column>,
            expected: Option<ProcessedRow>,
        ) {
            let result = process_columns(schema, page_size, cols);
            match (result, expected) {
                (Ok(cols), Some(exp)) => assert_eq!(exp, cols),
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
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
        check(
            schema.clone(),
            64,
            cols.clone(),
            Some(ProcessedRow {
                schema: schema.clone(),
                columns: vec![
                    (RowCol::Int(7), None),
                    (RowCol::Bool(true), None),
                    (
                        RowCol::VarChar(RowVarChar {
                            inline: "0123456789".to_string(),
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
        let cols = vec![
            Column::Int(7),
            Column::Bool(true),
            Column::VarChar(var_char_value),
        ];
        check(
            schema.clone(),
            8, // absurdly low to verify paging is correct
            cols.clone(),
            Some(ProcessedRow {
                schema,
                columns: vec![
                    (RowCol::Int(7), None),
                    (RowCol::Bool(true), None),
                    (
                        RowCol::VarChar(RowVarChar {
                            inline: base_str,
                            next_page: None,
                        }),
                        Some(vec![
                            "012".as_bytes().to_vec(),
                            "345".as_bytes().to_vec(),
                            "678".as_bytes().to_vec(),
                            "9".as_bytes().to_vec(),
                        ]),
                    ),
                ],
            }),
        );
    }

    #[test]
    fn test_processed_rows_finalize() {
        let mut store = Memory::new(64);
        let mut data_cache = Cache::new(store.clone());
        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();
        let base_str = "1".repeat(MAX_INLINE_VAR_LEN_COL_SIZE);
        let pr = ProcessedRow {
            schema: schema.clone(),
            columns: vec![
                (RowCol::Int(7), None),
                (RowCol::Bool(true), None),
                (
                    RowCol::VarChar(RowVarChar {
                        inline: base_str.clone(),
                        next_page: None,
                    }),
                    Some(vec![
                        "012".as_bytes().to_vec(),
                        "345".as_bytes().to_vec(),
                        "678".as_bytes().to_vec(),
                        "9".as_bytes().to_vec(),
                    ]),
                ),
            ],
        };

        let row = pr.finalize(&mut data_cache).unwrap();

        assert_eq!(
            Row {
                schema,
                body: RefCell::new(vec![
                    RowCol::Int(7),
                    RowCol::Bool(true),
                    RowCol::VarChar(RowVarChar {
                        inline: base_str,
                        next_page: Some(1),
                    }),
                ]),
            },
            row
        );

        // ensure that the 4 pages were allocated by seeing the next allocated one is 5
        assert_eq!(5, store.allocate().unwrap());
        assert_eq!(
            &vec![1u8, 0, 0, 0, 2, 48, 49, 50],
            data_cache.get(1).unwrap()
        );
        assert_eq!(
            &vec![1u8, 0, 0, 0, 3, 51, 52, 53],
            data_cache.get(2).unwrap()
        );
        assert_eq!(
            &vec![1u8, 0, 0, 0, 4, 54, 55, 56],
            data_cache.get(3).unwrap()
        );
        assert_eq!(&vec![0u8, 0, 0, 0, 1, 57], data_cache.get(4).unwrap());
    }
}
