use std::cell::RefCell;
use std::io::Write;

use super::Error;
use crate::pagestore::TablePageStore;
use crate::schema::{size_of_packed_cols, Column, Schema, MAX_INLINE_VAR_LEN_COL_SIZE};
use cache::store::DataCache;

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
    pub(crate) fn encode(&self) -> Vec<u8> {
        // TODO: so the problem here is that we need to split out the overflowed varchars and
        // return those up. But where do we actually handle storing those as we need to know their
        // ids before we encode them.

        todo!()
    }
}

pub(crate) struct ProcessedRow {
    schema: Schema,
    columns: Vec<(RowCol, Option<Vec<Vec<u8>>>)>,
}

impl ProcessedRow {
    pub(crate) fn allocate_and_store_pages<S: TablePageStore>(
        self,
        cache: &mut DataCache<S>,
    ) -> Result<Row, Error> {
        let ProcessedRow { schema, columns } = self;

        let mut result_rows = Vec::with_capacity(columns.len());
        for (row, data_pages) in columns {
            let data_pages = match data_pages {
                Some(data_pages) => data_pages,
                None => {
                    result_rows.push(row);
                    continue;
                }
            };

            let page_ids = (0..data_pages.len()).map(|_| cache.allocate()).collect()?;
            let first_next_page = page_ids[0];
            let mut next_page_ids: Vec<Option<usize>> = page_ids
                .into_iter()
                .skip(1)
                .map(|pg_id| Some(*pg_id))
                .collect();
            next_page_ids.push(None);

            for ((page_id, page), next_page_id) in data_pages.into_iter().zip(next_page_ids) {
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

            todo!()
        }

        Ok(Row {
            schema,
            body: RefCell::new(RowBody::Unpacked(result_rows)),
        })
    }
}

pub(crate) fn process_columns(
    schema: Schema,
    page_size: usize,
    cols: Vec<Column>,
) -> Result<ProcessedRow, Error> {
    let row_cols = cols
        .into_iter()
        .map(|col| match col {
            Column::Int(i) => Ok(RowCol::Int(i)),
            Column::Bool(b) => Ok(RowCol::Bool(b)),
            Column::VarChar(s) => {
                let bs = s.into_bytes();
                let (inline, next_page) = if bs.len() > MAX_INLINE_VAR_LEN_COL_SIZE {
                    let inline = bs[..MAX_INLINE_VAR_LEN_COL_SIZE].to_vec();

                    // chunk out the remaining and assign page ids to them, see the below
                    // comment for description of the 5 byte header.
                    let data_pages: Vec<Vec<u8>> = bs[MAX_INLINE_VAR_LEN_COL_SIZE..]
                        .chunks(page_size - 5)
                        .map(|b| b.to_vec())
                        .collect()?;

                    //let (next_page, _) = data_pages[0];

                    // shift the page_ids forward one so that each page is paired with the next
                    // page's id
                    //let mut next_page_ids: Vec<Option<usize>> = data_pages
                    //    .iter()
                    //    .skip(1)
                    //    .map(|(pd_id, _)| Some(*pd_id))
                    //    .collect();
                    //next_page_ids.push(None);

                    // each data page has the following format:
                    //   * 1 byte indicating whether this is the final page in this value (0)
                    //     or that it is a middle page
                    //   * 4 bytes that hold a u32 which are either the length of the final
                    //   part of the value (when the type byte is 0), or the page id of the
                    //   next page in this value (when the type byte is 1).
                    //   * The remaining of the page data follows that 5 byte header.
                    for ((page_id, page), next_page_id) in data_pages.into_iter().zip(next_page_ids)
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
                        data_cache.put(page_id, page_to_write)?;
                    }

                    (inline, Some(next_page))
                } else {
                    (bs, None)
                };

                let inline =
                    String::from_utf8(inline).map_err(|e| Error::InvalidColumn(e.to_string()))?;
                Ok(RowCol::VarChar(RowVarChar { inline, next_page }))
            }
        })
        .collect::<Result<Vec<RowCol>, Error>>()?;

    let body = RefCell::new(RowBody::Unpacked(row_cols));

    Ok(Row { schema, body })
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
        fn check(schema: Schema, cols: Vec<Column>, expected: Option<Row>) {
            let mut data_cache = DataCache::new(Memory::new(64 * 1024));
            let result = Row::process_columns(schema, &mut data_cache, cols);
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
            cols.clone(),
            Some(Row {
                schema,
                body: RefCell::new(RowBody::Unpacked(vec![
                    RowCol::Int(7),
                    RowCol::Bool(true),
                    RowCol::VarChar(RowVarChar {
                        inline: "0123456789".to_string(),
                        next_page: None,
                    }),
                ])),
            }),
        );

        let cols = vec![
            Column::Int(7),
            Column::Bool(true),
            Column::VarChar("TODO: repeat me to test paging of big varchar values".to_string()),
        ];
        check(
            schema.clone(),
            cols.clone(),
            Some(Row {
                schema,
                body: RefCell::new(RowBody::Unpacked(vec![
                    RowCol::Int(7),
                    RowCol::Bool(true),
                    RowCol::VarChar(RowVarChar {
                        inline: "0123456789".to_string(),
                        next_page: None,
                    }),
                ])),
            }),
        );
    }
}
