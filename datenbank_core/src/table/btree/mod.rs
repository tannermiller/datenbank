use crate::pagestore::{Error as PageError, TablePageStore, TablePageStoreBuilder};
use crate::schema::{Column, Schema};
use cache::Cache;
use node::{Internal, Leaf, Node, NodeBody};
use row::RowCol;

pub mod cache;
mod encode;
mod insert;
pub(crate) mod node;
pub(crate) mod row;

pub use encode::decode;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] PageError),
    #[error("attempted to insert duplicate entry with key {0:?}")]
    DuplicateEntry(Vec<u8>),
    #[error("invalid column: {0}")]
    InvalidColumn(String),
    #[error("empty table")]
    EmptyTable,
    #[error("unrecoverable error: {0}")]
    UnrecoverableError(String),
}

// BTRee is a B+ tree that stores the data in a key value store.
#[derive(Debug)]
pub struct BTree<S: TablePageStore> {
    // the name of this tree, often its the table or index name
    pub(crate) name: String,
    pub(crate) schema: Schema,
    // the order or branching factor of the B+ Tree
    pub(crate) order: usize,
    pub(crate) root: Option<usize>,
    pub(crate) node_cache: Cache<S, Node>,
    pub(crate) data_cache: Cache<S, Vec<u8>>,
    pub(crate) store: S,
}

impl<S: TablePageStore> BTree<S> {
    // Build a brand new btree with no data in it.
    pub fn new<SB: TablePageStoreBuilder<TablePageStore = S>>(
        name: String,
        schema: Schema,
        store_builder: &mut SB,
    ) -> Result<BTree<S>, Error> {
        // row size itself + 4 bytes for the body len
        let row_size = schema.max_inline_row_size() + 4;

        // 9 bytes of node-overhead + 4 bytes for # of rows + 4 bytes for right_sibling + row_body
        let store = store_builder.build(&name)?;
        let order = (store.usable_page_size() - 17) / row_size;

        Ok(BTree {
            name: name.clone(),
            order,
            schema,
            root: None,
            node_cache: Cache::new(store_builder.build(&name)?),
            data_cache: Cache::new(store_builder.build(&name)?),
            store,
        })
    }

    pub fn scan(&mut self, columns: Vec<String>) -> Result<Vec<Vec<Column>>, Error> {
        let root_id = match self.root {
            None => return Ok(vec![]),
            Some(root_id) => root_id,
        };

        let mut node = self.node_cache.get(root_id)?;

        let mut leaf_id = loop {
            let child_page_id = match &node.body {
                NodeBody::Leaf(_) => break node.id,
                NodeBody::Internal(Internal { children, .. }) => children[0],
            };
            node = self.node_cache.get(child_page_id)?;
        };

        let mut final_result = Vec::with_capacity(self.order);
        loop {
            let node = self.node_cache.get(leaf_id)?;

            let (rows, right_sibling) = match &node.body {
                NodeBody::Leaf(Leaf {
                    rows,
                    right_sibling,
                }) => (rows, right_sibling),
                NodeBody::Internal(_) => unreachable!(),
            };

            for row in rows {
                let mut row_values = Vec::with_capacity(columns.len());

                for (col, (schema_col, _)) in row.body.iter().zip(self.schema.columns()) {
                    if !columns.contains(schema_col) {
                        continue;
                    }

                    let val = match col {
                        RowCol::Int(i) => Column::Int(*i),
                        RowCol::Bool(b) => Column::Bool(*b),
                        RowCol::VarChar(vc) => {
                            let mut vc_data = vc.inline.to_vec();

                            if let Some(data_page_id) = vc.next_page {
                                let mut data_page_id = data_page_id;
                                loop {
                                    let vc_page = self.data_cache.get(data_page_id)?;

                                    if vc_page.len() < 4 {
                                        break;
                                    }

                                    let next_pointer =
                                        u32::from_be_bytes(vc_page[..4].try_into().map_err(
                                            |e: std::array::TryFromSliceError| {
                                                Error::InvalidColumn(e.to_string())
                                            },
                                        )?);

                                    vc_data.extend(&vc_page[4..]);

                                    if next_pointer != 0 {
                                        data_page_id = next_pointer as usize;
                                    } else {
                                        break;
                                    }
                                }
                            }

                            match String::from_utf8(vc_data) {
                                Ok(s) => Column::VarChar(s),
                                Err(e) => {
                                    return Err(Error::InvalidColumn(e.to_string()));
                                }
                            }
                        }
                    };
                    row_values.push(val);
                }

                final_result.push(row_values);
            }

            leaf_id = match right_sibling {
                Some(page_id) => *page_id,
                None => break,
            };
        }
        Ok(final_result)
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.node_cache.commit()?;
        self.data_cache.commit()
    }

    // The encoding for a BTree is just:
    //   - the order encoded as 4 byte integer
    //   - the root node's page id as a 4 byte integer
    //     - if the root node has not been initialied, then a value of 0 will be stored here
    pub fn encode(&self) -> Vec<u8> {
        encode::encode(self)
    }
}
