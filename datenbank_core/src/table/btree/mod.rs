use std::collections::HashMap;

use crate::pagestore::{Error as PageError, TablePageStore};
use crate::schema::{Column, Schema};
use node::Node;
use row::Row;

mod cache;
mod encode;
mod node;
mod row;

pub use encode::decode;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] PageError),
    #[error("attempted to insert duplicate entry with key {0}")]
    DuplicateEntry(String),
    #[error("invalid column: {0}")]
    InvalidColumn(String),
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
    pub(crate) node_cache: HashMap<usize, Node>,
    pub(crate) data_cache: HashMap<usize, Vec<u8>>,
    pub(crate) store: S,
}

impl<S: TablePageStore> BTree<S> {
    // Build a brand new btree with no data in it.
    pub fn new(name: String, schema: Schema, store: S) -> Result<BTree<S>, Error> {
        // TODO: There's going to be some other per-node overhead that needs to be factored in
        // here.
        let row_size = schema.max_inline_row_size();
        let order = store.usable_page_size() / row_size;

        Ok(BTree {
            name,
            order,
            schema,
            root: None,
            node_cache: HashMap::new(),
            data_cache: HashMap::new(),
            store,
        })
    }

    // Insert the row according to the key specificed by the btree schema. Returns the number of
    // rows affected. We currently only allow inserting fully specified rows so these must already
    // be put in order via schema.put_columns_in_order().
    pub fn insert(&mut self, values: Vec<Vec<Column>>) -> Result<usize, Error> {
        // first of all, handle an empty tree.
        if self.root.is_none() {
            let root_id = self.store.allocate()?;
            self.root = Some(root_id);

            let root_node = Node::new_leaf(root_id, self.order);

            self.node_cache.insert(root_id, root_node);
        }

        for value in values {
            let row = Row::from_columns(self.schema.clone(), value);
            // TODO: convert to row::Row and do the allocation of outside pages as part of it
        }

        todo!()
    }

    // The encoding for a BTree is just:
    //   - the order encoded as 4 byte integer
    //   - the root node's page id as a 4 byte integer
    //     - if the root node has not been initialied, then a value of 0 will be stored here
    pub fn encode(&self) -> Vec<u8> {
        encode::encode(self)
    }
}
