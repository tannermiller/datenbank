use crate::pagestore::{Error as PageError, TablePageStore};
use crate::schema::{Column, Schema};
use cache::Cache;
use node::{Node, NodeBody};
use row::Row;

pub mod cache;
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
    pub(crate) node_cache: Cache<S, Node>,
    pub(crate) data_cache: Cache<S, Vec<u8>>,
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
            node_cache: Cache::new(store.clone()),
            data_cache: Cache::new(store.clone()),
            store,
        })
    }

    // Insert the row according to the key specificed by the btree schema. Returns the number of
    // rows affected. We currently only allow inserting fully specified rows so these must already
    // be put in order via schema.put_columns_in_order().
    pub fn insert(&mut self, values: Vec<Vec<Column>>) -> Result<usize, Error> {
        // first of all, handle an empty tree.
        let root_id = match self.root {
            None => {
                let root_id = self.store.allocate()?;
                self.root = Some(root_id);

                let root_node = Node::new_leaf(root_id, self.order);

                self.node_cache.put(root_id, root_node)?;
                root_id
            }
            Some(root_id) => root_id,
        };

        let mut count_affected = 0;
        for value in values {
            let row =
                row::process_columns(self.schema.clone(), self.store.usable_page_size(), value)?
                    .finalize(&mut self.data_cache)?;

            self.insert_row(root_id, row)?;

            count_affected += 1;
        }

        self.commit()?;

        Ok(count_affected)
    }

    fn insert_row(&mut self, root_id: usize, row: Row) -> Result<(), Error> {
        let root = self.node_cache.get_mut(root_id)?;

        if let Some(new_child) = root.insert_row(row)?.finalize(&mut self.node_cache)? {
            // if we get a new_child from this split then we've split the root and need to create a
            // new root node with the previous root and new child node as children.
            todo!()
        }

        Ok(())
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
