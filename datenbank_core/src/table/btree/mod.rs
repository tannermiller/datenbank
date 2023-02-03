use std::io::Write;

use crate::pagestore::{Error as PageError, TablePageStore};
use crate::schema::Schema;
use row::Row;

mod row;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] PageError),
    #[error("attempted to insert duplicate entry with key {0}")]
    DuplicateEntry(String),
}

// TODO: The btree needs to know the page size so it can manage and size its nodes appropriately.
// TODO: Probably need to implement this a bit more to understand what that at-rest format looks
// like before I can determine what how exactly that works.

// BTRee is a B+ tree that stores the data in a key value store.
pub struct BTree<S: TablePageStore> {
    // the name of this tree, often its the table or index name
    name: String,
    schema: Schema,
    // the order or branching factor of the B+ Tree
    order: usize,
    root: Option<Node>,
    node_cache: Vec<Node>,
    store: S,
}

impl<S: TablePageStore> BTree<S> {
    // Build a brand new btree with no data in it.
    pub fn new(name: String, schema: Schema, store: S) -> Result<BTree<S>, Error> {
        // TODO: Should this write something to the store? I don't know what, there's no root yet.
        // But we probably need a top-level metadata struct (which is BTree itself, probably).
        // Yes, this will write to the store, a header in the 0th page. What is in that header and
        // how its serialized is TBD.
        let order = store.usable_page_size() / 1000; // TODO: this magic number should be figured
                                                     // out and pased on how big each node will be.
        Ok(BTree {
            name,
            order,
            schema,
            root: None,
            node_cache: Vec::new(),
            store,
        })
    }

    // Load a btree that has already persisted data.
    pub fn load(id: &str, store: S) -> Result<BTree<S>, Error> {
        // TODO: Load the root info from the store and hydrate the tree
        todo!()
    }

    // Insert the row according to the key specificed by the btree schema.
    pub fn insert(&mut self, row: Row) -> Result<(), Error> {
        todo!()
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        todo!()
    }

    // Do a direct key lookup of the row.
    pub fn query_key(&self, key: &str) -> Result<Row, Error> {
        todo!()
    }

    // Query for every row whose key matches the key prefix and apply the provided function to the
    // row.
    pub fn query_prefix<F: Fn(&Row)>(&self, key_prefix: &str, f: F) -> Result<(), Error> {
        todo!()
    }

    // Perform a full table scan and pass every row into the provided function.
    pub fn scan<F: Fn(&Row)>(&self, f: F) -> Result<(), Error> {
        todo!()
    }

    // The encoding for a BTree is just:
    //   - the order encoded as 4 byte integer
    //   - the root node's page id as a 4 byte integer
    //     - if the root node has not been initialied, then a value of 0 will be stored here
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8);
        bytes
            .write_all(&(self.order as u32).to_be_bytes())
            .expect("can't fail writing to vec");
        if let Some(root) = &self.root {
            bytes
                .write_all(&(root.id as u32).to_be_bytes())
                .expect("can't fail writing to vec");
        } else {
            bytes
                .write_all(&0u32.to_be_bytes())
                .expect("can't fail writing to vec");
        }
        bytes
    }
}

// This represents a single node in the B+ Tree, it contains the metadata of the node as well as
// the node body itself.
struct Node {
    // The ID of this node acts as the key we use to store it with.
    id: usize,
    // the order or branching factor of this B+ Tree
    order: usize,
    body: NodeBody,
}

// This represents which type this node is and contains the type-specific data for each one.
enum NodeBody {
    // Internal nodes contain the boundary keys (first of each next child) and the pointers to the
    // child nodes. No rows are stored here.
    Internal {
        boundary_keys: Vec<String>, // TODO: should these be Vec<u8>?
        children: Vec<String>,
    },
    // Leaf nodes contain the keys and rows that correspond to them.
    Leaf {
        // the actual row data for this leaf node
        rows: Vec<Row>,
        // the leaf node to the right of this one, used for table scans
        right_sibling: Option<String>,
    },
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pagestore::Memory;

    #[test]
    fn test_encode() {
        let empty_tree = BTree {
            name: "something".to_string(),
            schema: Schema::new(vec![]).unwrap(),
            order: 1000,
            root: None,
            node_cache: vec![],
            store: Memory::new(64 * 1024),
        };
        assert_eq!(vec![0, 0, 3, 232, 0, 0, 0, 0], empty_tree.encode());

        let filled_tree = BTree {
            name: "something".to_string(),
            schema: Schema::new(vec![]).unwrap(),
            order: 1000,
            root: Some(Node {
                id: 7,
                order: 1000,
                body: NodeBody::Leaf {
                    rows: vec![],
                    right_sibling: None,
                },
            }),
            node_cache: vec![],
            store: Memory::new(64 * 1024),
        };
        assert_eq!(vec![0, 0, 3, 232, 0, 0, 0, 7], filled_tree.encode());
    }
}
