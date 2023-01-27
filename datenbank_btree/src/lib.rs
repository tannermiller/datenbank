use datenbank_kv::{Error as KvError, KeyValueStore};
pub use schema::Schema;

mod schema;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] KvError),
    #[error("attempted to insert duplicate entry with key {0}")]
    DuplicateEntry(String),
    #[error("column names must be unique, found duplicate {0}")]
    NonUniqueColumn(String),
}

// BTRee is a B+ tree that stores the data in a key value store.
pub struct BTree<S: KeyValueStore> {
    // the name of this tree, often its the table or index name
    id: String,
    schema: Schema,
    // the order or branching factor of the B+ Tree
    order: usize,
    root: Option<Node>,
    node_cache: Vec<Node>,
    store: S,
}

impl<S: KeyValueStore> BTree<S> {
    // Build a brand new btree with no data in it.
    pub fn new(id: String, order: usize, schema: Schema, store: S) -> BTree<S> {
        // TODO: Should this write something to the store? I don't know what, there's no root yet.
        // But we probably need a top-level metadata struct (which is BTree itself, probably).
        BTree {
            id: id.clone(),
            order,
            schema,
            root: None,
            node_cache: Vec::new(),
            store,
        }
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
}

// This represents a single node in the B+ Tree, it contains the metadata of the node as well as
// the node body itself.
struct Node {
    // TODO: What are these ids? UUIDs? "{BTree.id}_{some_number_count}"? just some integer?
    // The ID of this node acts as the key we use to store it with.
    id: String,
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

// This holds a single row's worth of data.
pub struct Row {
    schema: Schema,
    data: Vec<u8>,
}

impl Row {
    fn new(schema: Schema, cols: Vec<Column>) -> Self {
        // TODO: Validate columns match schema
        // pack column data into single Vec<u8>
        todo!()
    }
}

pub enum Column {
    // VarChar is variable length string with max length of 65,535.
    VarChar(String),
    // Int is a signed integer with max value of 2,147,483,647.
    Int(i32),
    // Bool is a boolean value.
    Bool(bool),
}