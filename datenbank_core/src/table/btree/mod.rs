use crate::pagestore::{Error as PageError, TablePageStore};
use crate::schema::{Column, Schema};
use cache::Cache;
use node::{Internal, Node, NodeBody};
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
    #[error("empty table")]
    EmptyTable,
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

    // find the leaf that either contains a key or should be the leaf into which the key would be
    // inserted
    fn find_containing_leaf(&mut self, key_id: &String) -> Result<(usize, Vec<usize>), Error> {
        let mut node_id = match self.root {
            Some(root_id) => root_id,
            None => return Err(Error::EmptyTable),
        };
        let mut path = vec![];

        loop {
            let node = self.node_cache.get(node_id)?;

            match &node.body {
                NodeBody::Internal(Internal {
                    boundary_keys,
                    children,
                }) => {
                    path.push(node_id);
                    let key_idx = match boundary_keys.binary_search(key_id) {
                        // means we found it exactly, will be the first element in the
                        // children[i+1]
                        Ok(i) => i + 1,
                        // means we didn't find it exactly, but this i will be BEFORE the
                        // corresponding boundary key, so we want the child to the left of it
                        Err(i) => i,
                    };
                    node_id = children[key_idx];
                }
                NodeBody::Leaf(_) => return Ok((node_id, path)),
            }
        }
    }

    fn insert_row(&mut self, root_id: usize, row: Row) -> Result<(), Error> {
        // The insert algo goes like:
        //   * Search down the tree and find the node id of the leaf we need to insert the row
        //     into.
        //   * Insert the row in the leaf.
        //   * Iff the sibling splits, then recurse back up the parent chain inserting the
        //     children and splitting internal nodes.
        //   * If we walk all the way up the parent chain then we've split the root and must create
        //     a new root with the old root and the new sibling as its children.

        let (leaf_id, path) = self.find_containing_leaf(&row.key())?;

        let leaf_node = self.node_cache.get_mut(leaf_id)?;

        let row_outcome = match leaf_node.body {
            NodeBody::Internal(_) => unreachable!(),
            NodeBody::Leaf(ref mut leaf) => leaf.insert_row(self.order, row)?,
        };

        let body = match row_outcome {
            Some(body) => body,
            None => return Ok(()), // nothing more to do if we didn't split the leaf
        };

        // We only get past this point if we've split the leaf node and need to insert the new
        // sibling. left_child and the new_child_id will be the vars that we use to communicate up
        // each level of the internal nodes to indicate a newly inserted split node.
        let mut left_child = body.left_child();
        let mut new_child_id = self.node_cache.allocate()?;

        let split_node = Node {
            id: new_child_id,
            order: self.order,
            body,
        };
        self.node_cache.put(new_child_id, split_node)?;

        // now ensure the original leaf has the correct right_sibling set, to the new
        // split leaf
        match self.node_cache.get_mut(leaf_id)?.body {
            NodeBody::Internal(_) => unreachable!(),
            NodeBody::Leaf(ref mut leaf) => leaf.right_sibling = Some(new_child_id),
        }

        // finally, we insert the new leaf node in the parent internal node, if we happen to split
        // that internal node, then we need to recurse back up the internal node above that, etc

        for parent in path.into_iter().rev() {
            let parent_node = self.node_cache.get_mut(parent)?;

            let internal_node = match &mut parent_node.body {
                NodeBody::Internal(internal) => internal,
                NodeBody::Leaf(_) => unreachable!(),
            };

            let child_outcome = internal_node.insert_child(self.order, new_child_id, left_child)?;

            let body = match child_outcome {
                Some(body) => body,
                None => return Ok(()), // didn't split, we're done inserting
            };

            // update left_child and new_child_id and so that the next loop picks up this value and
            // inserts into that internal node
            left_child = body.left_child();
            new_child_id = self.node_cache.allocate()?;

            let split_node = Node {
                id: new_child_id,
                order: self.order,
                body,
            };
            self.node_cache.put(new_child_id, split_node)?;
        }

        // if we get here then we've split the root node and need a new one whose children are the
        // old root and the new sibling
        let new_root_id = self.node_cache.allocate()?;
        let new_root = Node {
            id: new_root_id,
            order: self.order,
            body: NodeBody::Internal(Internal {
                boundary_keys: vec![left_child],
                children: vec![root_id, new_child_id],
            }),
        };
        self.node_cache.put(new_root_id, new_root)?;

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

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use super::node::Leaf;
    use super::row::{RowBody, RowCol};
    use super::*;
    use crate::pagestore::Memory;
    use crate::schema::ColumnType;

    fn leaf_node(id: usize, order: usize, schema: Schema, row_data: Vec<RowCol>) -> Node {
        Node {
            id,
            order,
            body: NodeBody::Leaf(Leaf {
                rows: vec![Row {
                    schema,
                    body: RefCell::new(RowBody::Unpacked(row_data)),
                }],
                right_sibling: None,
            }),
        }
    }

    fn internal_node(
        id: usize,
        order: usize,
        boundary_keys: Vec<String>,
        children: Vec<usize>,
    ) -> Node {
        Node {
            id,
            order,
            body: NodeBody::Internal(Internal {
                boundary_keys,
                children,
            }),
        }
    }

    #[test]
    fn test_find_containing_leaf_root_is_leaf() {
        let schema = Schema::new(vec![("foo".into(), ColumnType::Int)]).unwrap();
        let store = Memory::new(64 * 1024);
        let data_cache = Cache::new(store.clone());
        let mut node_cache = Cache::new(store.clone());

        let root_id = node_cache.allocate().unwrap();
        node_cache
            .put(
                root_id,
                leaf_node(root_id, 10, schema.clone(), vec![RowCol::Int(1)]),
            )
            .unwrap();

        let mut btree = BTree {
            name: "test".to_string(),
            schema,
            order: 10,
            root: Some(root_id),
            node_cache,
            data_cache,
            store,
        };

        let (leaf_id, parents) = btree.find_containing_leaf(&"1".to_string()).unwrap();

        assert_eq!(root_id, leaf_id);
        assert!(parents.is_empty());
    }

    #[test]
    fn test_find_containing_leaf_root_is_internal() {
        let schema = Schema::new(vec![("foo".into(), ColumnType::Int)]).unwrap();
        let store = Memory::new(64 * 1024);
        let data_cache = Cache::new(store.clone());
        let mut node_cache = Cache::new(store.clone());

        let root_id = node_cache.allocate().unwrap();
        let left_id = node_cache.allocate().unwrap();
        let right_id = node_cache.allocate().unwrap();
        node_cache
            .put(
                root_id,
                internal_node(root_id, 10, vec!["10".to_string()], vec![left_id, right_id]),
            )
            .unwrap();
        node_cache
            .put(
                left_id,
                leaf_node(
                    left_id,
                    10,
                    schema.clone(),
                    vec![RowCol::Int(1), RowCol::Int(7)],
                ),
            )
            .unwrap();
        node_cache
            .put(
                right_id,
                leaf_node(
                    right_id,
                    10,
                    schema.clone(),
                    vec![RowCol::Int(10), RowCol::Int(15)],
                ),
            )
            .unwrap();

        let mut btree = BTree {
            name: "test".to_string(),
            schema,
            order: 10,
            root: Some(root_id),
            node_cache,
            data_cache,
            store,
        };

        let (leaf_id, parents) = btree.find_containing_leaf(&"15".to_string()).unwrap();

        assert_eq!(right_id, leaf_id);
        assert_eq!(vec![root_id], parents);
    }
}
