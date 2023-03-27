use crate::pagestore::{Error as PageError, TablePageStore, TablePageStoreBuilder};
use crate::schema::{Column, Schema};
use cache::Cache;
use node::{Internal, Leaf, Node, NodeBody};
use row::{Row, RowCol};

pub mod cache;
mod encode;
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

    // Insert the row according to the key specificed by the btree schema. Returns the number of
    // rows affected. We currently only allow inserting fully specified rows so these must already
    // be put in order via schema.put_columns_in_order().
    pub fn insert(&mut self, values: Vec<Vec<Column>>) -> Result<(usize, bool), Error> {
        let mut changed_root = false;

        // first of all, handle an empty tree.
        let mut root_id = match self.root {
            None => {
                changed_root = true;
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
            // process the row to ensure it will fit in the leaf node, and split out any overflow
            // and store those in the data cache.
            let row = row::process_columns(self.store.usable_page_size(), value)?
                .finalize(&mut self.data_cache)?;

            if let Some(new_root_id) = self.insert_row(root_id, row)? {
                // if we split the root node then we need set the root id for the next iteration to
                // be this new_root_id.
                root_id = new_root_id;
            }

            count_affected += 1;
        }

        // TODO: Should this commit be here, at the tree level? or should it be at the table level?
        // or should it be even higher than that?
        // We'll probably share the node_cache across all btrees/tables/indices so we can probably
        // just cache that once.
        // we're writing the table header up a level, so we should probably write there too
        self.commit()?;

        if root_id != self.root.unwrap() {
            changed_root = true;
            self.root = Some(root_id);
        };

        Ok((count_affected, changed_root))
    }

    // find the leaf that either contains a key or should be the leaf into which the key would be
    // inserted
    fn find_containing_leaf(&mut self, key_id: &Vec<u8>) -> Result<(usize, Vec<usize>), Error> {
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

    fn insert_row(&mut self, root_id: usize, row: Row) -> Result<Option<usize>, Error> {
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
            None => return Ok(None), // nothing more to do if we didn't split the leaf
        };

        // We only get past this point if we've split the leaf node and need to insert the new
        // sibling. new_child_id will be the var that we use to communicate up each level of the
        // internal nodes to indicate a newly inserted split node.
        let left_child = match &body {
            NodeBody::Leaf(Leaf { rows, .. }) => rows[0].key(),
            _ => unreachable!(), // We know this is a leaf since we just split a leaf
        };
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

            let child_outcome =
                internal_node.insert_child(self.order, new_child_id, left_child.clone())?;

            let body = match child_outcome {
                Some(body) => body,
                None => return Ok(None), // didn't split, we're done inserting
            };

            // update new_child_id and so that the next loop picks up this value and inserts into
            // that internal node
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

        Ok(Some(new_root_id))
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

#[cfg(test)]
mod test {
    use super::node::Leaf;
    use super::row::RowCol;
    use super::*;
    use crate::pagestore::MemoryBuilder;
    use crate::schema::ColumnType;

    fn leaf_node(id: usize, order: usize, row_data: Vec<RowCol>) -> Node {
        Node {
            id,
            order,
            body: NodeBody::Leaf(Leaf {
                rows: vec![Row { body: row_data }],
                right_sibling: None,
            }),
        }
    }

    fn internal_node(
        id: usize,
        order: usize,
        boundary_keys: Vec<Vec<u8>>,
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
        let mut store_builder = MemoryBuilder::new(64 * 1024);
        let data_cache = Cache::new(store_builder.build("test").unwrap());
        let mut node_cache = Cache::new(store_builder.build("test").unwrap());

        let root_id = node_cache.allocate().unwrap();
        node_cache
            .put(root_id, leaf_node(root_id, 10, vec![RowCol::Int(1)]))
            .unwrap();

        let mut btree = BTree {
            name: "test".to_string(),
            schema,
            order: 10,
            root: Some(root_id),
            node_cache,
            data_cache,
            store: store_builder.build("test").unwrap(),
        };

        let (leaf_id, parents) = btree.find_containing_leaf(&b"1".to_vec()).unwrap();

        assert_eq!(root_id, leaf_id);
        assert!(parents.is_empty());
    }

    #[test]
    fn test_find_containing_leaf_root_is_internal() {
        let schema = Schema::new(vec![("foo".into(), ColumnType::Int)]).unwrap();
        let mut store_builder = MemoryBuilder::new(64 * 1024);
        let data_cache = Cache::new(store_builder.build("test").unwrap());
        let mut node_cache = Cache::new(store_builder.build("test").unwrap());

        let root_id = node_cache.allocate().unwrap();
        let left_id = node_cache.allocate().unwrap();
        let right_id = node_cache.allocate().unwrap();
        node_cache
            .put(
                root_id,
                internal_node(root_id, 10, vec![b"10".to_vec()], vec![left_id, right_id]),
            )
            .unwrap();
        node_cache
            .put(
                left_id,
                leaf_node(left_id, 10, vec![RowCol::Int(1), RowCol::Int(7)]),
            )
            .unwrap();
        node_cache
            .put(
                right_id,
                leaf_node(right_id, 10, vec![RowCol::Int(10), RowCol::Int(15)]),
            )
            .unwrap();

        let mut btree = BTree {
            name: "test".to_string(),
            schema,
            order: 10,
            root: Some(root_id),
            node_cache,
            data_cache,
            store: store_builder.build("test").unwrap(),
        };

        let (leaf_id, parents) = btree.find_containing_leaf(&b"15".to_vec()).unwrap();

        assert_eq!(right_id, leaf_id);
        assert_eq!(vec![root_id], parents);
    }
}
