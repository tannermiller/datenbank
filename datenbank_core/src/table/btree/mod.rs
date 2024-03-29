use std::rc::Rc;

use crate::cache::{Cache, Error as CacheError};
use crate::pagestore::{Error as PageError, PageID, TablePageStore, TablePageStoreBuilder};
use crate::row::{Error as RowError, Predicate};
use crate::schema::{Column, Schema};
use node::{Internal, Leaf, Node, NodeBody};

mod encode;
mod insert;
pub(crate) mod node;

pub use encode::decode;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] PageError),
    #[error("attempted to insert duplicate entry with key {0:?}")]
    DuplicateEntry(Vec<u8>),
    #[error("empty table")]
    EmptyTable,
    #[error("cache error")]
    Cache(#[from] CacheError),
    #[error("row error")]
    Row(#[from] RowError),
}

// BTRee is a B+ tree that stores the data in a key value store.
#[derive(Debug)]
pub struct BTree<S: TablePageStore> {
    // the name of this tree, often its the table or index name
    pub(crate) name: Rc<String>,
    pub(crate) schema: Schema,
    // the order or branching factor of the B+ Tree
    pub(crate) order: usize,
    pub(crate) root: Option<PageID>,
    pub(crate) node_cache: Cache<S, Node>,
    pub(crate) data_cache: Cache<S, Vec<u8>>,
    pub(crate) store: S,
}

impl<S: TablePageStore> BTree<S> {
    // Build a brand new btree with no data in it.
    pub fn new<SB: TablePageStoreBuilder<PageStore = S>>(
        name: Rc<String>,
        schema: Schema,
        store_builder: &mut SB,
    ) -> Result<BTree<S>, Error> {
        // row size itself + 4 bytes for the body len
        let row_size = schema.max_inline_row_size() + 4;

        // 9 bytes of node-overhead + 4 bytes for # of rows + 4 bytes for right_sibling + row_body
        let store = store_builder.build()?;
        let order = (store.usable_page_size() - 17) / row_size;

        Ok(BTree {
            name,
            order,
            schema,
            root: None,
            node_cache: Cache::new(store_builder.build()?),
            data_cache: Cache::new(store_builder.build()?),
            store,
        })
    }

    pub fn scan(
        &mut self,
        columns: &[Rc<String>],
        rp: impl Predicate<S>,
    ) -> Result<Vec<Vec<Column>>, Error> {
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
                if rp.is_satisfied_by(&self.schema, &mut self.data_cache, row)? {
                    final_result.push(row.to_columns(
                        &mut self.data_cache,
                        &self.schema,
                        columns,
                    )?);
                }
            }

            leaf_id = match right_sibling {
                Some(page_id) => *page_id,
                None => break,
            };
        }
        Ok(final_result)
    }

    // find the leaf that either contains a key or should be the leaf into which the key would be
    // inserted
    fn find_containing_leaf(&mut self, key_id: &Vec<u8>) -> Result<(PageID, Vec<PageID>), Error> {
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

    pub fn lookup(
        &mut self,
        key: &Vec<u8>,
        columns: &[Rc<String>],
    ) -> Result<Option<Vec<Column>>, Error> {
        let (leaf_id, _) = self.find_containing_leaf(key)?;

        let node = self.node_cache.get(leaf_id)?;

        let rows = match &node.body {
            NodeBody::Internal(_) => unreachable!(),
            NodeBody::Leaf(Leaf { rows, .. }) => rows,
        };

        let i = match rows.binary_search_by_key(key, |r| r.key(&self.schema)) {
            Ok(i) => i,
            Err(_) => {
                return Ok(None);
            }
        };

        rows[i]
            .to_columns(&mut self.data_cache, &self.schema, columns)
            .map(Some)
            .map_err(Into::into)
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.node_cache.commit()?;
        self.data_cache.commit().map_err(Into::into)
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
    use super::*;
    use crate::pagestore::{MemoryManager, TablePageStoreBuilder, TablePageStoreManager};
    use crate::row::{Row, RowCol};
    use crate::schema::ColumnType;

    pub(crate) fn leaf_node(
        id: PageID,
        order: usize,
        row_data: Vec<Vec<RowCol>>,
        right_sibling: Option<PageID>,
    ) -> Node {
        Node {
            id,
            order,
            body: NodeBody::Leaf(Leaf {
                rows: row_data.into_iter().map(|body| Row { body }).collect(),
                right_sibling,
            }),
        }
    }

    pub(crate) fn internal_node(
        id: PageID,
        order: usize,
        boundary_keys: Vec<Vec<u8>>,
        children: Vec<PageID>,
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
    fn test_find_containing_leaf_and_lookup_root_is_leaf() {
        let schema = Schema::new(vec![("foo".into(), ColumnType::Int)], None, vec![]).unwrap();
        let mut store_builder = MemoryManager::new(64 * 1024).builder("test").unwrap();
        let data_cache = Cache::new(store_builder.build().unwrap());
        let mut node_cache = Cache::new(store_builder.build().unwrap());

        let root_id = node_cache.allocate().unwrap();
        node_cache
            .put(
                root_id,
                leaf_node(root_id, 10, vec![vec![RowCol::Int(1)]], None),
            )
            .unwrap();

        let mut btree = BTree {
            name: "test".to_string().into(),
            schema,
            order: 10,
            root: Some(root_id),
            node_cache,
            data_cache,
            store: store_builder.build().unwrap(),
        };

        let (leaf_id, parents) = btree.find_containing_leaf(&b"1".to_vec()).unwrap();

        assert_eq!(root_id, leaf_id);
        assert!(parents.is_empty());

        let all_cols = vec!["foo".to_string().into()];

        let missing = btree.lookup(&vec![0, 0, 0, 7], &all_cols).unwrap();
        assert_eq!(None, missing);

        let found = btree.lookup(&vec![0, 0, 0, 1], &all_cols).unwrap().unwrap();
        assert_eq!(vec![Column::Int(1)], found);
    }

    #[test]
    fn test_find_containing_leaf_and_lookup_root_is_internal() {
        let schema = Schema::new(vec![("foo".into(), ColumnType::Int)], None, vec![]).unwrap();
        let mut store_builder = MemoryManager::new(64 * 1024).builder("test").unwrap();
        let data_cache = Cache::new(store_builder.build().unwrap());
        let mut node_cache = Cache::new(store_builder.build().unwrap());

        let root_id = node_cache.allocate().unwrap();
        let left_id = node_cache.allocate().unwrap();
        let right_id = node_cache.allocate().unwrap();
        node_cache
            .put(
                root_id,
                internal_node(
                    root_id,
                    10,
                    vec![vec![0, 0, 0, 10]],
                    vec![left_id, right_id],
                ),
            )
            .unwrap();
        node_cache
            .put(
                left_id,
                leaf_node(
                    left_id,
                    10,
                    vec![vec![RowCol::Int(1)], vec![RowCol::Int(7)]],
                    None,
                ),
            )
            .unwrap();
        node_cache
            .put(
                right_id,
                leaf_node(
                    right_id,
                    10,
                    vec![vec![RowCol::Int(10)], vec![RowCol::Int(15)]],
                    None,
                ),
            )
            .unwrap();

        let mut btree = BTree {
            name: "test".to_string().into(),
            schema,
            order: 10,
            root: Some(root_id),
            node_cache,
            data_cache,
            store: store_builder.build().unwrap(),
        };

        let (leaf_id, parents) = btree.find_containing_leaf(&b"15".to_vec()).unwrap();

        assert_eq!(right_id, leaf_id);
        assert_eq!(vec![root_id], parents);

        let all_cols = vec!["foo".to_string().into()];

        let missing = btree.lookup(&vec![0, 0, 0, 9], &all_cols).unwrap();
        assert_eq!(None, missing);

        let found = btree.lookup(&vec![0, 0, 0, 1], &all_cols).unwrap().unwrap();
        assert_eq!(vec![Column::Int(1)], found);
        let found = btree.lookup(&vec![0, 0, 0, 7], &all_cols).unwrap().unwrap();
        assert_eq!(vec![Column::Int(7)], found);
        let found = btree
            .lookup(&vec![0, 0, 0, 10], &all_cols)
            .unwrap()
            .unwrap();
        assert_eq!(vec![Column::Int(10)], found);
        let found = btree
            .lookup(&vec![0, 0, 0, 15], &all_cols)
            .unwrap()
            .unwrap();
        assert_eq!(vec![Column::Int(15)], found);
    }
}
