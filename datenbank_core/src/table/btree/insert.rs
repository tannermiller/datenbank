use super::node::{Internal, Leaf, Node, NodeBody};
use super::{BTree, Error};
use crate::pagestore::TablePageStore;
use crate::row::{self, Row};
use crate::schema::Column;

impl<S: TablePageStore> BTree<S> {
    // Insert the row according to the key specificed by the btree schema. Returns the number of
    // rows affected. We currently only allow inserting fully specified rows so these must already
    // be put in order via schema.put_columns_in_order().
    pub fn insert(&mut self, values: Vec<Vec<Column>>) -> Result<(Vec<Row>, bool), Error> {
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

        let mut inserted_rows = Vec::with_capacity(values.len());
        for value in values {
            // process the row to ensure it will fit in the leaf node, and split out any overflow
            // and store those in the data cache.
            let row = row::process_columns(self.store.usable_page_size(), value)?
                .finalize(&mut self.data_cache)?;

            // TODO: Can I make the rows Rc's so I can both insert and return it?
            //       This clone is not ideal.
            inserted_rows.push(row.clone());

            if let Some(new_root_id) = self.insert_row(root_id, row)? {
                // if we split the root node then we need set the root id for the next iteration to
                // be this new_root_id.
                root_id = new_root_id;
                changed_root = true;
                self.root = Some(root_id);
            }
        }

        // TODO: Should this commit be here, at the tree level? or should it be at the table level?
        // or should it be even higher than that?
        // We'll probably share the node_cache across all btrees/tables/indices so we can probably
        // just cache that once.
        // we're writing the table header up a level, so we should probably write there too
        self.commit()?;

        Ok((inserted_rows, changed_root))
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

        let (leaf_id, path) = self.find_containing_leaf(&row.key(&self.schema))?;

        let leaf_node = self.node_cache.get_mut(leaf_id)?;

        let row_outcome = match leaf_node.body {
            NodeBody::Internal(_) => unreachable!(),
            NodeBody::Leaf(ref mut leaf) => leaf.insert_row(&self.schema, self.order, row)?,
        };

        let body = match row_outcome {
            Some(body) => body,
            None => return Ok(None), // nothing more to do if we didn't split the leaf
        };

        // We only get past this point if we've split the leaf node and need to insert the new
        // sibling. new_child_id will be the var that we use to communicate up each level of the
        // internal nodes to indicate a newly inserted split node.
        let mut new_child_key = match &body {
            NodeBody::Leaf(Leaf { rows, .. }) => rows[0].key(&self.schema),
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
                internal_node.insert_child(self.order, new_child_id, new_child_key.clone())?;

            let (child_key, body) = match child_outcome {
                Some(out) => out,
                None => return Ok(None), // didn't split, we're done inserting
            };

            // update new_child_id and so that the next loop picks up this value and inserts into
            // that internal node
            new_child_id = self.node_cache.allocate()?;
            new_child_key = child_key;

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
                boundary_keys: vec![new_child_key],
                children: vec![root_id, new_child_id],
            }),
        };
        self.node_cache.put(new_root_id, new_root)?;

        Ok(Some(new_root_id))
    }
}

#[cfg(test)]
mod test {
    use super::super::test::{internal_node, leaf_node};
    use super::super::Cache;
    use super::row::RowCol;
    use super::*;
    use crate::pagestore::{MemoryManager, TablePageStoreBuilder, TablePageStoreManager};
    use crate::schema::{ColumnType, Schema};

    fn assert_node<S: TablePageStore>(node: &Node, node_cache: &mut Cache<S, Node>) {
        let loaded_node = node_cache.get(node.id).unwrap();
        assert_eq!(node, loaded_node,);
    }

    #[test]
    fn test_insert_across_split() {
        let schema = Schema::new(vec![("foo".into(), ColumnType::Int)], None, vec![]).unwrap();
        let table_name = "test";
        let mut store_builder = MemoryManager::new(64 * 1024).builder(&table_name).unwrap();
        let order = 2;
        let data_cache = Cache::new(store_builder.build().unwrap());
        let mut node_cache = Cache::new(store_builder.build().unwrap());

        let orig_root_id = node_cache.allocate().unwrap();
        node_cache
            .put(
                orig_root_id,
                leaf_node(orig_root_id, order, vec![vec![RowCol::Int(0)]], None),
            )
            .unwrap();

        let mut btree = BTree {
            name: table_name.to_string(),
            schema,
            order,
            root: Some(orig_root_id),
            node_cache,
            data_cache,
            store: store_builder.build().unwrap(),
        };

        let to_insert = vec![
            vec![Column::Int(1)],
            vec![Column::Int(2)],
            vec![Column::Int(3)],
        ];
        let (inserted, root_changed) = btree.insert(to_insert).unwrap();
        assert_eq!(3, inserted.len());
        assert!(root_changed);

        let new_root_id = btree.root.unwrap();
        assert!(orig_root_id != new_root_id);
        let mut node_cache: Cache<_, Node> = Cache::new(store_builder.build().unwrap());
        assert_node(
            &internal_node(new_root_id, 2, vec![vec![0, 0, 0, 2]], vec![1, 2]),
            &mut node_cache,
        );

        let left_id = 1;
        let right_id = 2;
        assert_node(
            &leaf_node(
                left_id,
                2,
                vec![vec![RowCol::Int(0)], vec![RowCol::Int(1)]],
                Some(right_id),
            ),
            &mut node_cache,
        );
        assert_node(
            &leaf_node(
                right_id,
                2,
                vec![vec![RowCol::Int(2)], vec![RowCol::Int(3)]],
                None,
            ),
            &mut node_cache,
        );

        let (inserted, root_changed) = btree.insert(vec![vec![Column::Int(4)]]).unwrap();
        assert_eq!(1, inserted.len());
        assert!(root_changed);
        let mut node_cache: Cache<_, Node> = Cache::new(store_builder.build().unwrap());

        let next_new_root_id = btree.root.unwrap();
        assert!(new_root_id != next_new_root_id);
        assert_node(
            &internal_node(next_new_root_id, 2, vec![vec![0, 0, 0, 4]], vec![3, 5]),
            &mut node_cache,
        );

        assert_node(
            &internal_node(3, 2, vec![vec![0, 0, 0, 2]], vec![1, 2]),
            &mut node_cache,
        );

        assert_node(
            &leaf_node(
                1,
                2,
                vec![vec![RowCol::Int(0)], vec![RowCol::Int(1)]],
                Some(2),
            ),
            &mut node_cache,
        );
        assert_node(
            &leaf_node(
                2,
                2,
                vec![vec![RowCol::Int(2)], vec![RowCol::Int(3)]],
                Some(4),
            ),
            &mut node_cache,
        );

        assert_node(&internal_node(5, 2, vec![], vec![4]), &mut node_cache);

        assert_node(
            &leaf_node(4, 2, vec![vec![RowCol::Int(4)]], None),
            &mut node_cache,
        );
    }
}
