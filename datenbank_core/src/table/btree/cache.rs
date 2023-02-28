use std::collections::HashMap;

use super::{Error, Node};
use crate::pagestore::TablePageStore;

struct NodeWrapper {
    node: Node,
    was_mutated: bool,
}

impl NodeWrapper {
    fn new(node: Node) -> Self {
        NodeWrapper {
            node,
            was_mutated: false,
        }
    }
}

pub struct NodeCache<S: TablePageStore> {
    cache: HashMap<usize, NodeWrapper>,
    store: S,
}

impl<S: TablePageStore> NodeCache<S> {
    pub(crate) fn new(store: S) -> Self {
        NodeCache {
            cache: HashMap::new(),
            store,
        }
    }

    pub(crate) fn get_mut(&mut self, node_id: usize) -> Result<Option<&mut Node>, Error> {
        if let Some(wrapper) = self.cache.get_mut(&node_id) {
            wrapper.was_mutated = true;
            return Ok(Some(&mut wrapper.node));
        }

        let node = self.store.get(node_id)?;

        // TODO: need to deserialize node & node body
        //self.cache.insert(node_id, NodeWrapper::new(node));

        todo!()
    }
}

struct DataWrapper {
    data: Vec<u8>,
    was_mutated: bool,
}

impl DataWrapper {
    fn new(data: Vec<u8>) -> Self {
        DataWrapper {
            data,
            was_mutated: false,
        }
    }

    fn new_mutated(data: Vec<u8>) -> Self {
        DataWrapper {
            data,
            was_mutated: true,
        }
    }
}

pub struct DataCache<S: TablePageStore> {
    cache: HashMap<usize, DataWrapper>,
    store: S,
}

impl<S: TablePageStore> DataCache<S> {
    pub(crate) fn new(store: S) -> Self {
        DataCache {
            cache: HashMap::new(),
            store,
        }
    }

    pub(crate) fn put_new(&mut self, data: Vec<u8>) -> Result<usize, Error> {
        let data_id = self.store.allocate()?;

        self.cache.insert(data_id, DataWrapper::new_mutated(data));

        Ok(data_id)
    }

    pub(crate) fn page_size(&self) -> usize {
        self.store.usable_page_size()
    }
}
