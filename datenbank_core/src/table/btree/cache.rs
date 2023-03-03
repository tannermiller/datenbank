use std::collections::{HashMap, HashSet};

use super::{Error, Node};
use crate::pagestore::{Error as PageError, TablePageStore};

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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct DataCache<S: TablePageStore> {
    cache: HashMap<usize, DataWrapper>,
    store: S,

    // the allocated, but not inserted page ids
    allocated: HashSet<usize>,
}

impl<S: TablePageStore> DataCache<S> {
    pub(crate) fn new(store: S) -> Self {
        DataCache {
            cache: HashMap::new(),
            store,
            allocated: HashSet::new(),
        }
    }

    pub(crate) fn allocate(&mut self) -> Result<usize, Error> {
        let page_id = self.store.allocate()?;
        self.allocated.insert(page_id);
        Ok(page_id)
    }

    pub(crate) fn put(&mut self, page_id: usize, data: Vec<u8>) -> Result<(), Error> {
        self.allocated.remove(&page_id);
        self.cache.insert(page_id, DataWrapper::new_mutated(data));
        Ok(())
    }

    pub(crate) fn get(&mut self, page_id: usize) -> Result<Vec<u8>, Error> {
        let page = self.cache.get(&page_id);
        match page {
            Some(v) => Ok(v.data.clone()),
            None => Err(Error::Io(PageError::UnallocatedPage(page_id))),
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.store.usable_page_size()
    }
}
