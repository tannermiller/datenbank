use std::collections::hash_map;
use std::collections::{HashMap, HashSet};
use std::mem;

use crate::pagestore::{Error as PageError, PageID, TablePageStore};

// TODO: move the management of the free list into here, it will go into the WAL when I implement
// that.

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] PageError),
    #[error("unrecoverable error: {0}")]
    UnrecoverableError(String),
}

pub trait Page: std::fmt::Debug + Clone {
    fn encode(&self) -> Vec<u8>;
    fn decode(data: &[u8]) -> Result<Self, Error>;
}

#[derive(Debug)]
struct Wrapper<P: Page> {
    data: P,
    was_mutated: bool,
}

impl<P: Page> Wrapper<P> {
    fn new(data: P) -> Self {
        Wrapper {
            data,
            was_mutated: false,
        }
    }

    fn new_mutated(data: P) -> Self {
        Wrapper {
            data,
            was_mutated: true,
        }
    }
}

#[derive(Debug)]
pub struct Cache<S: TablePageStore, P: Page> {
    cache: HashMap<PageID, Wrapper<P>>,
    store: S,

    // the allocated, but not inserted page ids
    allocated: HashSet<PageID>,
}

impl<S: TablePageStore, P: Page> Cache<S, P> {
    pub(crate) fn new(store: S) -> Self {
        Cache {
            cache: HashMap::new(),
            store,
            allocated: HashSet::new(),
        }
    }

    pub(crate) fn get_mut(&mut self, page_id: PageID) -> Result<&mut P, Error> {
        if let hash_map::Entry::Vacant(e) = self.cache.entry(page_id) {
            let page_data = self.store.get(page_id)?;
            let page = P::decode(&page_data)?;

            e.insert(Wrapper::new_mutated(page));
        };

        let wrapper = self
            .cache
            .get_mut(&page_id)
            .ok_or(Error::Io(PageError::UnallocatedPage(page_id)))?; // shouldn't happen
                                                                     //
        wrapper.was_mutated = true;

        Ok(&mut wrapper.data)
    }

    pub(crate) fn allocate(&mut self) -> Result<PageID, Error> {
        let mut alloc_state = self.store.allocation_state()?;

        let page_id = if let Some(page_id) = alloc_state.free_list.pop() {
            self.store.remove_from_free_list(page_id)?;
            page_id
        } else {
            self.store.allocate_new()?
        };

        self.allocated.insert(page_id);
        Ok(page_id)
    }

    pub(crate) fn put(&mut self, page_id: PageID, data: P) -> Result<(), Error> {
        self.allocated.remove(&page_id);
        self.cache.insert(page_id, Wrapper::new_mutated(data));
        Ok(())
    }

    pub(crate) fn get(&mut self, page_id: PageID) -> Result<&P, Error> {
        if let hash_map::Entry::Vacant(e) = self.cache.entry(page_id) {
            let data = self.store.get(page_id)?;
            let page = P::decode(&data)?;
            e.insert(Wrapper::new(page));
        };

        self.cache
            .get(&page_id)
            .map(|w| &w.data)
            .ok_or(Error::Io(PageError::UnallocatedPage(page_id))) // shouldn't happen
    }

    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        let old_cache = mem::take(&mut self.cache);

        for (page_id, wrapper) in old_cache {
            if wrapper.was_mutated {
                self.store.put(page_id, wrapper.data.encode())?;
            }
        }

        // delete the allocated but unused pages, so that they can be reused
        for unused_page in mem::take(&mut self.allocated) {
            self.store.add_to_free_list(unused_page)?;
        }

        Ok(())
    }
}

impl Page for Vec<u8> {
    fn encode(&self) -> Vec<u8> {
        self.clone()
    }

    fn decode(data: &[u8]) -> Result<Self, Error> {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod test {
    use crate::pagestore::{Memory, MemoryManager, TablePageStoreBuilder, TablePageStoreManager};

    use super::*;

    #[test]
    fn test_cache() {
        let mut store_builder = MemoryManager::new(64 * 1024).builder("test").unwrap();
        let mut cache: Cache<Memory, Vec<u8>> = Cache::new(store_builder.build().unwrap());

        assert!(cache.get(1u32.into()).is_err());

        let first_page = cache.allocate().unwrap();
        assert_eq!(<u32 as std::convert::Into<PageID>>::into(1u32), first_page);
        assert!(cache.allocated.contains(&first_page));

        cache
            .put(first_page, "Hello, World!".as_bytes().to_vec())
            .unwrap();
        assert!(!cache.allocated.contains(&first_page));

        let mut store = store_builder.build().unwrap();
        assert_eq!("Hello, World!".as_bytes(), cache.get(first_page).unwrap());
        assert_eq!(Vec::<u8>::new(), store.get(first_page).unwrap());

        cache.get_mut(first_page).unwrap().push(b'?');
        assert_eq!("Hello, World!?".as_bytes(), cache.get(first_page).unwrap());

        cache.commit().unwrap();

        assert_eq!(0, cache.cache.len());
        assert_eq!(0, cache.allocated.len());

        assert_eq!("Hello, World!?".as_bytes(), store.get(first_page).unwrap());
    }
}
