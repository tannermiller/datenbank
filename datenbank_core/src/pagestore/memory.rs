use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use super::{
    AllocationState, Error, PageID, TablePageStore, TablePageStoreBuilder, TablePageStoreManager,
};

#[derive(Debug)]
pub struct Memory {
    inner: Rc<RefCell<Inner>>,
}

// use an inner struct with all the functionality so we can support Clone on the TablePageStore
// implementation.
#[derive(Debug)]
struct Inner {
    page_size: usize,
    last_page: PageID,
    store: HashMap<PageID, Vec<u8>>,
    free_list: VecDeque<PageID>,
}

impl Inner {
    fn allocate(&mut self) -> Result<PageID, Error> {
        match self.free_list.pop_front() {
            Some(free_id) => Ok(free_id),
            None => {
                // we've got to "allocate" the 0th page for table stuff, so the first id we return
                // from here is at least 1
                self.last_page.0 += 1;
                Ok(self.last_page)
            }
        }
    }

    fn allocation_state(&mut self) -> Result<AllocationState, Error> {
        Ok(AllocationState {
            maximum_id: self.last_page,
            free_list: self.free_list.iter().cloned().collect(),
        })
    }

    fn usable_page_size(&self) -> usize {
        // we don't need any header to manage pages, so we can use the whole page
        self.page_size
    }

    fn get(&mut self, page_id: PageID) -> Result<Vec<u8>, Error> {
        match self.store.get(&page_id) {
            Some(payload) => Ok(payload.clone()),
            // 0th page is auto "allocated" and anything thats under the last page has probably
            // just been returned from the free list
            None if page_id.0 == 0 || page_id.0 <= self.last_page.0 => Ok(vec![]),
            None => Err(Error::UnallocatedPage(page_id)),
        }
    }

    fn put(&mut self, page_id: PageID, payload: Vec<u8>) -> Result<(), Error> {
        if page_id > self.last_page {
            return Err(Error::UnallocatedPage(page_id));
        }

        if payload.len() > self.page_size {
            return Err(Error::PayloadTooLong(payload.len(), self.page_size));
        }

        self.store.insert(page_id, payload);

        Ok(())
    }

    fn delete(&mut self, page_id: PageID) -> Result<(), Error> {
        if page_id > self.last_page {
            return Err(Error::UnallocatedPage(page_id));
        }

        self.store.remove(&page_id);
        self.free_list.push_back(page_id);

        Ok(())
    }
}

impl Memory {
    pub fn new(page_size: usize) -> Self {
        Memory {
            inner: Rc::new(RefCell::new(Inner {
                page_size,
                last_page: PageID(0),
                store: HashMap::new(),
                free_list: VecDeque::new(),
            })),
        }
    }
}

impl TablePageStore for Memory {
    fn allocate(&mut self) -> Result<PageID, Error> {
        self.inner.borrow_mut().allocate()
    }

    fn allocation_state(&mut self) -> Result<AllocationState, Error> {
        self.inner.borrow_mut().allocation_state()
    }

    fn usable_page_size(&self) -> usize {
        self.inner.borrow().usable_page_size()
    }

    fn get(&mut self, page_id: PageID) -> Result<Vec<u8>, Error> {
        self.inner.borrow_mut().get(page_id)
    }

    fn put(&mut self, page_id: PageID, payload: Vec<u8>) -> Result<(), Error> {
        self.inner.borrow_mut().put(page_id, payload)
    }

    fn delete(&mut self, page_id: PageID) -> Result<(), Error> {
        self.inner.borrow_mut().delete(page_id)
    }
}

#[derive(Debug)]
struct ManagerInner {
    page_size: usize,
    tables: HashMap<String, Memory>,
}

impl ManagerInner {
    fn build(&mut self, table_name: &str) -> Result<Memory, Error> {
        if let Some(page_store) = self.tables.get(table_name) {
            return Ok(Memory {
                inner: page_store.inner.clone(),
            });
        }

        let page_store = Memory::new(self.page_size);
        let ret_page_store = Memory {
            inner: page_store.inner.clone(),
        };
        self.tables.insert(table_name.to_string(), page_store);
        Ok(ret_page_store)
    }
}

#[derive(Debug)]
pub struct MemoryManager {
    inner: Rc<RefCell<ManagerInner>>,
}

impl MemoryManager {
    pub fn new(page_size: usize) -> Self {
        MemoryManager {
            inner: Rc::new(RefCell::new(ManagerInner {
                tables: HashMap::new(),
                page_size,
            })),
        }
    }
}

impl TablePageStoreManager for MemoryManager {
    type PageStore = Memory;
    type Builder = Memory;

    fn builder(&mut self, table_name: &str) -> Result<Self::Builder, Error> {
        self.inner.borrow_mut().build(table_name)
    }
}

impl TablePageStoreBuilder for Memory {
    type PageStore = Memory;

    fn build(&mut self) -> Result<Self::PageStore, Error> {
        Ok(Self {
            inner: self.inner.clone(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_memory() {
        let mut mem = Memory::new(10);
        assert_eq!(10, mem.usable_page_size());

        fn check(mem: &mut Memory, page_id: PageID) {
            // this page exists, but is empty
            assert_eq!(Vec::<u8>::new(), mem.get(page_id).unwrap());

            // put something in the page and assert it is returned
            assert!(mem.put(page_id, vec![1, 2, 3]).is_ok());
            assert_eq!(vec![1, 2, 3], mem.get(page_id).unwrap());

            // now put in something too big
            assert_eq!(
                Err(Error::PayloadTooLong(11, 10)),
                mem.put(page_id, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1])
            );

            // put something different, ensure its returned
            assert!(mem.put(page_id, vec![4, 5, 6, 7]).is_ok());
            assert_eq!(vec![4, 5, 6, 7], mem.get(page_id).unwrap());
        }

        // check 0th page is already setup for operation
        check(&mut mem, PageID(0));

        // create a new page and verify
        assert_eq!(Err(Error::UnallocatedPage(PageID(1))), mem.get(PageID(1)));
        assert_eq!(Ok(PageID(1)), mem.allocate());
        check(&mut mem, PageID(1));

        // delete the most recent page and ensure we get it back on re-allocation
        assert!(mem.delete(PageID(1)).is_ok());
        assert_eq!(Ok(PageID(1)), mem.allocate());
        check(&mut mem, PageID(1));

        // ok, now allocate a brand new page
        assert_eq!(Ok(PageID(2)), mem.allocate());
        check(&mut mem, PageID(2));

        // delete the first page again and verify we get it again from the free list and not
        // allocate an entirely new page
        assert!(mem.delete(PageID(1)).is_ok());
        assert_eq!(Ok(PageID(1)), mem.allocate());
        check(&mut mem, PageID(1));
    }
}
