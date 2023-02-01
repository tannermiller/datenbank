use std::collections::{HashMap, VecDeque};

use crate::{Error, TablePageStore};

pub struct Memory {
    page_size: usize,
    last_page: usize,
    store: HashMap<usize, Vec<u8>>,
    free_list: VecDeque<usize>,
}

impl Memory {
    pub fn new(page_size: usize) -> Self {
        Memory {
            page_size,
            last_page: 0,
            store: HashMap::new(),
            free_list: VecDeque::new(),
        }
    }
}

impl TablePageStore for Memory {
    fn allocate(&mut self) -> Result<usize, Error> {
        match self.free_list.pop_front() {
            Some(free_id) => Ok(free_id),
            None => {
                // we've got to "allocate" the 0th page for table stuff, so the first id we return
                // from here is at least 1
                self.last_page += 1;
                Ok(self.last_page)
            }
        }
    }

    fn usable_page_size(&self) -> usize {
        // we don't need any header to manage pages, so we can use the whole page
        self.page_size
    }

    fn get(&self, page_id: usize) -> Result<Vec<u8>, Error> {
        match self.store.get(&page_id) {
            Some(payload) => Ok(payload.clone()),
            // 0th page is auto "allocated" and anything thats under the last page has probably
            // just been returned from the free list
            None if page_id == 0 || page_id <= self.last_page => Ok(vec![]),
            None => Err(Error::UnallocatedPage(page_id)),
        }
    }

    fn put(&mut self, page_id: usize, payload: Vec<u8>) -> Result<(), Error> {
        if page_id > self.last_page {
            return Err(Error::UnallocatedPage(page_id));
        }

        if payload.len() > self.page_size {
            return Err(Error::PayloadTooLong(payload.len(), self.page_size));
        }

        self.store.insert(page_id, payload);

        Ok(())
    }

    fn delete(&mut self, page_id: usize) -> Result<(), Error> {
        if page_id > self.last_page {
            return Err(Error::UnallocatedPage(page_id));
        }

        self.store.remove(&page_id);
        self.free_list.push_back(page_id);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_memory() {
        let mut mem = Memory::new(10);
        assert_eq!(10, mem.usable_page_size());

        fn check(mem: &mut Memory, page_id: usize) {
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
        check(&mut mem, 0);

        // create a new page and verify
        assert_eq!(Err(Error::UnallocatedPage(1)), mem.get(1));
        assert_eq!(Ok(1), mem.allocate());
        check(&mut mem, 1);

        // delete the most recent page and ensure we get it back on re-allocation
        assert!(mem.delete(1).is_ok());
        assert_eq!(Ok(1), mem.allocate());
        check(&mut mem, 1);

        // ok, now allocate a brand new page
        assert_eq!(Ok(2), mem.allocate());
        check(&mut mem, 2);

        // delete the first page again and verify we get it again from the free list and not
        // allocate an entirely new page
        assert!(mem.delete(1).is_ok());
        assert_eq!(Ok(1), mem.allocate());
        check(&mut mem, 1);
    }
}
