use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::{
    AllocationState, Error, PageID, TablePageStore, TablePageStoreBuilder, TablePageStoreManager,
};

// File maintains the second page (that is the 1st, since 0-indexed) to hold the initial free list.
// The free list contains all the page ids, as big-endian u32s which have been allocated in the
// file but are not currently marked as "in-use". The free list page starts with a header of 8
// bytes:
//   * 4 bytes for a pointer to the next free list page; it's zeroed out if there is none
//   * 4 bytes for the count of page ids in this page
// When allocating a new page, the free list is checked and a page id is popped off of it if one
// exists. Only if no available page ids are in the free list then is a new page allocated to the
// end of the file.
// Another peculiarity of the free list is that the last page of it may be empty, but we can't free
// it as that would cause the previous page to overflow, and thus we need a new free list page to
// hold the free list page we just freed. So it is valid state for the last free page to either be
// full and have no further pages or the last free list page may have no free pages. If the empty
// free page has another free page_id then we can clear it and re-use it as the allocated page; we
// must merely change the pointer on the previous free page to be clear.

#[derive(Debug)]
pub struct File {
    page_size: u32,
    file: fs::File,
}

// WAL Allocation Strategy:
//
// Initially, we'll see that the AllocationStatus.maximum_id is 1 as that accounts for the table
// header (page 0) and the initially empty free list (page 1) and the free list is empty. On
// database startup the free list and current maximum page id will be read for each table and kept
// in memory in the WAL. Then the uncheckpointed WAL entries will be read and any pages that are
// written to will be removed from the in-memory free list if they are on there and kept in a
// reused free list. In addition if any page written is beyond the persisted maximum page id, then
// that will be set to the highest page id found in WAL entries. When a transaction requires a new
// page to be allocated, the WAL will first check the in-memory free list and if there are page ids
// stored on there then the last one will be popped off the free list and used for the new data
// page. That page id will also be stored in the removed list.If the free list is empty then the
// in-memory maximum page id will be bumped by 1 and that new value will be used as the allocated
// page id. When checkpointing a transaction from the WAL into the table file, if a file is in the
// persisted free list but is written to then it will be removed from the persisted free list
// before writing the page to the table. Also, if the maximum id of the pages in the transaction is
// above the persisted maximum id then new pages will be allocated up to that maximum id. In the
// case where a transaction deletes a page, then a special entry in the WAL will be added which
// contains the page ids of any deleted pages. These will be added to the in-memory free list so
// they could be re-used in the next transaction that allocates. At checkpoint time, if the
// deleted page id is still considered deleted then it will be deleted in the persistent table file
// and added to the persistent free list.

impl TablePageStore for File {
    fn allocate(&mut self) -> Result<PageID, Error> {
        // Seek to end, get final position, divide by page_size to get current page_count,
        // write out a new page of zeroes

        let mut current_free_list_page = PageID(1);
        let mut previous_free_list_page = None;
        loop {
            let mut free_list_page = self.get(current_free_list_page)?;

            if free_list_page.len() < 8 {
                // somethings messed up with the free list page, just break and allocate a page id
                break;
            }

            let next_page_pointer = read_u32_from_slice(&free_list_page[..4])?;

            // if the next page pointer isn't zero, then there's another page out there, iterate to
            // that since we pop off the back of the free list
            if next_page_pointer != 0 {
                previous_free_list_page = Some(current_free_list_page);
                current_free_list_page = next_page_pointer.into();
                continue;
            }

            // now we know we're on the last page of the free list
            let list_len = read_u32_from_slice(&free_list_page[4..8])?;

            // If the list len is zero, then we're in the weird situation where we can't unfree
            // this page, because then we'd need another free list page to hold this unfreed page.
            // To sidestep that whole problem we'll just clear this page out and return it as the
            // newly allocated page and reset the previous page's pointer to blank, indicating that
            // it is now the last page.
            match (list_len, previous_free_list_page) {
                (0, None) => break, // first page, free list is empty, just allocate a new page
                (0, Some(prev)) => {
                    // return this page and make the prev page the last one
                    let mut prev_free_page = self.get(prev)?;

                    // clear the pointer bytes to indicate the prev page is now the last
                    prev_free_page.splice(0..4, [0, 0, 0, 0]);
                    self.put(prev, prev_free_page)?;

                    // return the current free page as the allocated page
                    return Ok(current_free_list_page);
                }
                _ => (), // otherwise continue down
            }

            // pull out the last list from the free page
            let start_idx = (8 + (list_len - 1) * 4) as usize;
            let page_id = read_u32_from_slice(&free_list_page[start_idx..start_idx + 4])?;
            free_list_page.truncate(free_list_page.len() - 4);
            free_list_page.splice(4..8, (list_len - 1).to_be_bytes());
            self.put(current_free_list_page, free_list_page)?;

            return Ok(page_id.into());
        }

        // if we made it past the loop, then we need to actually allocate a new page
        let position = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(e.to_string()))?;

        // this will give us the total written pages up to now, since the pages are 0-indexed this
        // will actually also be the returned index for the next page
        let page_count = position / self.page_size as u64;

        self.file
            .write_all(&vec![0; self.page_size as usize])
            .map_err(|e| Error::Io(e.to_string()))?;

        Ok(PageID(page_count as usize))
    }

    fn allocation_state(&mut self) -> Result<AllocationState, Error> {
        let mut free_list = Vec::new();

        let mut current_free_list_page = PageID(1);
        loop {
            let free_list_page = self.get(current_free_list_page)?;
            if free_list_page.len() < 8 {
                // Somethings messed up with the free list page, just break and return what we've
                // got.
                break;
            }

            let list_len = read_u32_from_slice(&free_list_page[4..8])?;

            // If the list len is zero, then we're in the weird situation where we can't unfree
            // this page, because then we'd need another free list page to hold this unfreed page.
            // To sidestep that whole problem we add this page to the end of our free list and will
            // use it first when we next allocate. We'll also remove the next page pointer that
            // points at this page then. Also, we don't do that if this is the initial free list
            // page.
            if list_len == 0 && current_free_list_page != PageID(1) {
                free_list.push(current_free_list_page);
                break;
            }

            // iterate through the free pages and add them to the free list
            for i in 0..(list_len as usize) {
                let start = 8 + i;
                let end = start + 4;
                let page_id = read_u32_from_slice(&free_list_page[start..end])?;
                free_list.push(page_id.into())
            }

            let next_page_pointer = read_u32_from_slice(&free_list_page[..4])?;
            if next_page_pointer == 0 {
                // If the next page pointer is 0 then that means we're on the last page so we can
                // break the loop here.
                break;
            }

            // if the next page pointer isn't zero, then there's another page out there, iterate to
            // that since we pop off the back of the free list
            current_free_list_page = next_page_pointer.into();
        }

        // now we must find the maximum allocated page id.
        let position = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(e.to_string()))?;

        // This will give us the total written pages up to now. Since the pages are 0-indexed this
        // will actually also be the returned index for the next page so the maximum id is 1 less
        // than this.
        let page_count = position / self.page_size as u64;

        let maximum_id = PageID((page_count - 1) as usize);
        Ok(AllocationState {
            maximum_id,
            free_list,
        })
    }

    fn usable_page_size(&self) -> usize {
        // we need 4 bytes per page to store the length of the actual page data, the rest of the
        // page is zeroed out on disk
        (self.page_size - 4) as usize
    }

    fn get(&mut self, page_id: PageID) -> Result<Vec<u8>, Error> {
        let PageID(pid) = page_id;
        let start_position = pid as u64 * (self.page_size as u64);

        let total_size = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(e.to_string()))?;

        // if the total size of the file is less than the start_position then we're passed the end
        // of the file and we haven't allocated it yet
        if total_size < start_position {
            return Err(Error::UnallocatedPage(page_id));
        }

        self.file
            .seek(SeekFrom::Start(start_position))
            .map_err(|e| Error::Io(e.to_string()))?;

        // the first 4 bytes are the actual len of the page
        let mut page_len_bytes = [0; 4];
        self.file
            .read_exact(&mut page_len_bytes)
            .map_err(|e| Error::Io(e.to_string()))?;
        let page_len = u32::from_be_bytes(page_len_bytes);

        // now read the page data itself, not including the trailing zeroes
        let mut page_data = vec![0; page_len as usize];
        self.file
            .read_exact(&mut page_data)
            .map_err(|e| Error::Io(e.to_string()))?;

        Ok(page_data)
    }

    fn put(&mut self, page_id: PageID, mut payload: Vec<u8>) -> Result<(), Error> {
        if payload.len() > self.usable_page_size() {
            return Err(Error::PayloadTooLong(
                payload.len(),
                self.usable_page_size(),
            ));
        }

        let PageID(pid) = page_id;
        let start_position = pid as u64 * (self.page_size as u64);

        let file_len = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(e.to_string()))?;

        if file_len < start_position {
            return Err(Error::UnallocatedPage(page_id));
        }

        self.file
            .seek(SeekFrom::Start(start_position))
            .map_err(|e| Error::Io(e.to_string()))?;

        // first write len of payload
        self.file
            .write_all(&(payload.len() as u32).to_be_bytes())
            .map_err(|e| Error::Io(e.to_string()))?;

        // now extend the payload with zeroes out to the full size of a page so we overwrite
        // any data that might be there before if this page has shrunk in size
        if payload.len() < self.usable_page_size() {
            payload.resize(self.usable_page_size(), 0);
        }

        self.file
            .write_all(&payload)
            .map_err(|e| Error::Io(e.to_string()))
    }

    fn delete(&mut self, page_id: PageID) -> Result<(), Error> {
        // On delete, we need to zero our the page and then add it to the free list. In order to
        // add to the free list, we start at the first free list page (index 1) and follow the
        // linked list of free list page nodes to the last one. We add this page id to the last
        // one. If the page id won't fit on the last page then we utilize this page id as the new
        // empty free list and the previous free list page has its pointer rewritten to point at
        // this page.

        let mut current_free_list_page = PageID(1);
        loop {
            let mut free_list_page = self.get(current_free_list_page)?;

            if free_list_page.len() < 8 {
                // something's wrong here, we're just going to reformat this free list page and add
                // our new page here

                // 4 for pointer, 4 for len, 4 for first page id
                let mut free_list_page = Vec::with_capacity(12);
                free_list_page.extend(0u32.to_be_bytes());
                free_list_page.extend(1u32.to_be_bytes());
                free_list_page.extend(page_id.to_be_bytes());

                self.put(current_free_list_page, free_list_page)?;
                return Ok(());
            }

            let next_page_pointer = read_u32_from_slice(&free_list_page[..4])?;
            if next_page_pointer != 0 {
                // head on to the next page
                current_free_list_page = next_page_pointer.into();
                continue;
            }

            let list_len = read_u32_from_slice(&free_list_page[4..8])?;
            let max_page_count = (self.usable_page_size() - 8) / 4;
            if list_len as usize == max_page_count {
                // this page is full, we need to make the deleted page a new free list page
                free_list_page.splice(..4, page_id.to_be_bytes());
                self.put(current_free_list_page, free_list_page)?;
                self.put(page_id, vec![0, 0, 0, 0, 0, 0, 0, 0])?;
                return Ok(());
            }

            // ok, we're sticking it in this page as we've got space in it
            free_list_page.extend(page_id.to_be_bytes());
            free_list_page.splice(4..8, (list_len + 1).to_be_bytes());
            self.put(current_free_list_page, free_list_page)?;

            return Ok(());
        }
    }
}

fn read_u32_from_slice(slice: &[u8]) -> Result<u32, Error> {
    match slice.try_into() {
        Ok(s) => Ok(u32::from_be_bytes(s)),
        Err(e) => Err(Error::Io(e.to_string())),
    }
}

#[derive(Debug)]
pub struct FileBuilder {
    page_size: u32,
    file_path: PathBuf,
}

impl TablePageStoreBuilder for FileBuilder {
    type PageStore = File;

    fn build(&mut self) -> Result<Self::PageStore, Error> {
        //let file_path = self.directory.join(&self.table_name).with_extension("dbdb");
        let mut file = fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.file_path)
            .map_err(|e| Error::Io(e.to_string()))?;

        let file_len = file
            .seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(e.to_string()))?;

        if file_len == 0 {
            // Write out two pages worth of zeroes to this new file so that we can allocate the 0th
            // to the table header and the 1st to the initial free list; all zeroes is a correct
            // encoding for a new, empty free list.
            file.write_all(&vec![0; (self.page_size * 2) as usize])
                .map_err(|e| Error::Io(e.to_string()))?;
        }

        Ok(File {
            file,
            page_size: self.page_size,
        })
    }
}

#[derive(Debug)]
pub struct FileManager {
    page_size: u32,
    directory: PathBuf,
}

impl TablePageStoreManager for FileManager {
    type PageStore = File;
    type Builder = FileBuilder;

    fn builder(&mut self, table_name: &str) -> Result<Self::Builder, Error> {
        Ok(FileBuilder {
            page_size: self.page_size,
            file_path: self.directory.join(table_name).with_extension("dbdb"),
        })
    }
}

impl FileManager {
    // Get a new FileManager which will store the tables. If the path is not a directory, then the
    // parent directory of the file will be used.
    pub fn new(directory_path: impl Into<PathBuf>, page_size: u32) -> Self {
        let path = directory_path.into();
        let directory = if path.is_file() {
            path.parent()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| PathBuf::from("/"))
        } else {
            path
        };

        Self {
            page_size,
            directory,
        }
    }
}

#[cfg(test)]
mod test {
    use std::env;
    use std::fs;

    use super::*;

    #[test]
    fn test_file_page_store() {
        let table_name = "file_store_test";
        let temp_dir = env::temp_dir();
        let db_file_path = temp_dir.join(table_name).with_extension("dbdb");
        // just cleaning up from a previous run, doesn't matter what happens
        let _ = fs::remove_file(&db_file_path);

        let mut file_manager = FileManager::new(temp_dir, 16 * 1024);
        let mut store = file_manager.builder(table_name).unwrap().build().unwrap();
        let next_page = store.allocate().unwrap();
        assert_eq!(PageID(2), next_page);

        let page_data = store.get(next_page).unwrap();
        assert!(page_data.is_empty());
        assert_eq!(
            Err(Error::UnallocatedPage(PageID(next_page.0 + 2))),
            store.get(PageID(next_page.0 + 2))
        );

        store
            .put(0u32.into(), b"some header data".to_vec())
            .unwrap();
        store
            .put(next_page, b"gonna put something here too".to_vec())
            .unwrap();

        assert_eq!(
            Err(Error::UnallocatedPage(PageID(next_page.0 + 2))),
            store.put(PageID(next_page.0 + 2), vec![1, 0]),
        );

        let page_data = store.get(0u32.into()).unwrap();
        assert_eq!(b"some header data".as_slice(), &page_data);

        let page_data = store.get(next_page).unwrap();
        assert_eq!(b"gonna put something here too".as_slice(), &page_data);

        let another_page = store.allocate().unwrap();
        assert_eq!(PageID(3), another_page);
        store
            .put(another_page, b"I'm hereeeeeeee".to_vec())
            .unwrap();
        let page_data = store.get(another_page).unwrap();
        assert_eq!(b"I'm hereeeeeeee".as_slice(), &page_data);

        // stick a full sized payload in the previous spot and ensure it doesn't bleed over
        store
            .put(next_page, b"f".repeat(store.usable_page_size()))
            .unwrap();
        let page_data = store.get(next_page).unwrap();
        assert_eq!(b"f".repeat(store.usable_page_size()).as_slice(), &page_data);

        let page_data = store.get(another_page).unwrap();
        assert_eq!(b"I'm hereeeeeeee".as_slice(), &page_data);

        assert_eq!(
            Err(Error::PayloadTooLong(
                store.usable_page_size() + 1,
                store.usable_page_size()
            )),
            store.put(next_page, b"f".repeat(store.usable_page_size() + 1)),
        );

        // test delete and allocate gives us this page back
        store.delete(next_page).unwrap();
        assert_eq!(next_page, store.allocate().unwrap());

        let _ = fs::remove_file(db_file_path);
    }

    #[test]
    fn test_free_list() {
        let table_name = "free_list_test";
        let temp_dir = env::temp_dir();
        let db_file_path = temp_dir.join(table_name).with_extension("dbdb");
        // just cleaning up from a previous run, doesn't matter what happens
        let _ = fs::remove_file(&db_file_path);

        let mut file_manager = FileManager::new(temp_dir, 16 * 1024);
        let mut store = file_manager.builder(table_name).unwrap().build().unwrap();
        let count = 10000;

        for i in 0..count {
            assert_eq!(PageID(i + 2), store.allocate().unwrap());
        }

        for i in 0..count {
            store.delete(PageID(i + 2)).unwrap();
        }

        for i in (0..count).rev() {
            assert_eq!(PageID(i + 2), store.allocate().unwrap());
        }

        let _ = fs::remove_file(db_file_path);
    }
}
