use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::{Error, TablePageStore, TablePageStoreBuilder};

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

impl TablePageStore for File {
    fn allocate(&mut self) -> Result<usize, Error> {
        // Seek to end, get final position, divide by page_size to get current page_count,
        // write out a new page of zeroes

        let mut current_free_list_page = 1;
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
                current_free_list_page = next_page_pointer as usize;
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
            free_list_page.splice(start_idx..start_idx + 4, [0, 0, 0, 0]);
            self.put(current_free_list_page, free_list_page)?;

            return Ok(page_id as usize);
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

        Ok(page_count as usize)
    }

    fn usable_page_size(&self) -> usize {
        // we need 4 bytes per page to store the length of the actual page data, the rest of the
        // page is zeroed out on disk
        (self.page_size - 4) as usize
    }

    fn get(&mut self, page_id: usize) -> Result<Vec<u8>, Error> {
        let start_position = page_id as u64 * (self.page_size as u64);

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

    fn put(&mut self, page_id: usize, mut payload: Vec<u8>) -> Result<(), Error> {
        if payload.len() > self.usable_page_size() {
            return Err(Error::PayloadTooLong(
                payload.len(),
                self.usable_page_size(),
            ));
        }

        let start_position = page_id as u64 * (self.page_size as u64);

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

    fn delete(&mut self, page_id: usize) -> Result<(), Error> {
        todo!()
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
    directory: PathBuf,
}

impl TablePageStoreBuilder for FileBuilder {
    type TablePageStore = File;

    fn build(&mut self, table_name: &str) -> Result<Self::TablePageStore, Error> {
        let file_path = self.directory.join(table_name).with_extension("dbdb");
        let mut file = fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
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

impl FileBuilder {
    // Get a new FileBuilder which will store the tables. If the path is not a directory, then the
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

        FileBuilder {
            directory,
            page_size,
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
        let temp_dir = env::temp_dir();
        let db_file_path = temp_dir.join("file_store_test").with_extension("dbdb");
        // just cleaning up from a previous run, doesn't matter what happens
        let _ = fs::remove_file(&db_file_path);

        let mut file_builder = FileBuilder::new(temp_dir, 16 * 1024);
        let mut store = file_builder.build("file_store_test").unwrap();
        let next_page = store.allocate().unwrap();
        assert_eq!(2, next_page);

        let page_data = store.get(next_page).unwrap();
        assert!(page_data.is_empty());
        assert_eq!(
            Err(Error::UnallocatedPage(next_page + 2)),
            store.get(next_page + 2)
        );

        store.put(0, b"some header data".to_vec()).unwrap();
        store
            .put(next_page, b"gonna put something here too".to_vec())
            .unwrap();

        assert_eq!(
            Err(Error::UnallocatedPage(next_page + 2)),
            store.put(next_page + 2, vec![1, 0]),
        );

        let page_data = store.get(0).unwrap();
        assert_eq!(b"some header data".as_slice(), &page_data);

        let page_data = store.get(next_page).unwrap();
        assert_eq!(b"gonna put something here too".as_slice(), &page_data);

        let another_page = store.allocate().unwrap();
        assert_eq!(3, another_page);
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

        let _ = fs::remove_file(db_file_path);
    }
}
