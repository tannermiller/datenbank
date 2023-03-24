use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::{Error, TablePageStore, TablePageStoreBuilder};

#[derive(Debug)]
pub struct File {
    page_size: u32,
    file: fs::File,
}

impl TablePageStore for File {
    fn allocate(&mut self) -> Result<usize, Error> {
        // Seek to end, get final position, divide by page_size to get current page_count,
        // write out a new page of zeroes

        // TODO: need to handle getting from the free list first and only allocating if nothing
        // free

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
