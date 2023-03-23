use std::fs;
use std::io::{Seek, SeekFrom, Write};
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

        let position = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(e.to_string()))?;

        // this will give us the total written pages up to now, since the pages are 0-indexed this
        // will actually also be the returned index for the next page
        let page_count = (position + 1) / self.page_size as u64;

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

    fn get(&self, page_id: usize) -> Result<Vec<u8>, Error> {
        todo!()
    }

    fn put(&mut self, page_id: usize, payload: Vec<u8>) -> Result<(), Error> {
        todo!()
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
    fn test_allocate() {
        let temp_dir = env::temp_dir();
        let db_file_path = temp_dir.join("allocate_test").with_extension("dbdb");
        // just cleaning up from a previous run, doesn't matter what happens
        let _ = fs::remove_file(&db_file_path);

        let mut file_builder = FileBuilder::new(temp_dir, 16 * 1024);
        let mut store = file_builder.build("allocate_test").unwrap();
        let next_page = store.allocate().unwrap();
        assert_eq!(2, next_page);

        let _ = fs::remove_file(db_file_path);
    }
}
