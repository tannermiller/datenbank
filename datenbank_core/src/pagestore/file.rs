use std::fs;
use std::path::PathBuf;

use super::{Error, TablePageStore, TablePageStoreBuilder};

// TODO: Do we need a page allocated per file that manages everything? free list, etc?

#[derive(Debug)]
pub struct File {
    page_size: u32,
    file: fs::File,
}

impl TablePageStore for File {
    fn allocate(&mut self) -> Result<usize, Error> {
        todo!()
    }

    fn usable_page_size(&self) -> usize {
        // we need 4 bytes per page to store the length of the actual page data, the rest of the
        // page is zeroed out on disk
        self.page_size - 4 as usize
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
        let file = fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .map_err(|e| Error::Io(e.to_string()))?;
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
