use std::fs::File as StdFile;
use std::path::PathBuf;

use super::{Error, TablePageStore, TablePageStoreBuilder};

#[derive(Clone, Debug)]
pub struct File {
    //file: StdFile,
}

impl TablePageStore for File {
    fn allocate(&mut self) -> Result<usize, Error> {
        todo!()
    }

    fn usable_page_size(&self) -> usize {
        todo!()
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

#[derive(Clone, Debug)]
pub struct FileBuilder {
    directory: PathBuf,
}

impl TablePageStoreBuilder for FileBuilder {
    type TablePageStore = File;

    fn build(&mut self, table_name: &str) -> Result<Self::TablePageStore, Error> {
        let file_path = self.directory.join(table_name).with_extension("dbdb");
        let table_file = StdFile::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .map_err(|e| Error::Io(e.to_string()))?;
        todo!()
    }
}

impl FileBuilder {
    // Get a new FileBuilder which will store the tables. If the path is not a directory, then the
    // parent directory of the file will be used.
    pub fn new(directory_path: impl Into<PathBuf>) -> Self {
        let path = directory_path.into();
        let directory = if path.is_file() {
            path.parent()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| PathBuf::from("/"))
        } else {
            path
        };

        FileBuilder { directory }
    }
}
