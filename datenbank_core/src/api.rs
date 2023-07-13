use std::path::PathBuf;

use crate::exec::execute;
use crate::parser::parse;

pub use crate::cache::Error as CacheError;
pub use crate::exec::{DatabaseResult, Error as ExecError, ExecResult, QueryResult};
pub use crate::pagestore::{Error as PageError, FileManager, MemoryManager, TablePageStoreManager};
pub use crate::parser::Error as ParseError;
pub use crate::row::Error as RowError;
pub use crate::schema::{Column, Error as SchemaError};
pub use crate::table::btree::Error as BTreeError;
pub use crate::table::Error as TableError;

// 16k page sizes for now
// TODO: How should this be made configurable?
const PAGE_SIZE: usize = 16 * 1024;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("error executing input")]
    Exec(#[from] ExecError),
    #[error("error parsing input")]
    Parse(#[from] ParseError),
}

pub struct Database<M: TablePageStoreManager> {
    manager: M,
}

impl Database<MemoryManager> {
    // Build a database built upon only in-memory storage for the tables.
    pub fn memory() -> Database<MemoryManager> {
        Database {
            manager: MemoryManager::new(PAGE_SIZE),
        }
    }

    pub fn file(path: impl Into<PathBuf>) -> Database<FileManager> {
        Database {
            manager: FileManager::new(path, PAGE_SIZE as u32),
        }
    }
}

impl<M: TablePageStoreManager> Database<M> {
    // Execute the provided SQL string against the database.
    pub fn exec(&mut self, input: &str) -> Result<DatabaseResult, Error> {
        let input = parse(input)?;
        execute(&mut self.manager, input).map_err(Into::into)
    }
}
