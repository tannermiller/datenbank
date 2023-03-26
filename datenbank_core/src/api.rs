use std::path::PathBuf;

use crate::exec::{execute, Error as ExecError};
use crate::parser::{parse, Error as ParseError};

pub use crate::exec::ExecResult;
pub use crate::pagestore::{FileBuilder, MemoryBuilder, TablePageStoreBuilder};

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

pub struct Database<B: TablePageStoreBuilder> {
    builder: B,
}

impl Database<MemoryBuilder> {
    // Build a database built upon only in-memory storage for the tables.
    pub fn memory() -> Database<MemoryBuilder> {
        Database {
            builder: MemoryBuilder::new(PAGE_SIZE),
        }
    }

    pub fn file(path: impl Into<PathBuf>) -> Database<FileBuilder> {
        Database {
            builder: FileBuilder::new(path, PAGE_SIZE as u32),
        }
    }
}

impl<B: TablePageStoreBuilder> Database<B> {
    // Execute the provided SQL string against the database.
    pub fn exec(&mut self, input: &str) -> Result<ExecResult, Error> {
        let input = parse(input)?;
        execute(&mut self.builder, input).map_err(Into::into)
    }
}
