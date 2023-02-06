use crate::exec::{execute, Error as ExecError};
use crate::pagestore::{MemoryBuilder, TablePageStoreBuilder};
use crate::parser::parse;

pub use crate::exec::ExecResult;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("error executing query")]
    Exec(#[from] ExecError),
}

pub struct Database<B: TablePageStoreBuilder> {
    builder: B,
}

impl Database<MemoryBuilder> {
    // Build a database built upon only in-memory storage for the tables.
    pub fn memory() -> Database<MemoryBuilder> {
        Database {
            builder: MemoryBuilder::new(),
        }
    }
}

impl<B: TablePageStoreBuilder> Database<B> {
    // Execute the provided SQL string against the database.
    pub fn exec(input: &str) -> Result<ExecResult, Error> {
        let input = parse(input);
        execute(input).map_err(Into::into)
    }
}
