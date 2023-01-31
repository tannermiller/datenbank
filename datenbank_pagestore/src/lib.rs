#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("error during i/o operation {0}")]
    Io(String),
    #[error("provided payload len {0} is larger than the page size {1}")]
    PayloadTooLong(usize, usize),
}

// TablePageStore is responsible for maintaining the persistence of data pages in persistence. This
// trait is a per-table resource, including any indices on that table. A page size will also be
// associated with this store and can not be changed after it has been established.
pub trait TablePageStore {
    // Allocate and prepare a new page in persistence, returning the page id.
    fn allocate(&mut self) -> Result<usize, Error>;
    // Get a page given a page id. If the page is not found, then an Ok(None) is returned.
    fn get(&self, page_id: usize) -> Result<Option<Vec<u8>>, Error>;
    // Write a page's payload to persistence. An Err(Error::PayloadTooLong) will be returned if the
    // payload
    fn put(&mut self, page_id: usize, payload: Vec<u8>) -> Result<(), Error>;
    fn delete(&mut self, page_id: usize) -> Result<(), Error>;
}
