mod memory;

pub use memory::Memory;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("error during i/o operation {0}")]
    Io(String),
    #[error("provided payload len {0} is larger than the page size {1}")]
    PayloadTooLong(usize, usize),
    #[error("provided page id has not been allocated yet: {0}")]
    UnallocatedPage(usize),
}

// TablePageStore is responsible for maintaining the persistence of data pages in persistence. This
// trait is a per-table resource, including any indices on that table. A page size will also be
// associated with this store and can not be changed after it has been established. Each
// implementation of this should automatically allocate the 0-indexed page at initial creation
// time. That 0th page will be used for table header information and not for table content.
pub trait TablePageStore: Clone {
    // Allocate and prepare a new page in persistence, returning the page id.
    fn allocate(&mut self) -> Result<usize, Error>;

    // Return the maximum usable size for each page. This may be less than the page size provided
    // to construct this store as each implementation may reserve some bytes to act as an
    // implementation specific header.
    fn usable_page_size(&self) -> usize;

    // Get a page given a page id. The page id must be one that has already been allocated, if not
    // an Err(Error::UnallocatedPage) error will be returned.
    fn get(&self, page_id: usize) -> Result<Vec<u8>, Error>;

    // Write a page's payload to persistence. An Err(Error::PayloadTooLong) will be returned if the
    // payload.
    fn put(&mut self, page_id: usize, payload: Vec<u8>) -> Result<(), Error>;

    // Delete the contents of the page and free it for reuse/deallocation.
    fn delete(&mut self, page_id: usize) -> Result<(), Error>;
}
