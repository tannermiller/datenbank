mod file;
mod memory;

pub use file::{File, FileBuilder, FileManager};
pub use memory::{Memory, MemoryManager};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("error during i/o operation {0}")]
    Io(String),
    #[error("provided payload len {0} is larger than the page size {1}")]
    PayloadTooLong(usize, usize),
    #[error("provided page id has not been allocated yet: {0}")]
    UnallocatedPage(PageID),
    #[error("can't remove page from free list: {0}")]
    InvalidFreePage(PageID),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageID(usize);

impl PageID {
    pub const fn to_be_bytes(self) -> [u8; 4] {
        (self.0 as u32).to_be_bytes()
    }
}

impl From<u32> for PageID {
    fn from(pid: u32) -> PageID {
        PageID(pid as usize)
    }
}

impl From<usize> for PageID {
    fn from(pid: usize) -> PageID {
        PageID(pid)
    }
}

impl std::fmt::Display for PageID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

pub struct AllocationState {
    // The maximum allocated page id.
    pub maximum_id: PageID,

    // The set of allocated pages that do not currently have any stored data and may be re-used.
    pub free_list: Vec<PageID>,
}

// TablePageStore is responsible for maintaining the persistence of data pages in persistence. This
// trait is a per-table resource, including any indices on that table. A page size will also be
// associated with this store and can not be changed after it has been established. Each
// implementation of this should automatically allocate the 0-indexed page at initial creation
// time. That 0th page will be used for table header information and not for table content.
pub trait TablePageStore: std::fmt::Debug {
    // Return the current inner state of the page allocations.
    fn allocation_state(&mut self) -> Result<AllocationState, Error>;

    // Allocate a new page from the end of the pagestore.
    fn allocate_new(&mut self) -> Result<PageID, Error>;

    // Remove the page id from the persisted free list. This is used to indicate that the page is
    // being used without allocating a new page.
    fn remove_from_free_list(&mut self, page_id: PageID) -> Result<(), Error>;

    // Add this page id to the free list. This indicates that the page id is no longer storing
    // useful data and should be added to the persisted free list. In addition, the persisted page
    // will be zeroed out.
    fn add_to_free_list(&mut self, page_id: PageID) -> Result<(), Error>;

    // Return the maximum usable size for each page. This may be less than the page size provided
    // to construct this store as each implementation may reserve some bytes to act as an
    // implementation specific header.
    fn usable_page_size(&self) -> usize;

    // Get a page given a page id. The page id must be one that has already been allocated, if not
    // an Err(Error::UnallocatedPage) error will be returned.
    fn get(&mut self, page_id: PageID) -> Result<Vec<u8>, Error>;

    // Write a page's payload to persistence. An Err(Error::PayloadTooLong) will be returned if the
    // payload.
    fn put(&mut self, page_id: PageID, payload: Vec<u8>) -> Result<(), Error>;
}

pub trait TablePageStoreBuilder: std::fmt::Debug {
    type PageStore: TablePageStore;

    // Build may either create an entirely new instance of a TablePageStore, or return a clone of
    // an already built one.
    fn build(&mut self) -> Result<Self::PageStore, Error>;
}

// TablePageStoreManager is responsible for constructing TablePageStoreBuilders for a given table.
pub trait TablePageStoreManager: std::fmt::Debug {
    type PageStore: TablePageStore;
    type Builder: TablePageStoreBuilder<PageStore = Self::PageStore>;

    // Builder returns a TablePageStoreBuilder which will repeatedly return a builder for a single
    // table name.
    fn builder(&mut self, table_name: &str) -> Result<Self::Builder, Error>;
}
