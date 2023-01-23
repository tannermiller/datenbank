use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error attempting operation: {0}")]
    Io(String),
}

pub trait KeyValueStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error>;
}

pub struct Memory {
    store: HashMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    pub fn new() -> Memory {
        Memory {
            store: HashMap::new(),
        }
    }
}

impl std::default::Default for Memory {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStore for Memory {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.store.get(key).cloned())
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        self.store.insert(key, value);
        Ok(())
    }
}
