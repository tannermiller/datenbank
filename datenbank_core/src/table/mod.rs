use std::io::Write;

use crate::pagestore::{Error as PageError, TablePageStore};
use crate::schema::Schema;
use btree::{BTree, Error as BTreeError};

mod btree;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] PageError),
    #[error("cannot create table that already exists")]
    TableExists,
    #[error("error from btree")]
    BTree(#[from] BTreeError),
    #[error("table name must be <= 255 chars")]
    NameTooLong,
}

// A Table is responsible for the management of everything pertaining to a single database table's
// data. It manages the table btree, any indices as well as the persistence of the table.
pub struct Table<S: TablePageStore> {
    name: String,
    schema: Schema,
    store: S,
    tree: BTree<S>,
}

impl<S: TablePageStore> Table<S> {
    // Create a new table, which is expected not to exist yet.
    pub fn create(name: String, schema: Schema, mut store: S) -> Result<Table<S>, Error> {
        if name.len() > 255 {
            return Err(Error::NameTooLong);
        }

        //if Self::load(name.clone(), store.clone())?.is_some() {
        //    return Err(Error::TableExists);
        //}

        let tree = BTree::new(name.clone(), schema.clone(), store.clone())?;
        store.put(0, encode_table_header(&name, &schema, &tree))?;

        Ok(Table {
            name,
            schema,
            store,
            tree,
        })
    }

    // Load an extant table.
    pub fn load(name: String, store: S) -> Result<Option<Table<S>>, Error> {
        todo!()
    }
}

// The table header is stored in page 0 of the table page store. It has the following format,
// starting at byte offset 0:
//   - 1 byte which stores the length of the table name
//   - the table name bytes
//   - 2 bytes to store the length of the encoded schema
//   - the schema as encoded bytes, the schema is self-decoding
//   - 2 bytes to store the length of the encoded btree root information
//   - the encoded btree information, it is most likely just the order and root, but more may
//     be added
//   - TBD, probably index information
fn encode_table_header<S: TablePageStore>(name: &str, schema: &Schema, tree: &BTree<S>) -> Vec<u8> {
    // guess at the capacity
    let mut header_bytes = Vec::with_capacity(13 + name.as_bytes().len() + 4 * schema.len());

    header_bytes
        .write_all(&(name.len() as u8).to_be_bytes())
        .expect("can't fail writing to vec");
    header_bytes
        .write_all(name.as_bytes())
        .expect("can't fail writing to vec");

    let schema_bytes = schema.encode();
    header_bytes
        .write_all(&(schema_bytes.len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");
    header_bytes
        .write_all(&schema_bytes)
        .expect("can't fail writing to vec");

    let btree_bytes = tree.encode();
    header_bytes
        .write_all(&(btree_bytes.len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");
    header_bytes
        .write_all(&btree_bytes)
        .expect("can't fail writing to vec");

    header_bytes
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pagestore::Memory;
    use crate::schema::{ColumnType, Schema};

    #[test]
    fn test_create() {
        let name = "babies_first_table".to_string();
        let schema = Schema::new(vec![
            ("sugar".into(), ColumnType::Int),
            ("spice".into(), ColumnType::VarChar(10)),
            ("everything_nice".into(), ColumnType::Bool),
        ])
        .unwrap();
        let store = Memory::new(1024 * 64);

        let table = Table::create(name.clone(), schema.clone(), store.clone()).unwrap();

        assert_eq!(&name, &table.name);
        assert_eq!(&schema, &table.schema);

        let table_header_page = store.get(0).unwrap();
        assert_eq!(
            vec![
                18, b'b', b'a', b'b', b'i', b'e', b's', b'_', b'f', b'i', b'r', b's', b't', b'_',
                b't', b'a', b'b', b'l', b'e', 0, 38, 0, 3, 1, 0, 5, 115, 117, 103, 97, 114, 0, 0,
                5, 115, 112, 105, 99, 101, 0, 10, 2, 0, 15, 101, 118, 101, 114, 121, 116, 104, 105,
                110, 103, 95, 110, 105, 99, 101, 0, 8, 0, 0, 0, 65, 0, 0, 0, 0
            ],
            table_header_page
        );
    }
}
