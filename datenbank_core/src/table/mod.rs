use crate::pagestore::{Error as PageError, TablePageStore};
use crate::schema::Schema;
use btree::{BTree, Error as BTreeError};

mod btree;
mod header;

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
    #[error("error decoding tree: {0}")]
    DecodingError(String),
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

        let tree = BTree::new(name.clone(), schema.clone(), store.clone())?;
        store.put(0, header::encode(&name, &schema, &tree))?;

        Ok(Table {
            name,
            schema,
            store,
            tree,
        })
    }

    // Load an extant table.
    pub fn load(name: String, store: S) -> Result<Option<Table<S>>, Error> {
        // TODO: read the table header page and see if there's anything there, if not then return
        // Ok(None)
        todo!()
    }
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

        let (decoded_name, decoded_schema, decoded_btree) =
            header::decode(&table_header_page, store).unwrap();
        assert_eq!(name, decoded_name);
        assert_eq!(schema, decoded_schema);
        assert_eq!(table.tree.order, decoded_btree.order);
        assert_eq!(table.tree.root, decoded_btree.root);
    }
}
