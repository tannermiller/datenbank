use crate::pagestore::{Error as PageError, TablePageStore, TablePageStoreBuilder};
use crate::schema::{Column, Error as SchemaError, Schema};
use btree::{BTree, Error as BTreeError};

pub(crate) mod btree;
mod header;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("io error attempting operation")]
    Io(#[from] PageError),
    #[error("cannot create table that already exists")]
    TableExists,
    #[error("error from btree")]
    BTree(#[from] BTreeError),
    #[error("schema error")]
    Schema(#[from] SchemaError),
    #[error("table name must be <= 255 chars")]
    NameTooLong,
    #[error("error decoding tree: {0}")]
    DecodingError(String),
    #[error("unable to insert row: {0}")]
    InvalidInsert(String),
}

// A Table is responsible for the management of everything pertaining to a single database table's
// data. It manages the table btree, any indices as well as the persistence of the table.
#[derive(Debug)]
pub struct Table<B: TablePageStoreBuilder> {
    name: String,
    schema: Schema,
    store: B::TablePageStore,
    tree: BTree<B::TablePageStore>,
}

impl<B: TablePageStoreBuilder> Table<B> {
    // Create a new table, which is expected not to exist yet.
    pub fn create(name: String, schema: Schema, mut store_builder: B) -> Result<Table<B>, Error> {
        if name.len() > 255 {
            return Err(Error::NameTooLong);
        }

        let mut store = store_builder.build(&name)?;

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
    pub fn load(name: &str, mut store_builder: B) -> Result<Option<Table<B>>, Error> {
        let store = store_builder.build(name)?;
        let header_page = store.get(0)?;
        if header_page.is_empty() {
            return Ok(None);
        }

        let (name, schema, tree) = header::decode(&header_page, store.clone())?;

        Ok(Some(Table {
            name,
            schema,
            store,
            tree,
        }))
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    // Insert the values into the specified columns. Returns the number of rows updated.
    pub fn insert(&mut self, columns: &[&str], values: Vec<Vec<Column>>) -> Result<usize, Error> {
        // For now, we only support inserting fully specififed rows, so we need to ensure that
        // every column is present in our inserted columns.
        let values_in_order = self.schema.put_columns_in_order(columns, values)?;

        self.tree.insert(values_in_order).map_err(Into::into)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pagestore::MemoryBuilder;
    use crate::schema::{ColumnType, Schema};

    #[test]
    fn test_create_and_load() {
        let name = "babies_first_table".to_string();
        let schema = Schema::new(vec![
            ("sugar".into(), ColumnType::Int),
            ("spice".into(), ColumnType::VarChar(10)),
            ("everything_nice".into(), ColumnType::Bool),
        ])
        .unwrap();
        let mut store_builder = MemoryBuilder::new(1024 * 64);

        {
            let table = Table::create(name.clone(), schema.clone(), store_builder.clone()).unwrap();

            assert_eq!(&name, &table.name);
            assert_eq!(&schema, &table.schema);

            let store = store_builder.build(&name).unwrap();
            let table_header_page = store.get(0).unwrap();
            assert_eq!(
                vec![
                    18, b'b', b'a', b'b', b'i', b'e', b's', b'_', b'f', b'i', b'r', b's', b't',
                    b'_', b't', b'a', b'b', b'l', b'e', 0, 38, 0, 3, 1, 0, 5, 115, 117, 103, 97,
                    114, 0, 0, 5, 115, 112, 105, 99, 101, 0, 10, 2, 0, 15, 101, 118, 101, 114, 121,
                    116, 104, 105, 110, 103, 95, 110, 105, 99, 101, 0, 8, 0, 0, 0, 65, 0, 0, 0, 0
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

        let load_result = Table::load("not_found", store_builder.clone());
        assert!(load_result.is_ok());
        assert!(load_result.unwrap().is_none());

        let loaded_table = Table::load(&name, store_builder.clone()).unwrap().unwrap();
        assert_eq!(name, loaded_table.name);
        assert_eq!(schema, loaded_table.schema);
    }
}
