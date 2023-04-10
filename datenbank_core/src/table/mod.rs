use crate::pagestore::{Error as PageError, TablePageStore, TablePageStoreBuilder};
use crate::schema::{Column, Error as SchemaError, Schema};
use btree::row::Row;
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
pub struct Table<S: TablePageStore> {
    name: String,
    schema: Schema,
    store: S,
    tree: BTree<S>,
}

impl<S: TablePageStore> Table<S> {
    // Create a new table, which is expected not to exist yet.
    pub fn create<SB: TablePageStoreBuilder<TablePageStore = S>>(
        name: String,
        schema: Schema,
        store_builder: &mut SB,
    ) -> Result<Table<S>, Error> {
        if name.len() > 255 {
            return Err(Error::NameTooLong);
        }

        let mut store = store_builder.build(&name)?;

        let tree = BTree::new(name.clone(), schema.clone(), store_builder)?;
        store.put(0, header::encode(&name, &schema, &tree))?;

        Ok(Table {
            name,
            schema,
            store,
            tree,
        })
    }

    // Load an extant table.
    pub fn load<SB: TablePageStoreBuilder<TablePageStore = S>>(
        name: &str,
        store_builder: &mut SB,
    ) -> Result<Option<Table<S>>, Error> {
        let mut store = store_builder.build(name)?;
        let header_page = store.get(0)?;
        if header_page.is_empty() {
            return Ok(None);
        }

        let (name, schema, tree) = header::decode(&header_page, store_builder)?;

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

        let (rows_affected, root_updated) = self.tree.insert(values_in_order)?;

        if root_updated {
            self.store
                .put(0, header::encode(&self.name, &self.schema, &self.tree))?;
        }

        Ok(rows_affected)
    }

    // Scan the entire table for and return the values for every row for the provided columns.
    pub fn scan(
        &mut self,
        columns: Vec<String>,
        rp: impl RowPredicate,
    ) -> Result<Vec<Vec<Column>>, Error> {
        self.tree.scan(columns, rp).map_err(Into::into)
    }
}

// A RowPredicate is used to select rows to return during a scan;
pub trait RowPredicate {
    fn is_satisfied_by(&self, row: &Row) -> bool;
}

// AllRows is a RowPredicate that matches, and therefore returns, all rows in a table.
pub struct AllRows;

impl RowPredicate for AllRows {
    fn is_satisfied_by(&self, _: &Row) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use super::btree::node::encode::decode_node;
    use super::btree::node::NodeBody;
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
            let table = Table::create(name.clone(), schema.clone(), &mut store_builder).unwrap();

            assert_eq!(&name, &table.name);
            assert_eq!(&schema, &table.schema);

            let mut store = store_builder.build(&name).unwrap();
            let table_header_page = store.get(0).unwrap();
            assert_eq!(
                vec![
                    18, b'b', b'a', b'b', b'i', b'e', b's', b'_', b'f', b'i', b'r', b's', b't',
                    b'_', b't', b'a', b'b', b'l', b'e', 0, 38, 0, 3, 1, 0, 5, 115, 117, 103, 97,
                    114, 0, 0, 5, 115, 112, 105, 99, 101, 0, 10, 2, 0, 15, 101, 118, 101, 114, 121,
                    116, 104, 105, 110, 103, 95, 110, 105, 99, 101, 0, 8, 0, 0, 7, 255, 0, 0, 0, 0
                ],
                table_header_page
            );

            let (decoded_name, decoded_schema, decoded_btree) =
                header::decode(&table_header_page, &mut store_builder).unwrap();
            assert_eq!(name, decoded_name);
            assert_eq!(schema, decoded_schema);
            assert_eq!(table.tree.order, decoded_btree.order);
            assert_eq!(table.tree.root, decoded_btree.root);
        }

        let load_result = Table::load("not_found", &mut store_builder);
        assert!(load_result.is_ok());
        assert!(load_result.unwrap().is_none());

        let loaded_table = Table::load(&name, &mut store_builder).unwrap().unwrap();
        assert_eq!(name, loaded_table.name);
        assert_eq!(schema, loaded_table.schema);
    }

    #[test]
    fn test_insert() {
        let name = "inserting_is_awesome".to_string();
        let schema = Schema::new(vec![
            ("zero".into(), ColumnType::Int),
            ("one".into(), ColumnType::VarChar(10)),
            ("infinity".into(), ColumnType::Bool),
        ])
        .unwrap();
        let mut store_builder = MemoryBuilder::new(1024 * 64);

        let mut table = Table::create(name.clone(), schema.clone(), &mut store_builder).unwrap();

        let rows_affected = table
            .insert(
                &["zero", "one", "infinity"],
                vec![vec![
                    Column::Int(7),
                    Column::VarChar("hola".to_string()),
                    Column::Bool(false),
                ]],
            )
            .unwrap();
        assert_eq!(1, rows_affected);

        let mut store = store_builder.build(&name).unwrap();
        let root_id = {
            let table_header_page = store.get(0).unwrap();
            let (_, _, decoded_btree) =
                header::decode(&table_header_page, &mut store_builder).unwrap();
            decoded_btree.root.unwrap()
        };

        let root_node_bytes = store.get(root_id).unwrap();
        let root_node = decode_node(&root_node_bytes).unwrap();
        let leaf = match root_node.body {
            NodeBody::Internal(_) => panic!("got internal root node"),
            NodeBody::Leaf(l) => l,
        };

        assert!(leaf.right_sibling.is_none());
        assert_eq!(1, leaf.rows.len());
        assert_eq!(3, leaf.rows[0].body.len());
    }

    #[test]
    fn test_big_table() {
        let name = "big_inserts".to_string();
        let schema = Schema::new(vec![
            ("one".into(), ColumnType::Int),
            ("two".into(), ColumnType::VarChar(512)),
            ("three".into(), ColumnType::Bool),
        ])
        .unwrap();
        let mut store_builder = MemoryBuilder::new(1024 * 16);
        let mut table = Table::create(name.clone(), schema.clone(), &mut store_builder).unwrap();

        let big_num = 10_000;
        let mut rows = Vec::with_capacity(big_num);
        for i in 0..big_num {
            rows.push(vec![
                Column::Int(i as i32),
                Column::VarChar((i % 10).to_string().repeat(100)),
                Column::Bool(i % 2 == 0),
            ]);
        }

        let rows_affected = table.insert(&["one", "two", "three"], rows).unwrap();

        assert_eq!(big_num, rows_affected);

        let mut store = store_builder.build(&name).unwrap();
        let root_id = {
            let table_header_page = store.get(0).unwrap();
            let (_, _, decoded_btree) =
                header::decode(&table_header_page, &mut store_builder).unwrap();
            decoded_btree.root.unwrap()
        };

        let root_node_bytes = store.get(root_id).unwrap();
        let root_node = decode_node(&root_node_bytes).unwrap();
        assert_eq!(30, root_node.order);

        let internal = match root_node.body {
            NodeBody::Internal(i) => i,
            NodeBody::Leaf(_) => panic!("got leaf root node"),
        };

        assert!(!internal.children.is_empty());

        let values = table
            .scan(
                vec!["one".to_string(), "two".to_string(), "three".to_string()],
                AllRows,
            )
            .unwrap();

        assert_eq!(big_num, values.len());

        for (i, vs) in values.iter().enumerate() {
            let i = i as i32;
            assert_eq!(3, vs.len());
            assert_eq!(Column::Int(i), vs[0]);
            assert_eq!(Column::VarChar((i % 10).to_string().repeat(100)), vs[1]);
            assert_eq!(Column::Bool(i % 2 == 0), vs[2]);
        }
    }
}
