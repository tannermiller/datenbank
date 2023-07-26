use std::collections::HashMap;
use std::rc::Rc;

use crate::pagestore::{Error as PageError, TablePageStore, TablePageStoreBuilder};
use crate::row::{Error as RowError, Predicate, Row, RowBytes, RowCol};
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
    #[error("row error")]
    Row(#[from] RowError),
}

// A Table is responsible for the management of everything pertaining to a single database table's
// data. It manages the table btree, any indices as well as the persistence of the table.
#[derive(Debug)]
pub struct Table<S: TablePageStore> {
    name: Rc<String>,
    schema: Schema,
    store: S,
    primary: BTree<S>,
    secondaries: Vec<BTree<S>>,
}

impl<S: TablePageStore> Table<S> {
    // Create a new table, which is expected not to exist yet.
    pub fn create<SB: TablePageStoreBuilder<PageStore = S>>(
        name: String,
        schema: Schema,
        store_builder: &mut SB,
    ) -> Result<Table<S>, Error> {
        if name.len() > 255 {
            return Err(Error::NameTooLong);
        }
        let name = Rc::new(name);

        let mut store = store_builder.build()?;

        let primary = BTree::new(name.clone(), schema.clone(), store_builder)?;

        let secondaries = schema
            .index_schemas()
            .into_iter()
            .map(|(idx_name, idx_schema)| {
                BTree::new(idx_name, idx_schema, store_builder).map_err(Into::into)
            })
            .collect::<Result<Vec<BTree<S>>, Error>>()?;

        store.put(0, header::encode(&name, &schema, &primary, &secondaries))?;

        Ok(Table {
            name,
            schema,
            store,
            primary,
            secondaries,
        })
    }

    // Load an extant table.
    pub fn load<SB: TablePageStoreBuilder<PageStore = S>>(
        store_builder: &mut SB,
    ) -> Result<Option<Table<S>>, Error> {
        let mut store = store_builder.build()?;
        let header_page = store.get(0)?;
        if header_page.is_empty() {
            return Ok(None);
        }

        let (name, schema, primary, secondaries) = header::decode(&header_page, store_builder)?;

        Ok(Some(Table {
            name: name.into(),
            schema,
            store,
            primary,
            secondaries,
        }))
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schemas(&self) -> Vec<(Rc<String>, &Schema)> {
        let mut schemas = Vec::with_capacity(self.secondaries.len() + 1);
        schemas.push((self.name.clone(), &self.schema));
        schemas.extend(self.secondaries.iter().map(|s| (s.name.clone(), &s.schema)));
        schemas
    }

    // Insert the values into the specified columns. Returns the number of rows updated.
    pub fn insert(&mut self, columns: &[&str], values: Vec<Vec<Column>>) -> Result<usize, Error> {
        // For now, we only support inserting fully specififed rows, so we need to ensure that
        // every column is present in our inserted columns.
        let values_in_order = self.schema.put_columns_in_order(columns, values)?;

        let (inserted_rows, mut root_updated) = self.primary.insert(values_in_order)?;

        let col_index_lookup: HashMap<&Rc<String>, usize> = self
            .schema
            .columns()
            .iter()
            .enumerate()
            .map(|(i, (col_name, _))| (col_name, i))
            .collect();

        for secondary in &mut self.secondaries {
            let secondary_root_updated =
                insert_secondary(secondary, &self.schema, &col_index_lookup, &inserted_rows)?;
            root_updated = root_updated || secondary_root_updated;
        }

        if root_updated {
            self.store.put(
                0,
                header::encode(&self.name, &self.schema, &self.primary, &self.secondaries),
            )?;
        }

        Ok(inserted_rows.len())
    }

    // Scan the entire table for and return the values for every row for the provided columns.
    pub fn scan(
        &mut self,
        columns: &[Rc<String>],
        rp: impl Predicate<S>,
    ) -> Result<Vec<Vec<Column>>, Error> {
        self.primary.scan(columns, rp).map_err(Into::into)
    }

    pub fn lookup(
        &mut self,
        key: &Vec<u8>,
        columns: &[Rc<String>],
    ) -> Result<Option<Vec<Column>>, Error> {
        self.primary.lookup(key, columns).map_err(Into::into)
    }

    pub fn lookup_via_index(
        &mut self,
        name: &Rc<String>,
        key: &Vec<u8>,
        columns: &[Rc<String>],
    ) -> Result<Option<Vec<Column>>, Error> {
        if name == &self.name {
            return self.lookup(key, columns);
        }

        for secondary in &mut self.secondaries {
            if &secondary.name != name {
                continue;
            }

            let pk = secondary
                // TODO: This key slice could be set up with a lazy_static if we care
                .lookup(key, &[Rc::new("key".to_string())])?
                .and_then(|key_row| key_row.into_iter().last());

            if let Some(Column::LongBlob(pk)) = pk {
                return self.lookup(&pk, columns);
            }
        }

        Ok(None)
    }
}

fn insert_secondary<S: TablePageStore>(
    secondary: &mut BTree<S>,
    schema: &Schema,
    col_index_lookup: &HashMap<&Rc<String>, usize>,
    inserted_rows: &[Row],
) -> Result<bool, Error> {
    let mut index_rows = Vec::with_capacity(inserted_rows.len());
    for row in inserted_rows {
        let index_row = secondary
            .schema
            .columns()
            .iter()
            .map(|(col_name, _)| {
                if **col_name == "key" {
                    Column::LongBlob(row.key(&schema))
                } else {
                    let i = col_index_lookup.get(col_name).unwrap();
                    match row.body[*i] {
                        RowCol::Int(col_int) => Column::Int(col_int),
                        RowCol::Bool(b) => Column::Bool(b),
                        RowCol::VarChar(RowBytes { ref inline, .. }) => Column::VarChar(
                            String::from_utf8(inline.clone())
                                .expect("stored bytes are valid string"),
                        ),
                        RowCol::LongBlob(RowBytes { ref inline, .. }) => {
                            Column::LongBlob(inline.clone())
                        }
                    }
                }
            })
            .collect();
        index_rows.push(index_row);
    }

    secondary
        .insert(index_rows)
        .map(|(_, root_updated)| root_updated)
        .map_err(Into::into)
}

#[cfg(test)]
mod test {
    use super::btree::node::encode::decode_node;
    use super::btree::node::NodeBody;
    use super::*;
    use crate::pagestore::{Memory, MemoryManager, TablePageStoreBuilder, TablePageStoreManager};
    use crate::row::AllRows;
    use crate::schema::{ColumnType, Schema};

    #[test]
    fn test_create_and_load() {
        let name = "babies_first_table".to_string();
        let schema = Schema::new(
            vec![
                ("sugar".into(), ColumnType::Int),
                ("spice".into(), ColumnType::VarChar(10)),
                ("everything_nice".into(), ColumnType::Bool),
            ],
            None,
            vec![
                ("su_sp", vec!["sugar", "spice"]),
                ("ev_sp", vec!["everything_nice", "spice"]),
            ],
        )
        .unwrap();
        let mut store_builder = MemoryManager::new(1024 * 64).builder(&name).unwrap();

        let load_result = Table::load(&mut store_builder);
        assert!(load_result.is_ok());
        assert!(load_result.unwrap().is_none());

        {
            let table = Table::create(name.clone(), schema.clone(), &mut store_builder).unwrap();

            assert_eq!(&name, &*table.name);
            assert_eq!(&schema, &table.schema);

            let mut store = store_builder.build().unwrap();
            let table_header_page = store.get(0).unwrap();
            assert_eq!(
                vec![
                    18, 98, 97, 98, 105, 101, 115, 95, 102, 105, 114, 115, 116, 95, 116, 97, 98,
                    108, 101, 0, 98, 0, 3, 1, 0, 5, 115, 117, 103, 97, 114, 0, 0, 5, 115, 112, 105,
                    99, 101, 0, 10, 2, 0, 15, 101, 118, 101, 114, 121, 116, 104, 105, 110, 103, 95,
                    110, 105, 99, 101, 0, 0, 0, 2, 0, 5, 115, 117, 95, 115, 112, 0, 2, 0, 5, 115,
                    117, 103, 97, 114, 0, 5, 115, 112, 105, 99, 101, 0, 5, 101, 118, 95, 115, 112,
                    0, 2, 0, 15, 101, 118, 101, 114, 121, 116, 104, 105, 110, 103, 95, 110, 105,
                    99, 101, 0, 5, 115, 112, 105, 99, 101, 0, 8, 0, 0, 7, 255, 0, 0, 0, 0, 0, 2, 0,
                    8, 0, 0, 1, 141, 0, 0, 0, 0, 0, 8, 0, 0, 1, 148, 0, 0, 0, 0
                ],
                table_header_page
            );

            let (decoded_name, decoded_schema, decoded_btree, decoded_secondaries) =
                header::decode(&table_header_page, &mut store_builder).unwrap();
            assert_eq!(name, *decoded_name);
            assert_eq!(schema, decoded_schema);
            assert_eq!(table.primary.order, decoded_btree.order);
            assert_eq!(table.primary.root, decoded_btree.root);
            assert_eq!(2, decoded_secondaries.len());
            for i in 0..2 {
                assert_eq!(table.secondaries[i].order, decoded_secondaries[i].order);
                assert_eq!(table.secondaries[i].root, decoded_secondaries[i].root);
            }
        }

        let loaded_table = Table::load(&mut store_builder).unwrap().unwrap();
        assert_eq!(name, *loaded_table.name);
        assert_eq!(schema, loaded_table.schema);
    }

    #[test]
    fn test_insert_no_indices() {
        let name = "inserting_is_awesome".to_string();
        let schema = Schema::new(
            vec![
                ("zero".into(), ColumnType::Int),
                ("one".into(), ColumnType::VarChar(10)),
                ("infinity".into(), ColumnType::Bool),
            ],
            None,
            vec![],
        )
        .unwrap();
        let mut store_builder = MemoryManager::new(1024 * 64).builder(&name).unwrap();

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

        let mut store = store_builder.build().unwrap();
        let root_id = {
            let table_header_page = store.get(0).unwrap();
            let (_, _, decoded_btree, _) =
                header::decode(&table_header_page, &mut store_builder).unwrap();
            decoded_btree.root.unwrap()
        };

        load_root_leaf_and_check_rows(
            &mut store,
            root_id,
            vec![vec![
                RowCol::Int(7),
                RowCol::VarChar(RowBytes {
                    inline: vec![b'h', b'o', b'l', b'a'],
                    next_page: None,
                }),
                RowCol::Bool(false),
            ]],
        );
    }

    fn load_root_leaf_and_check_rows(store: &mut Memory, root: usize, exp_rows: Vec<Vec<RowCol>>) {
        let index_node_bytes = store.get(root).unwrap();
        let root_node = decode_node(&index_node_bytes).unwrap();
        let leaf = match root_node.body {
            NodeBody::Internal(_) => panic!("got internal secondary root node"),
            NodeBody::Leaf(l) => l,
        };

        assert!(leaf.right_sibling.is_none());
        assert_eq!(exp_rows.len(), leaf.rows.len());
        for (exp_row, row) in exp_rows.iter().zip(leaf.rows.iter()) {
            assert_eq!(exp_row.len(), row.body.len());
            assert_eq!(exp_row, &row.body);
        }
    }

    #[test]
    fn test_insert_with_indices() {
        let name = "inserting_is_awesome".to_string();
        let schema = Schema::new(
            vec![
                ("zero".into(), ColumnType::Int),
                ("one".into(), ColumnType::VarChar(10)),
                ("infinity".into(), ColumnType::Bool),
            ],
            None,
            vec![
                ("idx_one_zero", vec!["one", "zero"]),
                ("idx_zero_infinity", vec!["zero", "infinity"]),
            ],
        )
        .unwrap();

        let mut store_builder = MemoryManager::new(1024 * 64).builder(&name).unwrap();

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

        let mut store = store_builder.build().unwrap();
        let table_header_page = store.get(0).unwrap();
        let (_, _, primary, secondaries) =
            header::decode(&table_header_page, &mut store_builder).unwrap();

        load_root_leaf_and_check_rows(
            &mut store,
            primary.root.unwrap(),
            vec![vec![
                RowCol::Int(7),
                RowCol::VarChar(RowBytes {
                    inline: b"hola".to_vec(),
                    next_page: None,
                }),
                RowCol::Bool(false),
            ]],
        );

        let secondary_rows = vec![
            vec![vec![
                RowCol::VarChar(RowBytes {
                    inline: b"hola".to_vec(),
                    next_page: None,
                }),
                RowCol::Int(7),
                RowCol::LongBlob(RowBytes {
                    inline: vec![0, 0, 0, 7, b'_', b'h', b'o', b'l', b'a', b'_', 0],
                    next_page: None,
                }),
            ]],
            vec![vec![
                RowCol::Int(7),
                RowCol::Bool(false),
                RowCol::LongBlob(RowBytes {
                    inline: vec![0, 0, 0, 7, b'_', b'h', b'o', b'l', b'a', b'_', 0],
                    next_page: None,
                }),
            ]],
        ];

        for (secondary, exp_rows) in secondaries.iter().zip(secondary_rows.into_iter()) {
            load_root_leaf_and_check_rows(&mut store, secondary.root.unwrap(), exp_rows);
        }
    }

    // TODO: This big test to should be moved to the integration tests.
    #[test]
    fn test_big_table() {
        let name = "big_inserts".to_string();
        let schema = Schema::new(
            vec![
                ("one".into(), ColumnType::Int),
                ("two".into(), ColumnType::VarChar(512)),
                ("three".into(), ColumnType::Bool),
            ],
            None,
            vec![],
        )
        .unwrap();
        let mut store_builder = MemoryManager::new(1024 * 16).builder(&name).unwrap();
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

        let mut store = store_builder.build().unwrap();
        let root_id = {
            let table_header_page = store.get(0).unwrap();
            let (_, _, decoded_btree, _) =
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
                &vec!["one", "two", "three"]
                    .into_iter()
                    .map(|s| s.to_string().into())
                    .collect::<Vec<Rc<String>>>(),
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

    #[test]
    fn test_lookup_via_index() {
        let name = "index_lookup_GOOOOOO".to_string();
        let schema = Schema::new(
            vec![
                ("im".into(), ColumnType::Int),
                ("so".into(), ColumnType::Int),
                ("very".into(), ColumnType::Bool),
                ("cool".into(), ColumnType::Int),
            ],
            Some(vec!["im", "so"]),
            vec![
                ("idx_so_im", vec!["so", "im"]),
                ("idx_very_so", vec!["very", "so"]),
            ],
        )
        .unwrap();

        let mut store_builder = MemoryManager::new(1024 * 64).builder(&name).unwrap();

        let mut table = Table::create(name.clone(), schema.clone(), &mut store_builder).unwrap();

        let rows_affected = table
            .insert(
                &["im", "so", "very", "cool"],
                vec![
                    vec![
                        Column::Int(1),
                        Column::Int(2),
                        Column::Bool(false),
                        Column::Int(3),
                    ],
                    vec![
                        Column::Int(4),
                        Column::Int(5),
                        Column::Bool(false),
                        Column::Int(6),
                    ],
                    vec![
                        Column::Int(7),
                        Column::Int(8),
                        Column::Bool(false),
                        Column::Int(9),
                    ],
                ],
            )
            .unwrap();
        assert_eq!(3, rows_affected);

        let all_cols: Vec<Rc<String>> = vec!["im", "so", "very", "cool"]
            .into_iter()
            .map(|s| s.to_string().into())
            .collect();

        assert_eq!(
            vec![
                Column::Int(1),
                Column::Int(2),
                Column::Bool(false),
                Column::Int(3),
            ],
            table
                .lookup_via_index(
                    &Rc::new(name),
                    &vec![0, 0, 0, 1, b'_', 0, 0, 0, 2],
                    &all_cols
                )
                .unwrap()
                .unwrap()
        );

        assert_eq!(
            vec![
                Column::Int(4),
                Column::Int(5),
                Column::Bool(false),
                Column::Int(6),
            ],
            table
                .lookup_via_index(
                    &Rc::new("idx_so_im".to_string()),
                    &vec![0, 0, 0, 5, b'_', 0, 0, 0, 4],
                    &all_cols,
                )
                .unwrap()
                .unwrap()
        );

        assert_eq!(
            vec![
                Column::Int(7),
                Column::Int(8),
                Column::Bool(false),
                Column::Int(9),
            ],
            table
                .lookup_via_index(
                    &Rc::new("idx_very_so".to_string()),
                    &vec![0, b'_', 0, 0, 0, 8],
                    &all_cols,
                )
                .unwrap()
                .unwrap()
        );
    }
}
