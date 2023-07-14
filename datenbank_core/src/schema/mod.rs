use std::collections::HashMap;
use std::rc::Rc;

use crate::key::MAX_KEY_VAR_CHAR_LEN;
use crate::parser::{Literal, SelectColumns};

mod encode;

pub use encode::decode;

// The maximum length that any variable length field will store inline in a row. Any amount over
// this amount will be stored external to the leaf page.
pub const MAX_INLINE_VAR_LEN_COL_SIZE: usize = 512;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("column names must be unique, found duplicate {0}")]
    NonUniqueColumn(String),
    #[error("unable to decode schema: {0}")]
    UnableToDecode(String),
    #[error("invalid column {0}")]
    InvalidColumn(String),
    #[error("invalid primary key column {0}")]
    InvalidPrimaryKeyColumn(String),
    #[error("invalid index column {0}")]
    InvalidIndexColumn(String),
}

// The primary key contains the column names in order as well as the indices relative to the table
// columns those fields correspond.
#[derive(Clone, Debug, PartialEq)]
pub struct PrimaryKey {
    column_names: Vec<Rc<String>>,
    column_indices: Vec<usize>,
}

impl PrimaryKey {
    fn new(table_columns: &[(Rc<String>, ColumnType)], column_names: Vec<Rc<String>>) -> Self {
        let mut column_indices = Vec::with_capacity(column_names.len());
        for pk_col_name in column_names.iter() {
            for (i, (col_name, _)) in table_columns.iter().enumerate() {
                if *pk_col_name == *col_name {
                    column_indices.push(i);
                    break;
                }
            }
        }
        PrimaryKey {
            column_names,
            column_indices,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    name: String,
    columns: Vec<Rc<String>>,
}

// The schema describes the columns that make each row in the table.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    // each column has a name a data type
    columns: Vec<(Rc<String>, ColumnType)>,
    primary_key: Option<PrimaryKey>,
    indices: Vec<Index>,
}

impl Schema {
    // Create a new Schema. Validate that the schema is valid.
    pub fn new(
        column_definitions: Vec<(String, ColumnType)>,
        primary_key_def: Option<Vec<&str>>,
        indices_def: Vec<(&str, Vec<&str>)>,
    ) -> Result<Self, Error> {
        let mut col_names = HashMap::new();
        let mut columns = Vec::with_capacity(column_definitions.len());
        for (name, ct) in column_definitions
            .into_iter()
            .map(|(s, ct)| (Rc::new(s), ct))
        {
            if col_names.insert(name.to_string(), name.clone()).is_some() {
                return Err(Error::NonUniqueColumn(name.to_string()));
            }
            columns.push((name.clone(), ct));
        }

        let primary_key = match primary_key_def {
            Some(pks) => {
                let mut primary_cols = Vec::with_capacity(pks.len());
                for pk in pks {
                    match col_names.get(pk) {
                        Some(col) => primary_cols.push(col.clone()),
                        None => return Err(Error::InvalidPrimaryKeyColumn(pk.to_string())),
                    }
                }
                Some(PrimaryKey::new(&columns, primary_cols))
            }
            None => None,
        };

        let indices = indices_def
            .into_iter()
            .map(|(name, cols)| {
                let columns = cols
                    .into_iter()
                    .map(|col_name| match col_names.get(col_name) {
                        Some(col) => Ok(col.clone()),
                        None => Err(Error::InvalidIndexColumn(col_name.to_string())),
                    })
                    .collect::<Result<Vec<Rc<String>>, Error>>()?;
                Ok(Index {
                    name: name.into(),
                    columns,
                })
            })
            .collect::<Result<Vec<Index>, Error>>()?;

        Ok(Schema {
            columns,
            primary_key,
            indices,
        })
    }

    // The encoded Schema has the following format:
    //   - 2 bytes which store the number of schema items
    //   - each item is then encoded as:
    //     - each item begins with 1 byte which indicates the type of the column
    //     - 2 bytes for the length of the name of the column
    //     - the bytes for the name of the column
    //     - for statically sized columns (e.g. Int, Bool), nothing else is encoded
    //     - for variable length columns (e.g. VarChar), the max len is encoded in however many
    //       bytes it requires
    //   - 2 bytes for the number of primary key items
    //   - each primary key entry is encoded as:
    //     - 2 bytes for the length of the name of the column
    //     - the bytes for the name of the column
    //  - 2 bytes for the number of indices
    //  - each index is then encoded as:
    //    - 2 bytes for the length of the index name
    //    - the bytes for the name of the index
    //    - 2 bytes for count of column names
    //    - each column name is encoded as:
    //      - 2 bytes for the length of the column name
    //      - the bytes of the column name
    pub fn encode(&self) -> Vec<u8> {
        encode::encode(self)
    }

    // Validate and convert the set of column names with sets of values in the same order against
    // this schema.
    pub fn literals_to_columns(
        &self,
        column_order: &[&str],
        literals: Vec<Vec<Literal>>,
    ) -> Result<Vec<Vec<Column>>, Error> {
        if column_order.is_empty() {
            return Err(Error::InvalidColumn(
                "column names must be non-empty".to_string(),
            ));
        }

        if literals.is_empty() {
            return Err(Error::InvalidColumn(
                "literals must be non-empty".to_string(),
            ));
        }

        // ensure the column names are valid and find their location in self.columns so we don't
        // have to iterate them more
        let mut col_types_in_order = Vec::with_capacity(column_order.len());
        for column in column_order {
            let mut found_column = None;
            for (col_name, col) in self.columns.iter() {
                if &**col_name == column {
                    found_column = Some(col);
                    break;
                }
            }

            if let Some(col) = found_column {
                col_types_in_order.push(col);
            } else {
                return Err(Error::InvalidColumn(column.to_string()));
            }
        }

        let mut column_values = Vec::with_capacity(literals.len());
        for lit_value in literals {
            if lit_value.len() != col_types_in_order.len() {
                return Err(Error::InvalidColumn(format!(
                    "({})",
                    lit_value
                        .into_iter()
                        .map(|l| l.to_string())
                        .collect::<Vec<String>>()
                        .join(",")
                )));
            }

            let col_vals = col_types_in_order
                .iter()
                .zip(lit_value.into_iter())
                .map(|(col_type, literal)| match (col_type, literal) {
                    (ColumnType::VarChar(max_size), Literal::String(s))
                        if s.len() <= *max_size as usize =>
                    {
                        Ok(Column::VarChar(s))
                    }

                    (ColumnType::Int, Literal::Int(i)) => Ok(Column::Int(i)),

                    (ColumnType::Bool, Literal::Bool(b)) => Ok(Column::Bool(b)),

                    (ColumnType::LongBlob(max_size), Literal::Bytes(bs))
                        if bs.len() <= *max_size as usize =>
                    {
                        Ok(Column::LongBlob(bs))
                    }

                    (ct, l) => Err(Error::InvalidColumn(format!(
                        "{l} is not congruent to column type {ct}",
                    ))),
                })
                .collect::<Result<Vec<Column>, _>>()?;

            column_values.push(col_vals);
        }

        Ok(column_values)
    }

    // Determine the maximum size of a row inline in the leaf node. Anything over this size will be
    // stored externally to the row.
    pub fn max_inline_row_size(&self) -> usize {
        // 4 for overall encoded row size
        // one per column, to determine the type
        // for varchar:
        //   * 2 for inline len
        //   * N for the acutal len
        //   * 4 for page of continued value
        // for int: 4 bytes
        // for bool: 1 bytes
        // for longblob:
        //   * 2 for inline len
        //   * N for the acutal len
        //   * 4 for page of continued value
        self.columns.iter().fold(0, |acc, (_, col)| match col {
            // size of type flag (1 byte) + len (2 bytes) + size of data itself + next page pointer
            // (4 bytes)
            ColumnType::VarChar(size) => {
                acc + 7 + std::cmp::min(*size as usize, MAX_INLINE_VAR_LEN_COL_SIZE)
            }
            ColumnType::Int => acc + 5,  // 4 bytes int + 1 for type flag
            ColumnType::Bool => acc + 2, // 1 bytes for bool val + 1 for type flag
            // size of type flag (1 byte) + len (2 bytes) + size of data itself + next page pointer
            // (4 bytes)
            ColumnType::LongBlob(size) => {
                acc + 7 + std::cmp::min(*size as usize, MAX_INLINE_VAR_LEN_COL_SIZE)
            }
        }) + 4
    }

    pub fn put_columns_in_order(
        &self,
        column_order: &[&str],
        columns_to_order: Vec<Vec<Column>>,
    ) -> Result<Vec<Vec<Column>>, Error> {
        if column_order.len() != self.columns.len() {
            return Err(Error::InvalidColumn(
                "must have same numer of columns as schema".to_string(),
            ));
        }

        let mut ordering = Vec::with_capacity(column_order.len());
        for co in column_order {
            let mut found = false;
            for (i, (col_name, _)) in self.columns.iter().enumerate() {
                if co == &**col_name {
                    ordering.push(i);
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(Error::InvalidColumn(format!(
                    "{co} is not a defined column"
                )));
            }
        }

        let ordered = columns_to_order
            .into_iter()
            .map(|cto| {
                if cto.len() < ordering.len() {
                    return Err(Error::InvalidColumn(
                        "values must have same number of columns as schema".to_string(),
                    ));
                }
                let mut to_order = vec![Column::Bool(false); cto.len()];

                for (col, next) in cto.into_iter().zip(ordering.iter()) {
                    to_order[*next] = col;
                }

                Ok(to_order)
            })
            .collect::<Result<Vec<Vec<Column>>, _>>()?;

        Ok(ordered)
    }

    // validate the select from columns and expand a *
    pub fn expand_select_columns(&self, columns: SelectColumns) -> Result<Vec<Rc<String>>, Error> {
        match columns {
            SelectColumns::Star => Ok(self.columns.iter().map(|(c, _)| c.clone()).collect()),
            SelectColumns::Explicit(cols) => {
                let mut result_columns = Vec::with_capacity(cols.len());
                for col in cols {
                    let mut found = false;
                    for (schema_col, _) in &self.columns {
                        if col == **schema_col {
                            result_columns.push(schema_col.clone());
                            found = true;
                            break;
                        }
                    }

                    if !found {
                        return Err(Error::InvalidColumn(col.to_string()));
                    }
                }
                Ok(result_columns)
            }
        }
    }

    pub fn columns(&self) -> &[(Rc<String>, ColumnType)] {
        &self.columns
    }

    pub fn primary_key_columns(&self) -> Option<&Vec<Rc<String>>> {
        self.primary_key.as_ref().map(|pk| &pk.column_names)
    }

    pub fn primary_key_indices(&self) -> Option<&Vec<usize>> {
        self.primary_key.as_ref().map(|pk| &pk.column_indices)
    }

    pub fn indices(&self) -> &[Index] {
        &self.indices
    }

    // Each non-primary index in the table schema is stored in a separate btree which each has its
    // own Schema which contains the index field columns plus an additional key column which
    // contains the actual primary row key into the primary btree. This method generates those
    // secondary btree schemas from the primary schema.
    pub fn index_schemas(&self) -> Vec<(String, Schema)> {
        let column_lookup: HashMap<&Rc<String>, &ColumnType> = self
            .columns
            .iter()
            .map(|(col_name, col_type)| (col_name, col_type))
            .collect();

        self.indices
            .iter()
            .map(|idx| {
                let primary_key = Some(PrimaryKey {
                    column_names: idx.columns.clone(),
                    column_indices: (0..idx.columns.len()).collect(),
                });

                let columns = idx
                    .columns
                    .iter()
                    .map(|col_name| {
                        // we've already validated that columns exist
                        (
                            col_name.clone(),
                            (*column_lookup.get(col_name).unwrap()).clone(),
                        )
                    })
                    .chain(
                        // add one more column which is the key into the primary btree for this row
                        vec![(
                            "key".to_string().into(),
                            ColumnType::LongBlob(MAX_KEY_VAR_CHAR_LEN as u32),
                        )]
                        .into_iter(),
                    )
                    .collect();

                (
                    idx.name.clone(),
                    Schema {
                        columns,
                        primary_key,
                        indices: vec![],
                    },
                )
            })
            .collect()
    }
}

// TODO: Rename this to Value as its independent of the column
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Column {
    // VarChar is variable length string with max length of 65,535.
    VarChar(String),
    // Int is a signed integer with max value of 2,147,483,647.
    Int(i32),
    // Bool is a boolean value.
    Bool(bool),
    // LongBlob is a variable length blob with max length of (2^32) - 1 bytes.
    LongBlob(Vec<u8>),
}

// A data type for a single column.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    // VarChar is variable length string with max length of 65,535.
    VarChar(u16),
    // Int is a signed integer with max value of 2,147,483,647.
    Int,
    // Bool is a boolean value.
    Bool,
    // LongBlob is a variable length blob with max length of (2^32) - 1 bytes.
    LongBlob(u32),
}

impl ColumnType {
    fn encoded_id(&self) -> u8 {
        match self {
            ColumnType::VarChar(_) => 0,
            ColumnType::Int => 1,
            ColumnType::Bool => 2,
            ColumnType::LongBlob(_) => 3,
        }
    }

    fn decode(id: u8) -> Result<Self, Error> {
        match id {
            0 => Ok(ColumnType::VarChar(0)),
            1 => Ok(ColumnType::Int),
            2 => Ok(ColumnType::Bool),
            3 => Ok(ColumnType::LongBlob(0)),
            _ => Err(Error::UnableToDecode(
                "unrecognized encoded column type".to_string(),
            )),
        }
    }

    pub fn is_congruent_literal(&self, lit: &Literal) -> bool {
        matches!(
            (self, lit),
            (ColumnType::VarChar(_), Literal::String(_))
                | (ColumnType::Int, Literal::Int(_))
                | (ColumnType::Bool, Literal::Bool(_))
                | (ColumnType::LongBlob(_), Literal::Bytes(_))
        )
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ColumnType::VarChar(size) => write!(f, "VARCHAR({size})"),
            ColumnType::Int => write!(f, "INT"),
            ColumnType::Bool => write!(f, "BOOL"),
            ColumnType::LongBlob(size) => write!(f, "LONGBLOB({size})"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_schema_non_unique_column() {
        let res = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("foo".into(), ColumnType::Bool),
            ],
            None,
            vec![],
        );
        assert_eq!(Err(Error::NonUniqueColumn("foo".into())), res);
    }

    #[test]
    fn test_literals_to_columns() {
        fn check(
            schema: &Schema,
            col_order: &[&str],
            literals: Vec<Vec<Literal>>,
            expected: Option<Vec<Vec<Column>>>,
        ) {
            let result = schema.literals_to_columns(col_order, literals);
            match (result, expected) {
                (Ok(cols), Some(exp)) => assert_eq!(exp, cols),
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        let schema = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(10)),
                ("baz".into(), ColumnType::LongBlob(10)),
            ],
            None,
            vec![],
        )
        .unwrap();

        check(&schema, &vec![], vec![], None);
        check(&schema, &vec!["foo"], vec![], None);
        check(&schema, &vec!["foo"], vec![vec![]], None);
        check(&schema, &vec!["foo"], vec![vec![Literal::Bool(true)]], None);
        check(
            &schema,
            &vec!["foo"],
            vec![vec![Literal::Int(7)]],
            Some(vec![vec![Column::Int(7)]]),
        );
        check(
            &schema,
            &vec!["foo"],
            vec![vec![Literal::Int(7), Literal::Bool(false)]],
            None,
        );
        check(
            &schema,
            &vec!["foo"],
            vec![vec![Literal::Int(7)], vec![Literal::Int(8)]],
            Some(vec![vec![Column::Int(7)], vec![Column::Int(8)]]),
        );
        check(
            &schema,
            &vec!["qux", "foo"],
            vec![vec![Literal::Int(7), Literal::String("bump".to_string())]],
            None,
        );
        check(
            &schema,
            &vec!["qux", "foo", "bar"],
            vec![vec![
                Literal::String("bump".to_string()),
                Literal::Int(7),
                Literal::Bool(false),
            ]],
            Some(vec![vec![
                Column::VarChar("bump".to_string()),
                Column::Int(7),
                Column::Bool(false),
            ]]),
        );
        check(
            &schema,
            &vec!["qux", "foo", "baz", "bar"],
            vec![vec![
                Literal::String("bump".to_string()),
                Literal::Int(7),
                Literal::Bytes(vec![42, 7, 100]),
                Literal::Bool(false),
            ]],
            Some(vec![vec![
                Column::VarChar("bump".to_string()),
                Column::Int(7),
                Column::LongBlob(vec![42, 7, 100]),
                Column::Bool(false),
            ]]),
        )
    }

    #[test]
    fn test_max_inline_row_size() {
        fn check(schema: &Schema, expected: usize) {
            assert_eq!(expected, schema.max_inline_row_size());
        }

        check(
            &Schema::new(
                vec![
                    ("foo".into(), ColumnType::Int),
                    ("bar".into(), ColumnType::Bool),
                    ("qux".into(), ColumnType::VarChar(10)),
                ],
                None,
                vec![],
            )
            .unwrap(),
            28,
        );
        check(
            &Schema::new(
                vec![
                    ("foo".into(), ColumnType::Int),
                    ("bar".into(), ColumnType::Bool),
                    ("qux".into(), ColumnType::VarChar(1024)),
                ],
                None,
                vec![],
            )
            .unwrap(),
            530,
        );
    }

    #[test]
    fn test_put_columns_in_order() {
        fn check(
            schema: &Schema,
            column_order: &[&str],
            cols: Vec<Vec<Column>>,
            expected: Option<Vec<Vec<Column>>>,
        ) {
            let result = schema.put_columns_in_order(column_order, cols);
            match (result, expected) {
                (Ok(cols), Some(exp)) => assert_eq!(exp, cols),
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        let schema = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(10)),
            ],
            None,
            vec![],
        )
        .unwrap();

        let cols = vec![vec![
            Column::Int(1),
            Column::Bool(true),
            Column::VarChar("hello".to_string()),
        ]];
        check(&schema, &["foo", "bar", "qux"], cols.clone(), Some(cols));

        let cols = vec![vec![
            Column::Int(1),
            Column::Bool(true),
            Column::VarChar("hello".to_string()),
        ]];
        check(&schema, &[], cols, None);

        let cols = vec![vec![Column::Int(1), Column::Bool(true)]];
        check(&schema, &["foo", "bar", "qux"], cols, None);

        let cols = vec![vec![
            Column::VarChar("hello".to_string()),
            Column::Bool(true),
            Column::Int(1),
        ]];
        let correct_cols = vec![vec![
            Column::Int(1),
            Column::Bool(true),
            Column::VarChar("hello".to_string()),
        ]];
        check(&schema, &["qux", "bar", "foo"], cols, Some(correct_cols));
    }

    #[test]
    fn test_expand_select_columns() {
        fn check(schema: &Schema, columns: SelectColumns, expected: Option<Vec<Rc<String>>>) {
            let result = schema.expand_select_columns(columns);
            match (result, expected) {
                (Ok(cols), Some(exp)) => assert_eq!(exp, cols),
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        let schema = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(10)),
            ],
            None,
            vec![],
        )
        .unwrap();

        check(
            &schema,
            SelectColumns::Star,
            Some(vec![
                Rc::new("foo".to_string()),
                Rc::new("bar".to_string()),
                Rc::new("qux".to_string()),
            ]),
        );
        check(
            &schema,
            SelectColumns::Explicit(vec!["foo"]),
            Some(vec![Rc::new("foo".to_string())]),
        );
        check(&schema, SelectColumns::Explicit(vec!["nope"]), None);
        check(
            &schema,
            SelectColumns::Explicit(vec!["bar", "foo"]),
            Some(vec![Rc::new("bar".to_string()), Rc::new("foo".to_string())]),
        );
    }

    #[test]
    fn test_index_schemas() {
        let schema = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(10)),
            ],
            None,
            vec![
                ("fb".into(), vec!["foo".into(), "bar".into()]),
                ("qf".into(), vec!["qux".into(), "foo".into()]),
                ("bq".into(), vec!["bar".into(), "qux".into()]),
            ],
        )
        .unwrap();

        assert_eq!(
            vec![
                (
                    "fb".into(),
                    Schema {
                        columns: vec![
                            ("foo".to_string().into(), ColumnType::Int),
                            ("bar".to_string().into(), ColumnType::Bool),
                            (
                                "key".to_string().into(),
                                ColumnType::LongBlob(MAX_KEY_VAR_CHAR_LEN as u32)
                            )
                        ],
                        primary_key: Some(PrimaryKey {
                            column_names: vec!["foo".to_string().into(), "bar".to_string().into()],
                            column_indices: vec![0, 1],
                        }),
                        indices: vec![],
                    }
                ),
                (
                    "qf".into(),
                    Schema {
                        columns: vec![
                            ("qux".to_string().into(), ColumnType::VarChar(10)),
                            ("foo".to_string().into(), ColumnType::Int),
                            (
                                "key".to_string().into(),
                                ColumnType::LongBlob(MAX_KEY_VAR_CHAR_LEN as u32)
                            )
                        ],
                        primary_key: Some(PrimaryKey {
                            column_names: vec!["qux".to_string().into(), "foo".to_string().into()],
                            column_indices: vec![0, 1],
                        }),
                        indices: vec![],
                    }
                ),
                (
                    "bq".into(),
                    Schema {
                        columns: vec![
                            ("bar".to_string().into(), ColumnType::Bool),
                            ("qux".to_string().into(), ColumnType::VarChar(10)),
                            (
                                "key".to_string().into(),
                                ColumnType::LongBlob(MAX_KEY_VAR_CHAR_LEN as u32)
                            )
                        ],
                        primary_key: Some(PrimaryKey {
                            column_names: vec!["bar".to_string().into(), "qux".to_string().into()],
                            column_indices: vec![0, 1],
                        }),
                        indices: vec![],
                    }
                )
            ],
            schema.index_schemas()
        );
    }
}
