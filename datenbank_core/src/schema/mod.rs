use std::collections::HashSet;

use crate::parser::Literal;
use crate::table::btree::row::{RowCol, RowVarChar};

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
}

// The schema describes the columns that make each row in the table.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    // each column has a name a data type
    columns: Vec<(String, ColumnType)>,
}

impl Schema {
    // Create a new Schema. Validate that the schema is valid.
    pub fn new(columns: Vec<(String, ColumnType)>) -> Result<Self, Error> {
        let mut col_names = HashSet::new();
        for (name, _) in columns.iter() {
            if !col_names.insert(name) {
                return Err(Error::NonUniqueColumn(name.to_string()));
            }
        }
        Ok(Schema { columns })
    }

    pub fn len(&self) -> usize {
        self.columns.len()
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
                if col_name == column {
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
        self.columns.iter().fold(0, |acc, (_, col)| match col {
            // size of len (2 bytes) + size of data itself
            ColumnType::VarChar(size) => {
                acc + 2 + std::cmp::min(*size as usize, MAX_INLINE_VAR_LEN_COL_SIZE)
            }
            ColumnType::Int => acc + 4,
            ColumnType::Bool => acc + 1,
        })
    }

    pub fn columns(&self) -> &[(String, ColumnType)] {
        &self.columns
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
                if co == col_name {
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
}

#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    // VarChar is variable length string with max length of 65,535.
    VarChar(String),
    // Int is a signed integer with max value of 2,147,483,647.
    Int(i32),
    // Bool is a boolean value.
    Bool(bool),
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
}

impl ColumnType {
    fn encoded_id(&self) -> u8 {
        match self {
            ColumnType::VarChar(_) => 0,
            ColumnType::Int => 1,
            ColumnType::Bool => 2,
        }
    }

    fn decode(id: u8) -> Result<Self, Error> {
        match id {
            0 => Ok(ColumnType::VarChar(0)),
            1 => Ok(ColumnType::Int),
            2 => Ok(ColumnType::Bool),
            _ => Err(Error::UnableToDecode(
                "unrecognized encoded column type".to_string(),
            )),
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ColumnType::VarChar(size) => write!(f, "VARCHAR({size})"),
            ColumnType::Int => write!(f, "INT"),
            ColumnType::Bool => write!(f, "BOOL"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_schema_non_unique_column() {
        let res = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("foo".into(), ColumnType::Bool),
        ]);
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

        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
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
    }

    #[test]
    fn test_max_inline_row_size() {
        fn check(schema: &Schema, expected: usize) {
            assert_eq!(expected, schema.max_inline_row_size());
        }

        check(
            &Schema::new(vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(10)),
            ])
            .unwrap(),
            17,
        );
        check(
            &Schema::new(vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(1024)),
            ])
            .unwrap(),
            519,
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

        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
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
}
