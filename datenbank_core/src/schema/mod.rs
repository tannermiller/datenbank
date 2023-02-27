use std::collections::HashSet;

use crate::parser::Literal;

mod encode;

pub use encode::decode;

// The maximum length that any variable length field will store inline in a row. Any amount over
// this amount will be stored external to the leaf page.
const MAX_INLINE_VAR_LEN_COL_SIZE: usize = 512;

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

    // TODO: replace this with the actual parsing logic, that is enough validation, probably
    pub fn validate_packed(&self, packed_row: &[u8]) -> bool {
        let mut i = 0;
        for (_, col) in self.columns.iter() {
            let consumed_len = match col {
                ColumnType::VarChar(schema_len) => {
                    if packed_row.len() < i + 2 {
                        return false;
                    }

                    // try to clone and convert to u16
                    if let Ok(len_bytes) = packed_row[i..i + 2].try_into() {
                        // get the expected len
                        let vc_len = u16::from_be_bytes(len_bytes);
                        if vc_len > *schema_len {
                            return false;
                        }

                        let total_column_len = 2 + vc_len as usize;

                        // ensure the packed_row is at least that long now
                        if packed_row.len() < i + total_column_len {
                            return false;
                        }

                        total_column_len
                    } else {
                        return false;
                    }
                }
                ColumnType::Int => {
                    if packed_row.len() < i + 4 {
                        return false;
                    }

                    4
                }
                ColumnType::Bool => {
                    if packed_row.len() < i + 1 {
                        return false;
                    }

                    1
                }
            };

            i += consumed_len;
        }

        // We've reached the end of our expected columns but if there is more in the packed row we
        // haven't consumed yet, then that's an issue.
        i == packed_row.len()
    }

    pub fn validate_columns(&self, cols: &[Column]) -> bool {
        if self.columns.len() != cols.len() {
            return false;
        }

        for ((_, schema_col), col) in self.columns.iter().zip(cols.iter()) {
            let is_valid = match (schema_col, col) {
                (ColumnType::VarChar(schema_len), Column::VarChar(col_value)) => {
                    col_value.len() <= *schema_len as usize
                }
                (ColumnType::Int, Column::Int(_)) => true,
                (ColumnType::Bool, Column::Bool(_)) => true,
                _ => false,
            };

            if !is_valid {
                return false;
            }
        }

        true
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

// Find the total allocated size of bytes we need to fit these columns in a node.
pub fn size_of_packed_cols(cols: &[Column]) -> usize {
    cols.iter().fold(0, |acc, col| match col {
        // size of len (2 bytes) + size of data itself
        Column::VarChar(s) => {
            let size = s.as_bytes().len();
            acc + 2 + std::cmp::min(size, MAX_INLINE_VAR_LEN_COL_SIZE)
        }
        Column::Int(_) => acc + 4,
        Column::Bool(_) => acc + 1,
    })
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
    fn test_schema_validate_columns() {
        fn check(schema: &Schema, cols: &[Column], expected_result: bool) {
            assert_eq!(expected_result, schema.validate_columns(cols));
        }

        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("baz".into(), ColumnType::VarChar(3000)),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();

        // valid
        check(
            &schema,
            &vec![
                Column::Int(7),
                Column::Bool(true),
                Column::VarChar("howdy".to_string()),
                Column::VarChar("partner".to_string()),
            ],
            true,
        );

        // not enough columns
        check(&schema, &vec![Column::Int(7)], false);

        // out of order
        check(
            &schema,
            &vec![
                Column::Bool(true),
                Column::Int(7),
                Column::VarChar("howdy".to_string()),
                Column::VarChar("partner".to_string()),
            ],
            false,
        );

        // too many columns
        check(
            &schema,
            &vec![
                Column::Bool(true),
                Column::Int(7),
                Column::VarChar("howdy".to_string()),
                Column::VarChar("partner".to_string()),
                Column::Bool(false),
            ],
            false,
        );

        // varchar too long
        check(
            &schema,
            &vec![
                Column::Int(7),
                Column::Bool(true),
                Column::VarChar("howdy".to_string()),
                Column::VarChar("01234567890123456789".to_string()),
            ],
            false,
        );
    }

    #[test]
    fn test_schema_validate_packed() {
        fn check(schema: &Schema, cols: &[u8], expected_result: bool) {
            assert_eq!(expected_result, schema.validate_packed(cols));
        }

        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();

        // valid
        check(
            &schema,
            &vec![0, 0, 0, 1, 1, 0, 5, 104, 111, 119, 100, 121],
            true,
        );

        // too short
        check(&schema, &vec![0], false);

        // too long varchar is too short
        check(
            &schema,
            &vec![0, 0, 0, 1, 1, 0, 5, 104, 111, 119, 100],
            false,
        );
    }

    #[test]
    fn test_size_of_packed_cols() {
        let cols = vec![
            Column::VarChar("012345678901234567890123456789".to_string()),
            Column::Int(7),
            Column::Bool(true),
        ];
        assert_eq!(37, size_of_packed_cols(&cols));
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
}
