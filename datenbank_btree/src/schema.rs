use std::collections::HashSet;

use crate::{Column, Error};

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

    fn validate_packed(&self, packed_row: &[u8]) -> bool {
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
        if i != packed_row.len() {
            false
        } else {
            true
        }
    }

    pub fn validate_columns(&self, cols: &[Column]) -> bool {
        if self.columns.len() != cols.len() {
            return false;
        }

        for ((_, schema_col), col) in self.columns.iter().zip(cols.into_iter()) {
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
}
