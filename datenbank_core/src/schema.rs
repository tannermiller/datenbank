use std::collections::HashSet;
use std::io::Write;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("column names must be unique, found duplicate {0}")]
    NonUniqueColumn(String),
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
        let mut bytes = Vec::with_capacity(2 + self.len());
        bytes
            .write_all(&(self.len() as u16).to_be_bytes())
            .expect("can't fail writing to vec");

        for column in &self.columns {
            encode_column_type(column, &mut bytes);
        }

        bytes
    }
}

// See Schema::encode for description of the format
fn encode_column_type((name, column): &(String, ColumnType), bytes: &mut Vec<u8>) {
    bytes
        .write_all(&column.encoded_id().to_be_bytes())
        .expect("can't fail writing to vec");
    bytes
        .write_all(&(name.as_bytes().len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");
    bytes
        .write_all(name.as_bytes())
        .expect("can't fail writing to vec");

    if let ColumnType::VarChar(max_size) = column {
        bytes
            .write_all(&max_size.to_be_bytes())
            .expect("can't fail writing to vec");
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

// TODO: This doesn't take off-page storage into account
// Find the total allocated size of bytes we need to fit these columns.
pub fn size_of_packed_cols(cols: &[Column]) -> usize {
    cols.iter().fold(0, |acc, col| match col {
        // size of len (2 bytes) + size of data itself
        Column::VarChar(s) => acc + 2 + s.as_bytes().len(),
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
    fn test_encode() {
        let schema = Schema::new(vec![
            ("wonder".into(), ColumnType::Int),
            ("whats".into(), ColumnType::Bool),
            ("next".into(), ColumnType::VarChar(10)),
        ])
        .unwrap();

        assert_eq!(
            vec![
                0, 3, 1, 0, 6, 119, 111, 110, 100, 101, 114, 2, 0, 5, 119, 104, 97, 116, 115, 0, 0,
                4, 110, 101, 120, 116, 0, 10
            ],
            schema.encode()
        );
    }
}