use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use nom::error::{make_error, ErrorKind};
use nom::multi::{length_count, length_value};
use nom::number::complete::{be_u16, be_u32, be_u8};
use nom::sequence::{pair, tuple};
use nom::{Err as NomErr, IResult};

use super::{ColumnType, Index, PrimaryKey, Schema};
use crate::encode::{write_len, write_len_and_bytes};
use crate::parser::identifier_bytes;

pub fn encode(schema: &Schema) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        2 + schema.columns().len()
            + 2
            + schema.primary_key_columns().map_or_else(|| 0, |k| k.len())
            + 2
            + schema.indices().len() * 34,
    );

    write_len(&mut bytes, schema.columns().len());
    for (name, column) in &schema.columns {
        encode_column_type(name, column, &mut bytes);
    }

    write_len(
        &mut bytes,
        schema.primary_key_columns().map_or_else(|| 0, |k| k.len()),
    );
    if let Some(primary_key) = schema.primary_key_columns() {
        for name in primary_key {
            write_len_and_bytes(&mut bytes, name.as_bytes());
        }
    }

    write_len(&mut bytes, schema.indices.len());
    for idx in &schema.indices {
        write_len_and_bytes(&mut bytes, idx.name.as_bytes());
        write_len(&mut bytes, idx.columns.len());
        for col in &idx.columns {
            write_len_and_bytes(&mut bytes, col.as_bytes());
        }
    }

    bytes
}

// See Schema::encode for description of the format
fn encode_column_type(name: &str, column: &ColumnType, bytes: &mut Vec<u8>) {
    bytes
        .write_all(&column.encoded_id().to_be_bytes())
        .expect("can't fail writing to vec");
    write_len_and_bytes(bytes, name.as_bytes());

    match column {
        ColumnType::VarChar(max_size) => {
            write_len(bytes, (*max_size) as usize);
        }
        ColumnType::LongBlob(max_size) => {
            bytes
                .write_all(&max_size.to_be_bytes())
                .expect("can't fail writing to vec");
        }
        _ => {}
    }
}

pub fn decode(input: &[u8]) -> IResult<&[u8], Schema> {
    let (input, (columns_bytes, primary_key_bytes, indices_bytes)) = tuple((
        length_count(be_u16, parse_column_type),
        parse_primary_key,
        parse_indices,
    ))(input)?;

    let mut column_lookup = HashMap::new();
    let mut columns: Vec<(Rc<String>, ColumnType)> = Vec::with_capacity(columns_bytes.len());
    for (col_name, col_type) in columns_bytes {
        let name = match String::from_utf8(col_name.to_vec()) {
            Ok(name) => Rc::new(name),
            Err(_) => return Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
        };
        column_lookup.insert(col_name, name.clone());
        columns.push((name, col_type));
    }

    let mut primary_key = None;

    if !primary_key_bytes.is_empty() {
        let mut pk_fields: Vec<Rc<String>> = Vec::new();
        for field_bytes in primary_key_bytes {
            match column_lookup.get(field_bytes) {
                Some(col_name) => pk_fields.push(col_name.clone()),
                None => return Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
            }
        }

        primary_key = Some(PrimaryKey::new(&columns, pk_fields));
    }

    let mut indices = Vec::with_capacity(indices_bytes.len());
    for (idx_name, idx_cols) in indices_bytes {
        let name = match String::from_utf8(idx_name.to_vec()) {
            Ok(name) => name.into(),
            Err(_) => return Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
        };

        let mut columns = Vec::with_capacity(idx_cols.len());
        for idx_col in idx_cols {
            match column_lookup.get(idx_col) {
                Some(col_name) => columns.push(col_name.clone()),
                None => return Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
            }
        }

        indices.push(Index { name, columns })
    }

    Ok((
        input,
        Schema {
            columns,
            primary_key,
            indices,
        },
    ))
}

fn parse_column_type(input: &[u8]) -> IResult<&[u8], (&[u8], ColumnType)> {
    let (rest, column_type) = be_u8(input)?;
    let column_type = match ColumnType::decode(column_type) {
        Ok(ct) => ct,
        Err(_) => return Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
    };

    let (rest, column_name) = length_value(be_u16, identifier_bytes)(rest)?;

    let (rest, column_type) = match column_type {
        ColumnType::VarChar(_) => {
            let (rest, size) = be_u16(rest)?;
            (rest, ColumnType::VarChar(size))
        }
        ColumnType::LongBlob(_) => {
            let (rest, size) = be_u32(rest)?;
            (rest, ColumnType::LongBlob(size))
        }
        ct => (rest, ct),
    };

    Ok((rest, (column_name, column_type)))
}

fn parse_primary_key(input: &[u8]) -> IResult<&[u8], Vec<&[u8]>> {
    length_count(be_u16, length_value(be_u16, identifier_bytes))(input)
}

fn parse_indices(input: &[u8]) -> IResult<&[u8], Vec<(&[u8], Vec<&[u8]>)>> {
    length_count(
        be_u16,
        pair(
            length_value(be_u16, identifier_bytes),
            length_count(be_u16, length_value(be_u16, identifier_bytes)),
        ),
    )(input)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encode_no_primary_key_no_indices() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
                ("nothing".into(), ColumnType::LongBlob(32)),
            ],
            None,
            vec![],
        )
        .unwrap();

        assert_eq!(
            vec![
                0, 4, 1, 0, 6, 119, 111, 110, 100, 101, 114, 2, 0, 5, 119, 104, 97, 116, 115, 0, 0,
                4, 110, 101, 120, 116, 0, 10, 3, 0, 7, 110, 111, 116, 104, 105, 110, 103, 0, 0, 0,
                32, 0, 0, 0, 0
            ],
            schema.encode()
        );
    }

    #[test]
    fn test_encode_with_primary_key_no_indices() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
                ("nothing".into(), ColumnType::LongBlob(32)),
            ],
            Some(vec!["wonder".into(), "whats".into()]),
            vec![],
        )
        .unwrap();

        assert_eq!(
            vec![
                0, 4, 1, 0, 6, 119, 111, 110, 100, 101, 114, 2, 0, 5, 119, 104, 97, 116, 115, 0, 0,
                4, 110, 101, 120, 116, 0, 10, 3, 0, 7, 110, 111, 116, 104, 105, 110, 103, 0, 0, 0,
                32, 0, 2, 0, 6, 119, 111, 110, 100, 101, 114, 0, 5, 119, 104, 97, 116, 115, 0, 0
            ],
            schema.encode()
        );
    }

    #[test]
    fn test_encode_no_primary_key_with_indices() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
            ],
            None,
            vec![
                ("inw".into(), vec!["next".into(), "wonder".into()]),
                ("iww".into(), vec!["wonder".into(), "whats".into()]),
            ],
        )
        .unwrap();

        assert_eq!(
            vec![
                0, 3, 1, 0, 6, 119, 111, 110, 100, 101, 114, 2, 0, 5, 119, 104, 97, 116, 115, 0, 0,
                4, 110, 101, 120, 116, 0, 10, 0, 0, 0, 2, 0, 3, b'i', b'n', b'w', 0, 2, 0, 4, b'n',
                b'e', b'x', b't', 0, 6, b'w', b'o', b'n', b'd', b'e', b'r', 0, 3, b'i', b'w', b'w',
                0, 2, 0, 6, b'w', b'o', b'n', b'd', b'e', b'r', 0, 5, b'w', b'h', b'a', b't', b's',
            ],
            schema.encode()
        );
    }

    #[test]
    fn test_encode_with_primary_key_with_indices() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
            ],
            Some(vec!["wonder".into(), "whats".into()]),
            vec![
                ("inw".into(), vec!["next".into(), "wonder".into()]),
                ("iww".into(), vec!["wonder".into(), "whats".into()]),
            ],
        )
        .unwrap();

        assert_eq!(
            vec![
                0, 3, 1, 0, 6, 119, 111, 110, 100, 101, 114, 2, 0, 5, 119, 104, 97, 116, 115, 0, 0,
                4, 110, 101, 120, 116, 0, 10, 0, 2, 0, 6, 119, 111, 110, 100, 101, 114, 0, 5, 119,
                104, 97, 116, 115, 0, 2, 0, 3, b'i', b'n', b'w', 0, 2, 0, 4, b'n', b'e', b'x',
                b't', 0, 6, b'w', b'o', b'n', b'd', b'e', b'r', 0, 3, b'i', b'w', b'w', 0, 2, 0, 6,
                b'w', b'o', b'n', b'd', b'e', b'r', 0, 5, b'w', b'h', b'a', b't', b's',
            ],
            schema.encode()
        );
    }

    #[test]
    fn test_reflective_no_primary_key() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
                ("send".into(), ColumnType::Bool),
                ("the".into(), ColumnType::VarChar(9999)),
                ("pain".into(), ColumnType::Int),
                ("below".into(), ColumnType::Int),
                ("vitamin_r".into(), ColumnType::LongBlob(42)),
            ],
            None,
            vec![],
        )
        .unwrap();

        let bytes = schema.encode();
        let (_, decoded) = decode(&bytes).unwrap();
        assert_eq!(schema, decoded);
    }

    #[test]
    fn test_reflective_with_primary_key() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
                ("send".into(), ColumnType::Bool),
                ("the".into(), ColumnType::VarChar(9999)),
                ("pain".into(), ColumnType::Int),
                ("below".into(), ColumnType::Int),
                ("vitamin_r".into(), ColumnType::LongBlob(42)),
            ],
            Some(vec!["below".into(), "next".into(), "the".into()]),
            vec![],
        )
        .unwrap();

        let bytes = schema.encode();
        let (_, decoded) = decode(&bytes).unwrap();
        assert_eq!(schema, decoded);
    }
}
