use std::io::Write;
use std::rc::Rc;

use nom::combinator::opt;
use nom::error::{make_error, ErrorKind};
use nom::multi::{length_count, length_value};
use nom::number::complete::{be_u16, be_u8};
use nom::{Err as NomErr, IResult};

use super::{ColumnType, Schema};
use crate::parser::identifier_bytes;

pub fn encode(schema: &Schema) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        2 + schema.columns().len() + schema.primary_key().map_or_else(|| 0, |k| 2 + k.len()),
    );

    bytes
        .write_all(&(schema.columns().len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");
    for (name, column) in &schema.columns {
        encode_column_type(name, column, &mut bytes);
    }

    if let Some(primary_key) = &schema.primary_key {
        bytes
            .write_all(&(primary_key.len() as u16).to_be_bytes())
            .expect("can't fail writing to vec");
        for name in primary_key {
            bytes
                .write_all(&(name.as_bytes().len() as u16).to_be_bytes())
                .expect("can't fail writing to vec");
            bytes
                .write_all(name.as_bytes())
                .expect("can't fail writing to vec");
        }
    }

    bytes
}

// See Schema::encode for description of the format
fn encode_column_type(name: &str, column: &ColumnType, bytes: &mut Vec<u8>) {
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

pub fn decode(input: &[u8]) -> IResult<&[u8], Schema> {
    let (input, columns) = length_count(be_u16, parse_column_type)(input)?;
    let (input, primary_key_bytes) = parse_primary_key(input)?;

    let columns: Vec<(Rc<String>, ColumnType)> =
        columns.into_iter().map(|(c, ct)| (c.into(), ct)).collect();
    let mut primary_key = None;

    if let Some(pk_bytes) = primary_key_bytes {
        let mut pk_fields: Vec<Rc<String>> = Vec::new();
        for field_bytes in pk_bytes {
            let mut found = false;
            for (col, _) in columns.iter() {
                if field_bytes == col.as_bytes() {
                    pk_fields.push(col.clone());
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(NomErr::Failure(make_error(input, ErrorKind::Verify)));
            }
        }

        primary_key = Some(pk_fields);
    }

    Ok((
        input,
        Schema {
            columns,
            primary_key,
        },
    ))
}

fn parse_column_type(input: &[u8]) -> IResult<&[u8], (String, ColumnType)> {
    let (rest, column_type) = be_u8(input)?;
    let column_type = match ColumnType::decode(column_type) {
        Ok(ct) => ct,
        Err(_) => return Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
    };

    let (rest, column_name) = length_value(be_u16, identifier_bytes)(rest)?;
    let column_name = match String::from_utf8(column_name.to_vec()) {
        Ok(column_name) => column_name,
        Err(_) => return Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
    };

    let (rest, column_type) = match column_type {
        ColumnType::VarChar(_) => {
            let (rest, size) = be_u16(rest)?;
            (rest, ColumnType::VarChar(size))
        }
        ct => (rest, ct),
    };

    Ok((rest, (column_name, column_type)))
}

fn parse_primary_key(input: &[u8]) -> IResult<&[u8], Option<Vec<&[u8]>>> {
    opt(length_count(be_u16, length_value(be_u16, identifier_bytes)))(input)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encode_no_primary_key() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
            ],
            None,
        )
        .unwrap();

        assert_eq!(
            vec![
                0, 3, 1, 0, 6, 119, 111, 110, 100, 101, 114, 2, 0, 5, 119, 104, 97, 116, 115, 0, 0,
                4, 110, 101, 120, 116, 0, 10
            ],
            schema.encode()
        );
    }

    #[test]
    fn test_encode_with_primary_key() {
        let schema = Schema::new(
            vec![
                ("wonder".into(), ColumnType::Int),
                ("whats".into(), ColumnType::Bool),
                ("next".into(), ColumnType::VarChar(10)),
            ],
            Some(vec!["wonder".into(), "whats".into()]),
        )
        .unwrap();

        assert_eq!(
            vec![
                0, 3, 1, 0, 6, 119, 111, 110, 100, 101, 114, 2, 0, 5, 119, 104, 97, 116, 115, 0, 0,
                4, 110, 101, 120, 116, 0, 10, 0, 2, 0, 6, 119, 111, 110, 100, 101, 114, 0, 5, 119,
                104, 97, 116, 115,
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
            ],
            None,
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
            ],
            Some(vec!["below".into(), "next".into(), "the".into()]),
        )
        .unwrap();

        let bytes = schema.encode();
        let (_, decoded) = decode(&bytes).unwrap();
        assert_eq!(schema, decoded);
    }
}
