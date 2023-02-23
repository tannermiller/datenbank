use std::io::Write;

use nom::error::{make_error, ErrorKind};
use nom::multi::{length_count, length_value};
use nom::number::complete::{be_u16, be_u8};
use nom::{Err as NomErr, IResult};

use super::{ColumnType, Error, Schema};
use crate::parser::identifier_bytes;

pub fn encode(schema: &Schema) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2 + schema.len());
    bytes
        .write_all(&(schema.len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");

    for (name, column) in &schema.columns {
        encode_column_type(name, column, &mut bytes);
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

pub fn decode(input: &[u8]) -> Result<Schema, Error> {
    match length_count(be_u16, parse_column_type)(input) {
        // TODO: What do I do with the rest of this?
        Ok((_rest, columns)) => Ok(Schema { columns }),
        Err(e) => Err(Error::UnableToDecode(e.to_string())),
    }
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
