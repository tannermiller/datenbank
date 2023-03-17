use std::io::Write;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::multi::length_count;
use nom::number::complete::be_u32;
use nom::sequence::pair;
use nom::IResult;

use super::{Row, RowCol, RowVarChar};

pub(crate) fn encode_row(row: &Row) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + 4 * row.body.len());

    // write the row count out so that we know how big this row is
    bytes
        .write_all(&(row.body.len() as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    for col in &row.body {
        match col {
            RowCol::VarChar(RowVarChar { inline, next_page }) => {
                bytes.push(0);

                // write len first
                bytes
                    .write_all(&(inline.len() as u16).to_be_bytes())
                    .expect("can't fail writing to vec");

                // and then write the string data
                bytes
                    .write_all(inline.as_bytes())
                    .expect("can't fail writing to vec");

                // finally write out the next page, or 0 if no next page
                let next_page = match next_page {
                    Some(np) => *np,
                    None => 0,
                };
                bytes
                    .write_all(&(next_page as u32).to_be_bytes())
                    .expect("can't fail writing to vec");
            }
            RowCol::Int(i) => {
                bytes.push(1);

                bytes
                    .write_all(&i.to_be_bytes())
                    .expect("can't fail writing to vec");
            }
            RowCol::Bool(b) => {
                bytes.push(2);

                let bb = if *b { 1 } else { 0 };
                bytes.write_all(&[bb]).expect("can't fail writing to vec");
            }
        }
    }

    bytes
}

pub(crate) fn decode_row(input: &[u8]) -> IResult<&[u8], Row> {
    let (input, row_cols) = length_count(
        be_u32,
        alt((
            pair(tag(&[0]), decode_varchar),
            pair(tag(&[1]), decode_int),
            pair(tag(&[2]), decode_bool),
        )),
    )(input)?;

    let body = row_cols.into_iter().map(|(_, rc)| rc).collect();

    if !schema.validate_columns(&body) {
        // TOOD: return error
        todo!()
    }

    Ok((
        input,
        Row {
            schema: schema.clone(),
            body,
        },
    ))
}

fn decode_varchar(input: &[u8]) -> IResult<&[u8], RowCol> {
    todo!()
}

fn decode_int(input: &[u8]) -> IResult<&[u8], RowCol> {
    todo!()
}

fn decode_bool(input: &[u8]) -> IResult<&[u8], RowCol> {
    todo!()
}
