use std::io::Write;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{rest, value};
use nom::multi::{length_count, length_value};
use nom::number::complete::{be_i32, be_u16, be_u32};
use nom::sequence::pair;
use nom::IResult;

use super::{Row, RowBytes, RowCol};

pub(crate) fn encode_row(row: &Row) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + 4 * row.body.len());

    // write the row count out so that we know how big this row is
    bytes
        .write_all(&(row.body.len() as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    for col in &row.body {
        match col {
            RowCol::VarChar(rb) => {
                bytes.push(0);
                encode_row_bytes(&mut bytes, rb)
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
            RowCol::LongBlob(rb) => {
                bytes.push(3);
                encode_row_bytes(&mut bytes, rb)
            }
        }
    }

    bytes
}

fn encode_row_bytes(bytes: &mut Vec<u8>, RowBytes { inline, next_page }: &RowBytes) {
    // write len first
    bytes
        .write_all(&(inline.len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");

    // and then write the string data
    bytes.write_all(inline).expect("can't fail writing to vec");

    // finally write out the next page, or 0 if no next page
    let next_page = match next_page {
        Some(np) => *np,
        None => 0,
    };
    bytes
        .write_all(&(next_page as u32).to_be_bytes())
        .expect("can't fail writing to vec");
}

pub(crate) fn decode_row(input: &[u8]) -> IResult<&[u8], Row> {
    let (input, row_cols) = length_count(
        be_u32,
        alt((
            pair(tag(&[0]), decode_varchar),
            pair(tag(&[1]), decode_int),
            pair(tag(&[2]), decode_bool),
            pair(tag(&[3]), decode_longblob),
        )),
    )(input)?;

    let body = row_cols.into_iter().map(|(_, rc)| rc).collect();

    Ok((input, Row { body }))
}

fn decode_varchar(input: &[u8]) -> IResult<&[u8], RowCol> {
    let (input, rb) = decode_row_bytes(input)?;

    Ok((input, RowCol::VarChar(rb)))
}

fn decode_longblob(input: &[u8]) -> IResult<&[u8], RowCol> {
    let (input, rb) = decode_row_bytes(input)?;

    Ok((input, RowCol::LongBlob(rb)))
}

fn decode_row_bytes(input: &[u8]) -> IResult<&[u8], RowBytes> {
    let (input, inline) = length_value(be_u16, rest)(input)?;

    let (input, next_page) = be_u32(input)?;
    let next_page = if next_page == 0 {
        None
    } else {
        Some(next_page as usize)
    };

    Ok((
        input,
        RowBytes {
            inline: inline.to_vec(),
            next_page,
        },
    ))
}

fn decode_int(input: &[u8]) -> IResult<&[u8], RowCol> {
    be_i32(input).map(|(input, val)| (input, RowCol::Int(val)))
}

fn decode_bool(input: &[u8]) -> IResult<&[u8], RowCol> {
    alt((
        value(RowCol::Bool(false), tag(&[0])),
        value(RowCol::Bool(true), tag(&[1])),
    ))(input)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let row = Row {
            body: vec![
                RowCol::Int(7),
                RowCol::Bool(true),
                RowCol::VarChar(RowBytes {
                    inline: b"Hello, World!".to_vec(),
                    next_page: Some(11),
                }),
                RowCol::VarChar(RowBytes {
                    inline: b"I'm different".to_vec(),
                    next_page: None,
                }),
                RowCol::Bool(false),
                RowCol::Int(8),
                RowCol::LongBlob(RowBytes {
                    inline: b"Hello, Blob!".to_vec(),
                    next_page: Some(11),
                }),
                RowCol::LongBlob(RowBytes {
                    inline: b"I'm blobin' different".to_vec(),
                    next_page: None,
                }),
            ],
        };

        let encoded = encode_row(&row);
        let (rest, decoded) = decode_row(&encoded).unwrap();

        assert!(rest.is_empty());
        assert_eq!(row, decoded);
    }
}
