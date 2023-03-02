use std::collections::HashMap;
use std::io::Write;

use nom::error::{make_error, ErrorKind};
use nom::multi::length_value;
use nom::number::complete::{be_u16, be_u8};
use nom::sequence::tuple;
use nom::{Err as NomErr, IResult};

use super::btree::cache::DataCache;
use super::btree::{decode as decode_btree, BTree};
use super::Error;
use crate::pagestore::TablePageStore;
use crate::parser::identifier_bytes;
use crate::schema::{decode as decode_schema, Schema};

// The table header is stored in page 0 of the table page store. It has the following format,
// starting at byte offset 0:
//   - 1 byte which stores the length of the table name
//   - the table name bytes
//   - 2 bytes to store the length of the encoded schema
//   - the schema as encoded bytes, the schema is self-decoding
//   - 2 bytes to store the length of the encoded btree root information
//   - the encoded btree information, it is most likely just the order and root, but more may
//     be added
//   - TBD, probably index information
pub fn encode<S: TablePageStore>(name: &str, schema: &Schema, tree: &BTree<S>) -> Vec<u8> {
    // guess at the capacity
    let mut header_bytes = Vec::with_capacity(13 + name.as_bytes().len() + 4 * schema.len());

    header_bytes
        .write_all(&(name.len() as u8).to_be_bytes())
        .expect("can't fail writing to vec");
    header_bytes
        .write_all(name.as_bytes())
        .expect("can't fail writing to vec");

    let schema_bytes = schema.encode();
    header_bytes
        .write_all(&(schema_bytes.len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");
    header_bytes
        .write_all(&schema_bytes)
        .expect("can't fail writing to vec");

    let btree_bytes = tree.encode();
    header_bytes
        .write_all(&(btree_bytes.len() as u16).to_be_bytes())
        .expect("can't fail writing to vec");
    header_bytes
        .write_all(&btree_bytes)
        .expect("can't fail writing to vec");

    header_bytes
}

pub fn decode<S: TablePageStore>(
    header_bytes: &[u8],
    store: S,
) -> Result<(String, Schema, BTree<S>), Error> {
    let (_, (name, schema, order, root)) =
        decode_parse(header_bytes).map_err(|e| Error::DecodingError(e.to_string()))?;
    let tree = BTree {
        name: name.clone(),
        order,
        schema: schema.clone(),
        root,
        node_cache: HashMap::new(),
        data_cache: DataCache::new(store.clone()),
        store,
    };
    Ok((name, schema, tree))
}

fn decode_parse(input: &[u8]) -> IResult<&[u8], (String, Schema, usize, Option<usize>)> {
    let (rest, (table_name, schema, (order, root))) = tuple((
        length_value(be_u8, parse_table_name),
        length_value(be_u16, decode_schema),
        length_value(be_u16, decode_btree),
    ))(input)?;
    Ok((rest, (table_name, schema, order, root)))
}

fn parse_table_name(input: &[u8]) -> IResult<&[u8], String> {
    let (rest, table_name_bytes) = identifier_bytes(input)?;
    match String::from_utf8(table_name_bytes.to_vec()) {
        Ok(table_name) => Ok((rest, table_name)),
        Err(_) => Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
    }
}
