use std::io::Write;

use nom::error::{make_error, ErrorKind};
use nom::multi::{length_count, length_value};
use nom::number::complete::{be_u16, be_u8};
use nom::sequence::tuple;
use nom::{Err as NomErr, IResult};

use super::btree::{decode as decode_btree, BTree};
use super::Error;
use crate::cache::Cache;
use crate::encode::{write_len, write_len_and_bytes};
use crate::pagestore::{TablePageStore, TablePageStoreBuilder};
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
//   - 2 bytes to store the number of secondary indices
//   - for each secondary index:
//     - 2 bytes to store the length of the encoded index btree root information
//     - the encoded btree information
pub fn encode<S: TablePageStore>(
    name: &str,
    schema: &Schema,
    primary: &BTree<S>,
    secondaries: &[BTree<S>],
) -> Vec<u8> {
    // guess at the capacity
    let mut header_bytes = Vec::with_capacity(
        13 + name.as_bytes().len()
            + 4 * schema.columns().len()
            + 4 * schema.primary_key_columns().map_or_else(|| 0, |k| k.len())
            + 4 * schema.indices().len(),
    );

    header_bytes
        .write_all(&(name.len() as u8).to_be_bytes())
        .expect("can't fail writing to vec");
    header_bytes
        .write_all(name.as_bytes())
        .expect("can't fail writing to vec");

    let schema_bytes = schema.encode();
    write_len_and_bytes(&mut header_bytes, &schema_bytes);

    let btree_bytes = primary.encode();
    write_len_and_bytes(&mut header_bytes, &btree_bytes);

    write_len(&mut header_bytes, secondaries.len());
    for sec_tree in secondaries {
        let sec_tree_bytes = sec_tree.encode();
        write_len_and_bytes(&mut header_bytes, &sec_tree_bytes);
    }

    header_bytes
}

pub fn decode<S: TablePageStore, SB: TablePageStoreBuilder<PageStore = S>>(
    header_bytes: &[u8],
    store_builder: &mut SB,
) -> Result<(String, Schema, BTree<S>, Vec<BTree<S>>), Error> {
    let (_, (name, schema, order, root, secondaries)) =
        decode_parse(header_bytes).map_err(|e| Error::DecodingError(e.to_string()))?;

    let tree = BTree {
        name: name.clone(),
        order,
        schema: schema.clone(),
        root,
        node_cache: Cache::new(store_builder.build()?),
        data_cache: Cache::new(store_builder.build()?),
        store: store_builder.build()?,
    };

    let secondaries = schema
        .index_schemas()
        .into_iter()
        .zip(secondaries.into_iter())
        .map(|((idx_name, schema), (order, root))| {
            Ok(BTree {
                name: idx_name,
                order,
                schema,
                root,
                node_cache: Cache::new(store_builder.build(&name)?),
                data_cache: Cache::new(store_builder.build(&name)?),
                store: store_builder.build(&name)?,
            })
        })
        .collect::<Result<Vec<BTree<S>>, Error>>()?;

    Ok((name, schema, tree, secondaries))
}

fn decode_parse(
    input: &[u8],
) -> IResult<
    &[u8],
    (
        String,
        Schema,
        usize,
        Option<usize>,
        Vec<(usize, Option<usize>)>,
    ),
> {
    let (rest, (table_name, schema, (order, root), secondaries)) = tuple((
        length_value(be_u8, parse_table_name),
        length_value(be_u16, decode_schema),
        length_value(be_u16, decode_btree),
        length_count(be_u16, length_value(be_u16, decode_btree)),
    ))(input)?;
    Ok((rest, (table_name, schema, order, root, secondaries)))
}

fn parse_table_name(input: &[u8]) -> IResult<&[u8], String> {
    let (rest, table_name_bytes) = identifier_bytes(input)?;
    match String::from_utf8(table_name_bytes.to_vec()) {
        Ok(table_name) => Ok((rest, table_name)),
        Err(_) => Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
    }
}
