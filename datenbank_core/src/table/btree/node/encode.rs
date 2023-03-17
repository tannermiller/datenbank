use std::io::Write;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::rest;
use nom::error::{make_error, ErrorKind};
use nom::multi::{length_count, length_value};
use nom::number::complete::be_u32;
use nom::sequence::pair;
use nom::{Err as NomErr, IResult};

use super::super::row::encode::{decode_row, encode_row};
use super::super::Error;
use super::{Internal, Leaf, Node, NodeBody};

pub(crate) fn encode_node(node: &Node) -> Vec<u8> {
    // we encode the body first even though it goes at the end of the byte vector so that we
    // can correctly allocate the returned vector.
    let body_bytes = encode_body(&node.body);

    // body + id + order + body type
    let mut bytes = Vec::with_capacity(body_bytes.len() + 4 + 4 + 1);

    bytes
        .write_all(&(node.id as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    bytes
        .write_all(&(node.order as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    let body_type = match node.body {
        NodeBody::Internal { .. } => 0u8,
        NodeBody::Leaf { .. } => 1,
    };
    bytes
        .write_all(&body_type.to_be_bytes())
        .expect("can't fail writing to vec");

    // finally write the body bytes
    bytes
        .write_all(&body_bytes)
        .expect("can't fail writing to vec");

    bytes
}

pub(crate) fn decode_node(input: &[u8]) -> Result<Node, Error> {
    match inner_decode_node(input) {
        Err(err) => Err(Error::UnrecoverableError(err.to_string())),
        // in this case, if there'e extra input but we parsed a whole node out then we are probably
        // fine as next time we edit this page we'll probably just overwrite the extra bytes
        Ok((_, node)) => Ok(node),
    }
}

fn inner_decode_node(input: &[u8]) -> IResult<&[u8], Node> {
    let (input, id) = be_u32(input)?;
    let (input, order) = be_u32(input)?;
    let (input, (_, body)) = alt((
        pair(tag(&[0u8]), decode_internal),
        pair(tag(&[1u8]), decode_leaf),
    ))(input)?;

    Ok((
        input,
        Node {
            id: id as usize,
            order: order as usize,
            body,
        },
    ))
}

fn decode_leaf(input: &[u8]) -> IResult<&[u8], NodeBody> {
    let (input, rows) = length_count(be_u32, length_value(be_u32, decode_row))(input)?;

    let (input, right_sibling) = be_u32(input)?;
    let right_sibling = if right_sibling == 0 {
        None
    } else {
        Some(right_sibling as usize)
    };

    Ok((
        input,
        NodeBody::Leaf(Leaf {
            rows,
            right_sibling,
        }),
    ))
}

fn decode_internal(input: &[u8]) -> IResult<&[u8], NodeBody> {
    let (input, bks) = length_count(be_u32, length_value(be_u32, rest))(input)?;
    let boundary_keys = bks
        .into_iter()
        .map(|v| {
            String::from_utf8(v.to_vec())
                .map_err(|_| NomErr::Failure(make_error(input, ErrorKind::Verify)))
        })
        .collect::<Result<Vec<String>, _>>()?;

    let (input, children) = length_count(be_u32, be_u32)(input)?;
    let children = children.into_iter().map(|c| c as usize).collect();

    Ok((
        input,
        NodeBody::Internal(Internal {
            boundary_keys,
            children,
        }),
    ))
}

fn encode_leaf(
    Leaf {
        rows,
        right_sibling,
    }: &Leaf,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(rows.len() * 100 + 4);

    bytes
        .write_all(&(rows.len() as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    for row in rows {
        let rb = encode_row(row);
        bytes
            .write_all(&(rb.len() as u32).to_be_bytes())
            .expect("can't fail writing to vec");
        bytes.write_all(&rb).expect("can't fail writing to vec");
    }

    let rs_encoded = match right_sibling {
        Some(rs) => *rs,
        None => 0,
    };
    bytes
        .write_all(&(rs_encoded as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    bytes
}

fn encode_internal(
    Internal {
        boundary_keys,
        children,
    }: &Internal,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(boundary_keys.len() * 20 + children.len() * 4);

    bytes
        .write_all(&(boundary_keys.len() as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    for k in boundary_keys {
        let kb = k.as_bytes();
        bytes
            .write_all(&(kb.len() as u32).to_be_bytes())
            .expect("can't fail writing to vec");
        bytes.write_all(kb).expect("can't fail writing to vec");
    }

    bytes
        .write_all(&(children.len() as u32).to_be_bytes())
        .expect("can't fail writing to vec");

    for c in children {
        bytes
            .write_all(&(*c as u32).to_be_bytes())
            .expect("can't fail writing to vec");
    }

    bytes
}

fn encode_body(body: &NodeBody) -> Vec<u8> {
    match body {
        NodeBody::Internal(internal) => encode_internal(internal),
        NodeBody::Leaf(leaf) => encode_leaf(leaf),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use super::super::Leaf;
    use crate::table::btree::row::{Row, RowCol, RowVarChar};

    #[test]
    fn test_encode_decode_leaf() {
        let leaf_node = Node {
            id: 7,
            order: 10,
            body: NodeBody::Leaf(Leaf {
                rows: vec![Row {
                    body: vec![
                        RowCol::Int(7),
                        RowCol::Bool(true),
                        RowCol::VarChar(RowVarChar {
                            inline: "Hello, World!".to_string(),
                            next_page: Some(11),
                        }),
                    ],
                }],
                right_sibling: Some(11),
            }),
        };

        let encoded = encode_node(&leaf_node);
        let decoded = decode_node(&encoded).unwrap();

        assert_eq!(leaf_node, decoded)
    }

    #[test]
    fn test_encode_decode_internal() {
        let internal_node = Node {
            id: 7,
            order: 10,
            body: NodeBody::Internal(Internal {
                boundary_keys: vec!["hello".to_string(), "world".to_string()],
                children: vec![5, 7, 11],
            }),
        };

        let encoded = encode_node(&internal_node);
        let decoded = decode_node(&encoded).unwrap();

        assert_eq!(internal_node, decoded)
    }
}
