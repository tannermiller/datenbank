use std::io::Write;

use super::cache::Page;
use super::row::Row;
use super::Error;

// This represents a single node in the B+ Tree, it contains the metadata of the node as well as
// the node body itself.
#[derive(Debug, Clone)]
pub(crate) struct Node {
    // The ID of this node acts as the key we use to store it with.
    id: usize,
    // the order or branching factor of this B+ Tree
    order: usize,
    body: NodeBody,
}

impl Node {
    pub(crate) fn new_leaf(id: usize, order: usize) -> Node {
        Node {
            id,
            order,
            body: NodeBody::Leaf {
                rows: vec![],
                right_sibling: None,
            },
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        // we encode the body first even though it goes at the end of the byte vector so that we
        // can correctly allocate the returned vector.
        let body_bytes = self.body.encode();

        // body + id + order + body type
        let mut bytes = Vec::with_capacity(body_bytes.len() + 4 + 4 + 1);

        bytes
            .write_all(&(self.id as u32).to_be_bytes())
            .expect("can't fail writing to vec");

        bytes
            .write_all(&(self.order as u32).to_be_bytes())
            .expect("can't fail writing to vec");

        let body_type = match self.body {
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

    pub fn insert_row(&mut self, row: Row) -> Result<bool, Error> {
        todo!()
    }
}

// This represents which type this node is and contains the type-specific data for each one.
#[derive(Debug, Clone)]
pub(crate) enum NodeBody {
    // Internal nodes contain the boundary keys (first of each next child) and the pointers to the
    // child nodes. No rows are stored here.
    Internal {
        boundary_keys: Vec<String>, // TODO: should these be Vec<u8>?
        children: Vec<usize>,
    },
    // Leaf nodes contain the keys and rows that correspond to them.
    Leaf {
        // the actual row data for this leaf node
        rows: Vec<Row>,
        // the leaf node to the right of this one, used for table scans
        right_sibling: Option<usize>,
    },
}

impl Page for Node {
    fn encode(&self) -> Vec<u8> {
        todo!()
    }

    fn decode(data: &[u8]) -> Result<Self, Error> {
        todo!()
    }
}

impl NodeBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            NodeBody::Internal {
                boundary_keys,
                children,
            } => {
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
            NodeBody::Leaf {
                rows,
                right_sibling,
            } => {
                let mut bytes = Vec::with_capacity(rows.len() * 100 + 4);

                bytes
                    .write_all(&(rows.len() as u32).to_be_bytes())
                    .expect("can't fail writing to vec");

                for row in rows {
                    let rb = row.encode();
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
        }
    }
}
