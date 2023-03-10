use std::io::Write;

use super::cache::Page;
use super::row::Row;
use super::Error;

// This represents a single node in the B+ Tree, it contains the metadata of the node as well as
// the node body itself.
#[derive(Debug, Clone)]
pub(crate) struct Node {
    // The ID of this node acts as the key we use to store it with.
    pub(crate) id: usize,
    // the order or branching factor of this B+ Tree
    pub(crate) order: usize,
    pub(crate) body: NodeBody,
}

impl Node {
    pub(crate) fn new_leaf(id: usize, order: usize) -> Node {
        Node {
            id,
            order,
            body: NodeBody::Leaf(Leaf {
                rows: vec![],
                right_sibling: None,
            }),
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
}

impl Page for Node {
    fn encode(&self) -> Vec<u8> {
        todo!()
    }

    fn decode(data: &[u8]) -> Result<Self, Error> {
        todo!()
    }
}

// This represents which type this node is and contains the type-specific data for each one.
#[derive(Debug, Clone)]
pub(crate) enum NodeBody {
    // Internal nodes contain the boundary keys (first of each next child) and the pointers to the
    // child nodes. No rows are stored here.
    Internal(Internal),
    // Leaf nodes contain the keys and rows that correspond to them.
    Leaf(Leaf),
}

impl NodeBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            NodeBody::Internal(Internal {
                boundary_keys,
                children,
            }) => {
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
            NodeBody::Leaf(Leaf {
                rows,
                right_sibling,
            }) => {
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

    pub(crate) fn left_child(&self) -> String {
        match self {
            NodeBody::Leaf(Leaf { rows, .. }) => rows[0].key(),
            NodeBody::Internal(Internal { children, .. }) => children[0].to_string(),
        }
    }
}

// Houses the internal-node specific fields for a node. The boundary_keys will always have a length
// one less than the children as they come from the first child of each non-first child.
#[derive(Debug, Clone)]
pub(crate) struct Internal {
    pub(crate) boundary_keys: Vec<String>, // TODO: should these be Vec<u8>?
    pub(crate) children: Vec<usize>,
}

impl Internal {
    // insert a child node into this internal node, if this node has to split, the new right
    // sibling's body is returned in the option
    pub(crate) fn insert_child(
        &mut self,
        order: usize,
        child_id: usize,
        child_key: String,
    ) -> Result<Option<NodeBody>, Error> {
        if self.children.len() + 1 < order {
            match self.boundary_keys.binary_search(&child_key) {
                Ok(_) => Err(Error::DuplicateEntry(child_key)),
                Err(i) => {
                    // insert this child into this node without splitting, the boundary
                    // keys are the first keys in each of the child to the right of the
                    // i+1th child
                    self.boundary_keys.insert(i, child_key);
                    self.children.insert(i + 1, child_id);
                    Ok(None)
                }
            }
        } else {
            // split it
            let midpoint = ((self.children.len() + 1) as f64 / 2.0).ceil() as usize;
            let mut right_sib_children = self.children.split_off(midpoint);
            let mut right_boundary_keys = self.boundary_keys.split_off(midpoint - 1);

            // insert the new child
            let (boundary_keys, children) = if child_key < right_boundary_keys[0] {
                // insert in left sibling
                (&mut self.boundary_keys, &mut self.children)
            } else {
                // insert in right sibling
                (&mut right_boundary_keys, &mut right_sib_children)
            };

            match boundary_keys.binary_search(&child_key) {
                Ok(_) => return Err(Error::DuplicateEntry(child_key)),
                Err(i) => {
                    boundary_keys.insert(i, child_key);
                    children.insert(i + 1, child_id);
                }
            }

            Ok(Some(NodeBody::Internal(Internal {
                boundary_keys: right_boundary_keys,
                children: right_sib_children,
            })))
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Leaf {
    // the actual row data for this leaf node
    pub(crate) rows: Vec<Row>,
    // the leaf node to the right of this one, used for table scans
    pub(crate) right_sibling: Option<usize>,
}

impl Leaf {
    // insert a row into this leaf node, if this node has to split, the new right sibling's body is
    // returned in the option
    pub(crate) fn insert_row(&mut self, order: usize, row: Row) -> Result<Option<NodeBody>, Error> {
        if self.rows.len() + 1 < order {
            match self.rows.binary_search_by_key(&row.key(), |r| r.key()) {
                Ok(_) => Err(Error::DuplicateEntry(row.key())),
                Err(i) => {
                    // don't need to expand, just insert the row and be done
                    self.rows.insert(i, row);
                    Ok(None)
                }
            }
        } else {
            // split the new right sibling out
            let midpoint = ((self.rows.len() + 1) as f64 / 2.0).ceil() as usize;
            let mut right_sib_rows = self.rows.split_off(midpoint);

            let rows = if row.key() < right_sib_rows[0].key() {
                &mut self.rows
            } else {
                &mut right_sib_rows
            };

            // gotta search again because we're not sure what the index is in its new sibling
            match rows.binary_search_by_key(&row.key(), |r| r.key()) {
                Ok(_) => return Err(Error::DuplicateEntry(row.key())),
                Err(i) => {
                    // don't need to expand, just insert the row and be done
                    rows.insert(i, row);
                }
            }

            // TODO: We need to set the right_sibling on the left sibling, and we don't know what
            // it is until we allocate

            Ok(Some(NodeBody::Leaf(Leaf {
                rows: right_sib_rows,
                right_sibling: self.right_sibling,
            })))
        }
    }
}
