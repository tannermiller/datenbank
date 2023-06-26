use std::io::Write;

use nom::combinator::map;
use nom::number::complete::be_u32;
use nom::sequence::pair;
use nom::IResult;

use super::BTree;
use crate::pagestore::TablePageStore;

// The encoding for a BTree is just:
//   - the order encoded as 4 byte integer
//   - the root node's page id as a 4 byte integer
//     - if the root node has not been initialied, then a value of 0 will be stored here
pub fn encode<S: TablePageStore>(tree: &BTree<S>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8);
    bytes
        .write_all(&(tree.order as u32).to_be_bytes())
        .expect("can't fail writing to vec");
    if let Some(root) = &tree.root {
        bytes
            .write_all(&(*root as u32).to_be_bytes())
            .expect("can't fail writing to vec");
    } else {
        bytes
            .write_all(&0u32.to_be_bytes())
            .expect("can't fail writing to vec");
    }
    bytes
}

pub fn decode(input: &[u8]) -> IResult<&[u8], (usize, Option<usize>)> {
    pair(
        map(be_u32, |order| order as usize),
        map(be_u32, |rid| match rid {
            0 => None,
            n => Some(n as usize),
        }),
    )(input)
}

#[cfg(test)]
mod test {
    use super::super::*;
    use super::*;
    use crate::pagestore::MemoryBuilder;

    #[test]
    fn test_encode() {
        let mut store_builder = MemoryBuilder::new(64 * 1024);
        let empty_tree = BTree {
            name: "something".to_string(),
            schema: Schema::new(vec![], None).unwrap(),
            order: 1000,
            root: None,
            node_cache: Cache::new(store_builder.build("something").unwrap()),
            data_cache: Cache::new(store_builder.build("something").unwrap()),
            store: store_builder.build("something").unwrap(),
        };
        assert_eq!(vec![0, 0, 3, 232, 0, 0, 0, 0], encode(&empty_tree));

        let filled_tree = BTree {
            name: "something".to_string(),
            schema: Schema::new(vec![], None).unwrap(),
            order: 1000,
            root: Some(7),
            node_cache: Cache::new(store_builder.build("something").unwrap()),
            data_cache: Cache::new(store_builder.build("something").unwrap()),
            store: store_builder.build("something").unwrap(),
        };
        let encoded = encode(&filled_tree);
        assert_eq!(&vec![0, 0, 3, 232, 0, 0, 0, 7], &encoded);

        let (rest, (order, root_id)) = decode(&encoded).unwrap();
        assert!(rest.is_empty());
        assert_eq!(filled_tree.order, order);
        assert_eq!(
            filled_tree.root.as_ref().unwrap(),
            root_id.as_ref().unwrap()
        );
    }
}
