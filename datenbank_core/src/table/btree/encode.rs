use std::io::Write;

use nom::combinator::map;
use nom::number::complete::be_u32;
use nom::sequence::pair;
use nom::IResult;

use super::BTree;
use crate::pagestore::{PageID, TablePageStore};

// The encoding for a BTree is just:
//   - the order encoded as 4 byte integer
//   - the root node's page id as a 4 byte integer
//     - if the root node has not been initialied, then a value of 0 will be stored here
pub fn encode<S: TablePageStore>(tree: &BTree<S>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8);
    bytes
        .write_all(&(tree.order as u32).to_be_bytes())
        .expect("can't fail writing to vec");
    bytes
        .write_all(&tree.root.unwrap_or_else(|| 0u32.into()).to_be_bytes())
        .expect("can't fail writing to vec");
    bytes
}

pub fn decode(input: &[u8]) -> IResult<&[u8], (usize, Option<PageID>)> {
    pair(
        map(be_u32, |order| order as usize),
        map(be_u32, |rid| match rid {
            0 => None,
            n => Some(n.into()),
        }),
    )(input)
}

#[cfg(test)]
mod test {
    use super::super::*;
    use super::*;
    use crate::pagestore::{MemoryManager, TablePageStoreBuilder, TablePageStoreManager};

    #[test]
    fn test_encode() {
        let mut store_builder = MemoryManager::new(64 * 1024).builder("something").unwrap();
        let empty_tree = BTree {
            name: "something".to_string().into(),
            schema: Schema::new(vec![], None, vec![]).unwrap(),
            order: 1000,
            root: None,
            node_cache: Cache::new(store_builder.build().unwrap()),
            data_cache: Cache::new(store_builder.build().unwrap()),
            store: store_builder.build().unwrap(),
        };
        assert_eq!(vec![0, 0, 3, 232, 0, 0, 0, 0], encode(&empty_tree));

        let filled_tree = BTree {
            name: "something".to_string().into(),
            schema: Schema::new(vec![], None, vec![]).unwrap(),
            order: 1000,
            root: Some(7u32.into()),
            node_cache: Cache::new(store_builder.build().unwrap()),
            data_cache: Cache::new(store_builder.build().unwrap()),
            store: store_builder.build().unwrap(),
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
