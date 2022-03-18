use std::ops::{Index, IndexMut};
use crate::common::structs::data::MaxNodeIds;

pub struct OffsetList<T> {
    vec: Vec<T>,
    offset: usize,
    total_nodes: usize,
    max_node_ids: MaxNodeIds,
}

impl <T: Clone> OffsetList<T> {
    pub fn new(default: T, max_node_ids: MaxNodeIds) -> OffsetList<T> {
        let positive_size = max_node_ids.known + 1;
        let negative_size = max_node_ids.unknown; // no +1 because no "0 node"
        let size = positive_size + negative_size;

        OffsetList {
            vec: vec![default; size],
            offset: negative_size,
            total_nodes: size,
            max_node_ids,
        }
    }

    pub fn new_same_size_as<DontCare>(default: T, list: &OffsetList<DontCare>) -> OffsetList<T> {
        OffsetList::new(
            default,
            MaxNodeIds {
                known: list.total_nodes - list.offset - 1,
                unknown: list.offset,
            },
        )
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn total_nodes(&self) -> usize {
        self.total_nodes
    }
    pub fn max_node_ids(&self) -> &MaxNodeIds {
        &self.max_node_ids
    }
}

impl <T> Index<i64> for OffsetList<T> {
    type Output = T;

    fn index(&self, index: i64) -> &Self::Output {
        &self.vec[(self.offset as i64 + index).unsigned_abs() as usize]
    }
}

impl <T> IndexMut<i64> for OffsetList<T> {
    fn index_mut(&mut self, index: i64) -> &mut Self::Output {
        &mut self.vec[(self.offset as i64 + index).unsigned_abs() as usize]
    }
}
