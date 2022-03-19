use std::ops::{Index, IndexMut};
use crate::common::structs::data::NodeBoundaries;

pub struct OffsetList<T> {
    vec: Vec<T>,
    offset: usize,
    total_nodes: usize,
    node_boundaries: NodeBoundaries,
}

impl <T: Clone> OffsetList<T> {
    pub fn new(default: T, boundaries: NodeBoundaries) -> OffsetList<T> {
        // let positive_size = max_node_ids.known + 1;
        // let negative_size = max_node_ids.unknown; // no +1 because no "0 node"

        let positive_size = (boundaries.max_node() + 1) as usize;
        let negative_size = -boundaries.min_node() as usize;
        let size = positive_size + negative_size;

        OffsetList {
            vec: vec![default; size],
            offset: negative_size,
            total_nodes: size,
            node_boundaries: boundaries,
        }
    }

    pub fn total_nodes(&self) -> usize {
        self.total_nodes
    }
    pub fn node_boundaries(&self) -> &NodeBoundaries {
        &self.node_boundaries
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
