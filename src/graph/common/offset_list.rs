use std::ops::{Index, IndexMut};
use crate::common::structs::data::NodeBoundaries;

pub struct OffsetList<T> {
    vec: Vec<T>,
    offset: usize,
}

#[allow(dead_code)]
impl <T: Clone> OffsetList<T> {
    pub fn new(default: T, boundaries: NodeBoundaries) -> OffsetList<T> {
        let positive_size = (boundaries.max_node() + 1) as usize;
        let negative_size = -boundaries.min_node() as usize;
        let size = positive_size + negative_size;

        OffsetList {
            vec: vec![default; size],
            offset: negative_size,
        }
    }
    pub fn len(&self) -> usize {
        self.vec.len()
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
