use std::ops::{Index, IndexMut};

pub struct OffsetList<T> {
    vec: Vec<T>,
    offset: usize,
    total_nodes: usize,
}

impl <T: Clone> OffsetList<T> {
    pub fn new(default: T, max_positive: usize, max_negative: usize) -> OffsetList<T> {
        let positive_size = max_positive + 1;
        let negative_size = max_negative; // no +1 because no "0 node"
        let size = positive_size + negative_size;

        OffsetList {
            vec: vec![default; size],
            offset: negative_size,
            total_nodes: size,
        }
    }

    pub fn new_same_size_as<DontCare>(default: T, list: &OffsetList<DontCare>) -> OffsetList<T> {
        OffsetList::new(
            default,
            list.total_nodes - list.offset - 1,
            list.offset,
        )
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn total_nodes(&self) -> usize {
        self.total_nodes
    }
}

impl <T> Index<usize> for OffsetList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.vec[self.offset + index]
    }
}

impl <T> IndexMut<usize> for OffsetList<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.vec[self.offset + index]
    }
}
