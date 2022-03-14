use std::collections::HashMap;
use std::ops::{Index, IndexMut};

pub struct SpareOffsetList<T: Clone> {
    map: HashMap<usize, T>,
    default: T
}

impl <T: Clone> SpareOffsetList<T> {
    pub fn new(default: T) -> SpareOffsetList<T> {
        SpareOffsetList {
            map: HashMap::new(),
            default: default.clone(),
        }
    }
}

impl <T: Clone> Index<usize> for SpareOffsetList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.map.get(&index).get_or_insert(&self.default.clone())
    }
}

impl <T: Clone> IndexMut<usize> for SpareOffsetList<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.map.get_mut(&index).get_or_insert(&mut self.default.clone())
    }
}
