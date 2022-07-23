use std::ops::{Index, IndexMut};
use hashbrown::hash_map::Iter;
use hashbrown::HashMap;

pub struct SparseOffsetList<T: Clone> {
    map: HashMap<i64, T>,
    default: T
}

impl <T: Clone> SparseOffsetList<T> {
    pub fn new(default: T) -> SparseOffsetList<T> {
        SparseOffsetList {
            map: HashMap::new(),
            default: default.clone(),
        }
    }

    pub fn get(&self, index: i64) -> &T {
        if !self.map.contains_key(&index) {
            &self.default
        } else {
            self.map.get(&index).unwrap()
        }
    }

    pub fn get_mut(&mut self, index: i64) -> &mut T {
        if !self.map.contains_key(&index) {
            let value = self.default.clone();
            self.map.insert(index, value);
        }

        self.map.get_mut(&index).unwrap()
    }

    pub fn set(&mut self, index: i64, value: T) {
        self.map.insert(index, value);
    }

    pub fn keys(&self) -> Vec<i64> {
        let keys = self.map.keys();
        let mapped_keys: Vec<i64> = keys.map(|i| *i).collect();

        mapped_keys
    }

    pub fn iter(&self) -> Iter<'_, i64, T> {
        self.map.iter()
    }

    pub fn total_nodes(&self) -> usize {
        self.map.keys().len()
    }
}

impl <T: Clone> Index<i64> for SparseOffsetList<T> {
    type Output = T;

    fn index(&self, index: i64) -> &Self::Output {
        &self.get(index)
    }
}

impl <T: Clone> IndexMut<i64> for SparseOffsetList<T> {
    fn index_mut(&mut self, index: i64) -> &mut Self::Output {
        self.get_mut(index)
    }
}
