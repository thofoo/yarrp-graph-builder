use std::collections::hash_map::Keys;
use std::collections::HashMap;
use crate::graph::common::collection_wrappers::GettableList;

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

    pub fn get(&mut self, index: i64) -> &T {
        if !self.map.contains_key(&index) {
            let value = self.default.clone();
            self.map.insert(index, value);
        }

        self.map.get(&index).unwrap()
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

    pub fn has(&self, index: i64) -> bool {
        self.map.contains_key(&index)
    }

    pub fn keys(&self) -> Vec<i64> {
        let keys = self.map.keys();
        let mapped_keys: Vec<i64> = keys.map(|i| *i).collect();

        mapped_keys
    }
}

impl <T: Clone> GettableList<T> for SparseOffsetList<T> {
    fn get(&self, index: i64) -> &T {
        &self.map.get(&index).unwrap_or(&self.default)
    }

    fn get_mut(&mut self, index: i64) -> &mut T {
        self.get_mut(index)
    }
}