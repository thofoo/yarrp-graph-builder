/**
 * Wrapper for HashMap to simulate a sparse Vec. Benefit: It does not allocate in-between values (if
 * 2 and 100 are assigned, it will not need memory for 1 to 99).
 * For unassigned values, it returns the default specified in the new function.
 */

use std::ops::{Index, IndexMut};
use hashbrown::hash_map::Iter;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
#[derive(Deserialize)]
pub struct SparseList<T: Clone + Serialize> {
    map: HashMap<i64, T>,
    default: T
}

impl <T: Clone + Serialize> SparseList<T> {
    pub fn new(default: T) -> SparseList<T> {
        SparseList {
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
}

impl <T: Clone + Serialize> Index<i64> for SparseList<T> {
    type Output = T;

    fn index(&self, index: i64) -> &Self::Output {
        &self.get(index)
    }
}

impl <T: Clone + Serialize> IndexMut<i64> for SparseList<T> {
    fn index_mut(&mut self, index: i64) -> &mut Self::Output {
        self.get_mut(index)
    }
}
