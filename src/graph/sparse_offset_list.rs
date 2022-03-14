use std::collections::HashMap;

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

    pub fn get(&mut self, index: usize) -> &T {
        if !self.map.contains_key(&index) {
            let value = self.default.clone();
            self.map.insert(index, value);
        }

        self.map.get(&index).unwrap()
    }

    pub fn get_mut(&mut self, index: usize) -> &mut T {
        if !self.map.contains_key(&index) {
            let value = self.default.clone();
            self.map.insert(index, value);
        }

        self.map.get_mut(&index).unwrap()
    }

    pub fn set(&mut self, index: usize, value: T) {
        self.map.insert(index, value);
    }
}
