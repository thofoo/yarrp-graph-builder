use crate::graph::collection_wrappers::{Queue, Stack};
use crate::graph::sparse_offset_list::SpareOffsetList;

pub struct BetweennessMemory {
    s_stack: Stack<usize>,
    p_list: SpareOffsetList<Vec<usize>>,
    sigma: SpareOffsetList<u64>,
    d: SpareOffsetList<i64>,
    q: Queue<usize>,
    delta: SpareOffsetList<f64>,
}

impl BetweennessMemory {
    pub fn new() -> BetweennessMemory {
        BetweennessMemory {
            s_stack: Stack::new(),
            p_list: SpareOffsetList::new(Vec::<usize>::new()),
            sigma: SpareOffsetList::new(0),
            d: SpareOffsetList::new(-1),
            q: Queue::new(),
            delta: SpareOffsetList::new(0.0),
        }
    }

    pub fn s_stack(&self) -> &Stack<usize> {
        &self.s_stack
    }
    pub fn p_list(&self) -> &SpareOffsetList<Vec<usize>> {
        &self.p_list
    }
    pub fn sigma(&self) -> &SpareOffsetList<u64> {
        &self.sigma
    }
    pub fn d(&self) -> &SpareOffsetList<i64> {
        &self.d
    }
    pub fn q(&self) -> &Queue<usize> {
        &self.q
    }
    pub fn delta(&self) -> &SpareOffsetList<f64> {
        &self.delta
    }
}