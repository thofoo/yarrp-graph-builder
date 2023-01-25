use std::collections::VecDeque;
use crate::graph::common::sparse_list::SparseList;

/**
 * Utility struct for the required data structures for the Brandes algorithm.
 */
pub struct BrandesMemory {
    pub s_stack: Vec<i64>,
    pub p_list: SparseList<Vec<i64>>,
    pub sigma: SparseList<u64>,
    pub d: SparseList<i64>,
    pub q: VecDeque<i64>,
    pub delta: SparseList<f64>,

    // this field is not used, though its purpose is to prevent public instantiation of the struct
    _init_gate: bool,
}

impl BrandesMemory {
    pub fn new() -> BrandesMemory {
        BrandesMemory {
            s_stack: Vec::new(),
            p_list: SparseList::new(Vec::<i64>::new()),
            sigma: SparseList::new(0),
            d: SparseList::new(-1),
            q: VecDeque::new(),
            delta: SparseList::new(0.0),
            _init_gate: true,
        }
    }
}