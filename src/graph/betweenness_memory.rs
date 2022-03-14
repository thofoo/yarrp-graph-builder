use crate::graph::collection_wrappers::{Queue, Stack};
use crate::graph::sparse_offset_list::SpareOffsetList;

pub struct BetweennessMemory {
    pub s_stack: Stack<usize>,
    pub p_list: SpareOffsetList<Vec<usize>>,
    pub sigma: SpareOffsetList<u64>,
    pub d: SpareOffsetList<i64>,
    pub q: Queue<usize>,
    pub delta: SpareOffsetList<f64>,

    // this field is not used, though its purpose is to prevent public instantiation
    _init_gate: bool,
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
            _init_gate: true,
        }
    }
}