use crate::graph::common::collection_wrappers::{Queue, Stack};
use crate::graph::common::sparse_offset_list::SparseOffsetList;

pub struct BrandesMemory {
    pub s_stack: Stack<i64>,
    pub p_list: SparseOffsetList<Vec<i64>>,
    pub sigma: SparseOffsetList<u64>,
    pub d: SparseOffsetList<i64>,
    pub q: Queue<i64>,
    pub delta: SparseOffsetList<f64>,

    // this field is not used, though its purpose is to prevent public instantiation
    _init_gate: bool,
}

impl BrandesMemory {
    pub fn new() -> BrandesMemory {
        BrandesMemory {
            s_stack: Stack::new(),
            p_list: SparseOffsetList::new(Vec::<i64>::new()),
            sigma: SparseOffsetList::new(0),
            d: SparseOffsetList::new(-1),
            q: Queue::new(),
            delta: SparseOffsetList::new(0.0),
            _init_gate: true,
        }
    }
}