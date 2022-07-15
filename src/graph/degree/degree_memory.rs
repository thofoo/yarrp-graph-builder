use std::fs::File;

use csv::Writer;

use crate::common::structs::data::NodeBoundaries;
use crate::graph::common::offset_list::OffsetList;

pub struct DegreeMemory {
    boundaries: NodeBoundaries,
    memory: OffsetList<(u32, u32)>,
}

impl DegreeMemory {
    pub fn new(boundaries: NodeBoundaries) -> DegreeMemory {
        let memory = OffsetList::new((0, 0), boundaries.clone());

        DegreeMemory {
            memory,
            boundaries,
        }
    }

    pub fn set_in_out_count(&mut self, id: i64, count: usize) -> bool {
        // we just increment "in" by one every time we visit the node.
        // we do both "in" and "out" at once to avoid the additional lookup
        // Important: this will set the "in" value for 0 to 1 (it is visited once),
        // so this needs to be removed further down the processing

        let mut entry = self.memory[id];
        entry.0 += 1;

        let is_new = entry.1 == 0;
        if is_new {
            entry.1 = count as u32;
        }

        is_new
    }

    pub fn persist(&mut self, writer: &mut Writer<File>) {
        self.memory[0].0 = 0; // required because the inc method also increments the in degree for 0

        writer.serialize(("node_id", "degree_in", "degree_out")).unwrap();
        for s in self.boundaries.range_inclusive() {
            let values = self.memory[s];
            writer.serialize((s, values.0, values.1)).unwrap();
        }
    }

    pub fn len(&self) -> usize {
        self.memory.len()
    }
}