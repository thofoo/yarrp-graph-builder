use std::fs::File;

use csv::Writer;
use pbr::ProgressBar;

use crate::graph::common::collection_wrappers::{GettableList, Stack};
use crate::graph::common::graph::Graph;
use crate::graph::degree::degree_memory::DegreeMemory;

pub struct DegreeCounter {
    graph: Graph,
    writer: Writer<File>
}

impl DegreeCounter {
    pub fn calculate_and_persist(&mut self) {
        let mut memory = DegreeMemory::new(self.graph.boundaries().clone());

        let edges = self.graph.edges();
        let mut node_stack = Stack::<i64>::new();
        node_stack.push(0);

        let mut progress_bar = ProgressBar::new(memory.len() as u64);
        let mut counter = 0;

        while !node_stack.is_empty() {
            let node = node_stack.upop();
            let next_nodes = edges.get(node);
            let node_is_new = memory.set_in_out_count(node, next_nodes.len());

            if node_is_new {
                for &next_node in next_nodes {
                    node_stack.push(next_node);
                }

                counter += 1;
                if counter % 10_000 == 0 {
                    progress_bar.add(10_000);
                }
            }
        }
        progress_bar.finish();

        memory.persist(&mut self.writer);
    }


    pub fn graph(self) -> Graph {
        self.graph
    }
}

impl DegreeCounter {
    pub fn new(graph: Graph, writer: Writer<File>) -> DegreeCounter {
        DegreeCounter {
            graph, writer
        }
    }
}