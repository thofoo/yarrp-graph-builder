use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;

use crate::graph::common::collection_wrappers::Stack;
use crate::graph::common::graph::Graph;
use crate::graph::degree::degree_memory::DegreeMemory;

pub struct DegreeCounter {
    graph: Graph,
    writer: Writer<File>
}

impl DegreeCounter {
    pub fn new(graph: Graph, writer: Writer<File>) -> DegreeCounter {
        DegreeCounter {
            graph, writer
        }
    }

    pub fn calculate_and_persist(&mut self) {
        let mut memory = DegreeMemory::new();

        let edges = self.graph.edges();
        let mut node_stack = Stack::<i64>::new();
        node_stack.push(0);

        info!("Counting degrees for all nodes...");
        let mut progress_bar = ProgressBar::new(self.graph.boundaries().len() as u64);
        let mut counter = 0;

        while !node_stack.is_empty() {
            let node = node_stack.upop();
            let next_nodes = &edges[node];
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

        self.persist(memory);
    }

    fn persist(&mut self, memory: DegreeMemory) {
        let mut progress_bar = ProgressBar::new(self.graph.boundaries().len() as u64);
        let mut counter = 0;

        info!("Calculating values and writing to file...");
        self.writer.serialize((
            "node_id",
            "degree_in",
            "degree_out",
            "and_in",
            "and_out",
            "and_total",
            "iand_in",
            "iand_out",
            "iand_total",
        )).unwrap();

        for node in self.graph.boundaries().range_inclusive() {
            self.writer.serialize(
                memory.collect_values_for_node(node, &mut self.graph).as_tuple()
            ).unwrap();

            counter += 1;
            if counter % 10_000 == 0 {
                progress_bar.add(10_000);
            }
        }

        self.writer.flush().unwrap();

        progress_bar.finish();
    }


    pub fn graph(self) -> Graph {
        self.graph
    }
}
