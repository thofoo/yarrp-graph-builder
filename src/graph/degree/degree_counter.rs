use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;

use crate::graph::common::graph::Graph;
use crate::graph::degree::degree_calculator::DegreeCalculator;

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

    /**
     * Calculates the degree statistics and writes them to the CSV file writer.
     * Stats:
     *     - degree in/out
     *     - average neigbor degree (and) in/out
     *     - iterated average neigbor degree (iand) in/out
     */
    pub fn calculate_and_persist(&mut self) {
        let calculator = DegreeCalculator::new();

        info!("Counting degrees for all nodes...");
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
            let values = calculator.collect_values_for_node(node, &mut self.graph);

            if values.is_non_zero() {
                self.writer.serialize(values.as_tuple()).unwrap();
            }

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
