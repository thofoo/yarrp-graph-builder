use std::collections::HashSet;
use std::fs::File;
use std::sync::Mutex;

use csv::Writer;
use log::info;
use math::round::ceil;
use pbr::ProgressBar;
use rayon::prelude::*;
use crate::graph::betweenness::BetweennessCalculatorMethod;

use crate::graph::betweenness::brandes::brandes_memory::BrandesMemory;
use crate::graph::common::collection_wrappers::GettableList;
use crate::graph::common::graph::Graph;
use crate::graph::common::offset_list::OffsetList;
use crate::graph::common::sparse_offset_list::SparseOffsetList;

pub struct ApproxSpanningTreeCentralityCalculator {
    graph: Graph,
    writer: Writer<File>,
}

impl ApproxSpanningTreeCentralityCalculator {
    pub fn new(graph: Graph, writer: Writer<File>) -> ApproxSpanningTreeCentralityCalculator {
        ApproxSpanningTreeCentralityCalculator { graph, writer }
    }

    fn calculate_and_persist(&mut self) {
        let c_list = &self.compute_betweenness_in_parallel();

        let boundaries = self.graph.edges().node_boundaries();

        self.writer.serialize(("node_id", "approx_spanning")).unwrap();
        for s in boundaries.range_inclusive() {
            self.writer.serialize((s, c_list[s])).unwrap();
        }
    }

    fn compute_spanning_tree_centrality_values_in_parallel(&self) -> OffsetList<f64> {
        let n = 0.0;
        let delta = 0.0;
        let epsilon = 0.0;

        let q = ceil((2*n / delta).log2() / (2 * epsilon * epsilon), 0);

        let boundaries = self.graph.edges().node_boundaries();
        let mut output: OffsetList<f64> = OffsetList::new(0.0, boundaries.clone());
        let mut T: OffsetList<f64> = OffsetList::new(0.0, boundaries.clone());
        for i in 1..q {
            T[i] = wilson(r)
        }
        output
    }

    fn wilson(&self, r: _) -> _ {

    }

}

impl BetweennessCalculatorMethod for ApproxSpanningTreeCentralityCalculator {
    fn calculate_and_write_to_disk(&mut self) {
        self.calculate_and_persist();
    }
}