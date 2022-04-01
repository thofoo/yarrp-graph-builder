use log::info;

use crate::common::structs::data::MaxNodeIds;
use crate::graph::bcd::bcd::BcdCalculator;
use crate::graph::brandes::betweenness::BetweennessCalculator;
use crate::graph::kpath::kpath_centrality::KpathCentralityCalculator;
use crate::graph::common::graph::Graph;
use crate::GraphBuilderParameters;

pub struct Grapher {
    config: GraphBuilderParameters,
}

impl Grapher {
    pub fn new(config: &GraphBuilderParameters) -> Grapher {
        Grapher { config: config.clone() }
    }

    pub fn collect_graph_stats(self) {
        if !self.config.should_compute_graph() {
            info!("Graph computation flag is FALSE - skipping graph computation.");
            return;
        }

        let betweenness_writer = csv::Writer::from_path(&self.config.output_paths().betweenness())
            .expect(&format!(
                "Could not create file for storing betweenness at {}",
                &self.config.output_paths().betweenness().to_str().unwrap()
            ));

        let edges_path = self.config.output_paths().edges();
        let max_node_id_path = self.config.output_paths().max_node_ids();

        let max_node_ids: MaxNodeIds = csv::Reader::from_path(max_node_id_path).unwrap()
            .deserialize()
            .next()
            .unwrap()
            .unwrap();

        let mut graph = Graph::new(max_node_ids);
        graph.parse(edges_path);

        // on 1 file of V4:
        // RUST_BACKTRACE=1 ./target/release/yarrp-graph-builder  77.35s user 0.95s system 99% cpu 1:18.31 total
        // on all of V4 (12671145 nodes) (i am negatively surprised):
        // RUST_BACKTRACE=1 ./target/release/yarrp-graph-builder  16984.65s user 162.81s system 99% cpu 4:46:18.79 total
        // on 4 files of V6:
        // RUST_BACKTRACE=1 ./target/release/yarrp-graph-builder  3709.02s user 164.09s system 98% cpu 1:05:20.81 total
        let mut calculator = BetweennessCalculator::new(graph, betweenness_writer);
        calculator.write_values_to_disk();

        return;

        let mut calculator = BcdCalculator::new(graph, betweenness_writer);
        calculator.write_values_to_disk();

        let mut calculator = KpathCentralityCalculator::new(graph, betweenness_writer);
        calculator.write_values_to_disk();
    }
}