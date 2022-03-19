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

        let mut calculator = BcdCalculator::new(graph, betweenness_writer);
        calculator.write_values_to_disk();

        return;

        let mut calculator = BetweennessCalculator::new(graph, betweenness_writer);
        calculator.write_values_to_disk();

        let mut calculator = KpathCentralityCalculator::new(graph, betweenness_writer);
        calculator.write_values_to_disk();
    }
}