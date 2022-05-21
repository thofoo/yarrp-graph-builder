use log::info;

use crate::common::structs::data::MaxNodeIds;
use crate::graph::betweenness::betweenness_methods::BetweennessMethod;
use crate::graph::betweenness::BetweennessCalculator;
use crate::graph::common::graph::Graph;
use crate::GraphBuilderParameters;

pub struct Grapher {
    config: GraphBuilderParameters,
}

impl Grapher {
    pub fn new(config: &GraphBuilderParameters) -> Grapher {
        Grapher { config: config.clone() }
    }

    pub fn collect_graph_stats(&self) {
        if !self.config.should_compute_graph() {
            info!("Graph computation flag is FALSE - skipping graph computation.");
            return;
        }

        let graph = self.build_graph();
        self.calculate_graph_parameters(graph);
    }

    fn build_graph(&self) -> Graph {
        info!("Building in-memory graph for calculating graph values. This may take a while \
        but only has to be done per run.");

        let edges_path = self.config.output_paths().edges();
        let max_node_id_path = self.config.output_paths().max_node_ids();

        let max_node_ids: MaxNodeIds = csv::Reader::from_path(max_node_id_path).unwrap()
            .deserialize()
            .next()
            .unwrap()
            .unwrap();

        let mut graph = Graph::new(max_node_ids);
        graph.parse(edges_path);
        graph
    }

    fn calculate_graph_parameters(&self, graph: Graph) {
        self.calculate_betweenness(graph);
        // TODO add more parameter types here
    }

    fn calculate_betweenness(&self, graph: Graph) {
        let method = BetweennessMethod::Brandes;
        info!("Calculating BETWEENNESS CENTRALITY using {:?}", method);

        let betweenness_writer = csv::Writer::from_path(&self.config.output_paths().betweenness())
            .expect(&format!(
                "Could not create file for storing betweenness at {}",
                &self.config.output_paths().betweenness().to_str().unwrap()
            ));

        BetweennessCalculator::new(method).calculate(graph, betweenness_writer);
    }
}