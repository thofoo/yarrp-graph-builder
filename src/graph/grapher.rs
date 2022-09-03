use log::info;
use crate::graph::betweenness::brandes_calculator::BrandesCalculator;

use crate::graph::common::graph::Graph;
use crate::graph::degree::degree_counter::DegreeCounter;
use crate::GraphBuilderParameters;

pub struct Grapher {
    config: GraphBuilderParameters,
}

impl Grapher {
    pub fn new(config: &GraphBuilderParameters) -> Grapher {
        Grapher { config: config.clone() }
    }

    pub fn collect_graph_stats(&self) {
        if !self.config.enabled_features().should_compute_graph() {
            info!("Graph computation flag is FALSE - skipping graph computation.");
            return;
        }

        let graph = self.build_graph();
        self.calculate_graph_parameters(graph);
    }

    fn build_graph(&self) -> Graph {
        info!("Building in-memory graph for calculating graph values. This may take a while \
        but only has to be done once per run.");

        Graph::new(&self.config, /* from_deduplicated = */ true)
    }

    fn calculate_graph_parameters(&self, graph: Graph) {
        let mut graph: Graph = graph;

        let should_compute = self.config.enabled_features().graph_parameters_to_compute();
        if should_compute.degree {
            graph = self.calculate_degree(graph);
        }
        if should_compute.betweenness {
            self.calculate_betweenness(graph);
        }
        // TODO add more parameter types here
    }

    fn calculate_betweenness(&self, graph: Graph) -> Graph {
        info!("Calculating BETWEENNESS CENTRALITY using BRANDES algorithm");

        let betweenness_writer = csv::Writer::from_path(&self.config.output_paths().betweenness())
            .expect(&format!(
                "Could not create file for storing betweenness at {}",
                &self.config.output_paths().betweenness().to_str().unwrap()
            ));

        let mut calculator = BrandesCalculator::new(
            graph,
            self.config.intermediary_file_path(),
            self.config.enabled_features().max_thread_count,
            betweenness_writer
        );
        calculator.calculate_and_persist();
        calculator.graph()
    }

    fn calculate_degree(&self, graph: Graph) -> Graph {
        info!("Calculating IN and OUT degree");

        let degree_writer = csv::Writer::from_path(&self.config.output_paths().degree())
            .expect(&format!(
                "Could not create file for storing degree at {}",
                &self.config.output_paths().degree().to_str().unwrap()
            ));

        let mut calculator = DegreeCounter::new(graph, degree_writer);
        calculator.calculate_and_persist();
        calculator.graph()
    }
}
