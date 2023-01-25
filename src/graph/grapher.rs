use log::info;
use crate::graph::betweenness::brandes_calculator::BrandesCalculator;

use crate::graph::common::graph::Graph;
use crate::graph::degree::degree_counter::DegreeCounter;
use crate::{DatasetConfig, GraphParametersToCompute, OutputPaths};

pub struct Grapher {
    config: DatasetConfig,
    output_paths: OutputPaths,
    parameters: GraphParametersToCompute,
}

impl Grapher {
    pub fn new(
        config: &DatasetConfig,
        output_paths: &OutputPaths,
        parameters: &GraphParametersToCompute
    ) -> Grapher {
        Grapher {
            config: config.clone(),
            output_paths: output_paths.clone(),
            parameters: parameters.clone(),
        }
    }

    /**
     * Builds the graph in memory and calculates the requested statistics.
     *
     *  Requires: edges_deduplicated.csv
     * Generates: One csv file for every request statistic
     */
    pub fn collect_graph_stats(&self) {
        let graph = self.build_graph();
        self.calculate_graph_parameters(graph);
    }

    fn build_graph(&self) -> Graph {
        info!("Building in-memory graph for calculating graph values. This may take a while \
        but only has to be done once per run.");

        Graph::new(&self.output_paths, /* from_deduplicated = */ true)
    }

    fn calculate_graph_parameters(&self, graph: Graph) {
        let mut graph: Graph = graph;

        if self.parameters.degree.enabled {
            graph = self.calculate_degree(graph);
        }
        if self.parameters.betweenness.enabled {
            self.calculate_betweenness(graph);
        }
    }

    /**
     * Calculates the degree statistics and writes them to the degree CSV file (degree.csv)
     */
    fn calculate_degree(&self, graph: Graph) -> Graph {
        info!("Calculating IN and OUT degree");

        let degree_writer = csv::Writer::from_path(&self.output_paths.degree)
            .expect(&format!(
                "Could not create file for storing degree at {}",
                &self.output_paths.degree.to_str().unwrap()
            ));

        let mut calculator = DegreeCounter::new(graph, degree_writer);
        calculator.calculate_and_persist();
        calculator.graph()
    }

    /**
     * Calculates the degree statistics and writes them to the betweenness CSV file (betweenness.csv)
     *
     * IMPORTANT: If intermediate files are present, it will read them in and continue computation
     * from there. If you want to start a new calculation, DELETE THE INTERMEDIATE FILES!
     */
    fn calculate_betweenness(&self, graph: Graph) -> Graph {
        info!("Calculating BETWEENNESS CENTRALITY using BRANDES algorithm");

        let betweenness_writer = csv::Writer::from_path(&self.output_paths.betweenness)
            .expect(&format!(
                "Could not create file for storing betweenness at {}",
                &self.output_paths.betweenness.to_str().unwrap()
            ));

        let mut calculator = BrandesCalculator::new(
            graph,
            &self.config.intermediate_path,
            self.parameters.betweenness.clone(),
            betweenness_writer
        );
        calculator.calculate_and_persist();
        calculator.graph()
    }
}
