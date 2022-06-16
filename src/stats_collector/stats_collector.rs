use std::collections::HashMap;
use log::info;
use crate::common::structs::output::DegreesForNode;
use crate::GraphBuilderParameters;
use crate::stats_collector::degree::degree_stats_calculator::DegreeStatsCalculator;

pub struct StatsCollector {
    config: GraphBuilderParameters,
}

impl StatsCollector {
    pub fn new(config: &GraphBuilderParameters) -> StatsCollector {
        StatsCollector { config: config.clone() }
    }

    pub fn calculate_stats(&mut self) {
        if !self.config.should_compute_stats() {
            info!("Stats computation flag is FALSE - skipping stats computation.");
            return;
        }

        let should_compute = self.config.graph_parameters_to_compute();
        if should_compute.betweenness {
            self.calculate_betweenness_stats();
        }
        if should_compute.degree {
            let path = self.config.output_paths().degree_folder().to_path_buf();
            let mut calculator = DegreeStatsCalculator::new(path);
            calculator.calculate_degree_stats();
        }
    }

    fn calculate_betweenness_stats(&self) {
        todo!("nothing implemented yet")
    }
}
