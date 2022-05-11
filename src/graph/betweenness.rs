use std::fs::File;
use csv::Writer;
use crate::graph::betweenness::bcd::bcd_calculator::BcdCalculator;
use crate::graph::betweenness::betweenness_methods::BetweennessMethod;
use crate::graph::betweenness::brandes::brandes_calculator::BrandesCalculator;
use crate::graph::betweenness::kpath::kpath_centrality_calculator::KpathCentralityCalculator;
use crate::graph::common::graph::Graph;

pub mod betweenness_methods;
mod brandes;
mod kpath;
mod bcd;

trait BetweennessCalculatorMethod {
    fn calculate_and_write_to_disk(&mut self);
}

pub struct BetweennessCalculator {
    betweenness_method: BetweennessMethod
}

impl BetweennessCalculator {
    pub fn new(betweenness_method: BetweennessMethod) -> BetweennessCalculator {
        BetweennessCalculator { betweenness_method }
    }

    pub fn calculate(&self, graph: Graph, writer: Writer<File>) {
        match &self.betweenness_method {
            BetweennessMethod::Brandes => self.run_calculation(&mut BrandesCalculator::new(graph, writer)),
            BetweennessMethod::BrandesApprox => todo!("Implement"),
            BetweennessMethod::Bcd => self.run_calculation(&mut BcdCalculator::new(graph, writer)),
            BetweennessMethod::Kpath => self.run_calculation(&mut KpathCentralityCalculator::new(graph, writer)),
        };
    }

    fn run_calculation(&self, calculator: &mut impl BetweennessCalculatorMethod) {
        calculator.calculate_and_write_to_disk();
    }
}
