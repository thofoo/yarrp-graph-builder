use std::fs::File;
use csv::Writer;
use crate::graph::betweenness::betweenness_methods::BetweennessMethod;
use crate::graph::betweenness::brandes::brandes_calculator::BrandesCalculator;
use crate::graph::common::graph::Graph;

pub mod betweenness_methods;
mod brandes;

pub struct BetweennessCalculator {
    betweenness_method: BetweennessMethod
}

impl BetweennessCalculator {
    pub fn new(betweenness_method: BetweennessMethod) -> BetweennessCalculator {
        BetweennessCalculator { betweenness_method }
    }

    pub fn calculate(&self, graph: Graph, writer: Writer<File>) -> Graph {
        match &self.betweenness_method {
            BetweennessMethod::Brandes => self.run_brandes(graph, writer),
        }
    }

    fn run_brandes(&self, graph: Graph, writer: Writer<File>) -> Graph {
        let mut calculator = BrandesCalculator::new(graph, writer);
        calculator.calculate_and_persist();
        calculator.graph()
    }
}
