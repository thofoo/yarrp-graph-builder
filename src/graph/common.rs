pub mod offset_list;
pub mod sparse_offset_list;
pub mod graph;
pub mod collection_wrappers;

use std::fs::File;
use csv::Writer;
use crate::graph::common::graph::Graph;

pub trait GraphParameterCalculator {
    fn calculate(&self, graph: Graph, writer: Writer<File>);
}