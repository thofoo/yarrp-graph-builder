use std::collections::HashSet;
use std::path::PathBuf;

use crate::common::structs::data::{CsvEdge, MaxNodeIds, NodeBoundaries};
use crate::graph::common::sparse_offset_list::SparseOffsetList;
use crate::GraphBuilderParameters;

pub struct Graph {
    edges: SparseOffsetList<HashSet<i64>>,
    boundaries: NodeBoundaries,
}

impl Graph {
    pub fn new(config: &GraphBuilderParameters, from_deduplicated: bool) -> Graph {
        let max_node_id_path = config.output_paths().max_node_ids();

        let max_node_ids: MaxNodeIds = csv::Reader::from_path(max_node_id_path).unwrap()
            .deserialize()
            .next()
            .unwrap()
            .unwrap();

        let edges_path = if from_deduplicated {
            config.output_paths().edges_deduplicated()
        } else {
            config.output_paths().edges()
        };

        let mut graph = Graph::init(max_node_ids);
        graph.parse(edges_path);
        graph
    }

    fn init(max_node_ids: MaxNodeIds) -> Graph {
        let boundaries = NodeBoundaries::new(max_node_ids);
        Graph {
            edges: SparseOffsetList::new(
                HashSet::<i64>::new()
            ),
            boundaries,
        }
    }

    fn parse(&mut self, edges_path: &PathBuf) {
        let mut edges_reader = csv::Reader::from_path(edges_path).unwrap();
        edges_reader.deserialize()
            .skip(1)
            .take_while(|edge| edge.is_ok())
            .for_each(|edge: Result<CsvEdge, _>| {
                let data = edge.unwrap();
                let data_from = data.from;
                self.edges[data_from].insert(data.to);
            });
    }

    pub fn edges(&self) -> &SparseOffsetList<HashSet<i64>> {
        &self.edges
    }

    pub fn boundaries(&self) -> &NodeBoundaries {
        &self.boundaries
    }
}
