use std::collections::HashSet;
use std::path::PathBuf;
use crate::common::structs::data::{CsvEdge, MaxNodeIds};
use crate::graph::offset_list::OffsetList;

pub struct Graph {
    edges: OffsetList<HashSet<i64>>
}

impl Graph {
    pub fn new(max_node_ids: MaxNodeIds) -> Graph {
        Graph {
            edges: OffsetList::new(HashSet::<i64>::new(), max_node_ids.known, max_node_ids.unknown),
        }
    }

    pub fn parse(&mut self, edges_path: &PathBuf) {
        let mut edges_reader = csv::Reader::from_path(edges_path).unwrap();
        edges_reader.deserialize()
            .skip(1)
            .take_while(|edge| edge.is_ok())
            .for_each(|edge: Result<CsvEdge, _>| {
                let data = edge.unwrap();
                let data_from = data.from.unsigned_abs() as usize;
                self.edges[data_from].insert(data.to);
            });
    }

    pub fn edges(&self) -> &OffsetList<HashSet<i64>> {
        &self.edges
    }
}