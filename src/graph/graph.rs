use std::collections::HashSet;
use std::path::PathBuf;
use crate::common::structs::data::{CsvEdge, MaxNodeIds};

pub struct Graph {
    known_node_edges: Vec<HashSet<i64>>,
    // list at node 0 is deliberately left empty to avoid off-by-one errors from conversions
    // this list represents the _negative_ node ids, i.e. the ones without an IP
    unknown_node_edges: Vec<HashSet<i64>>,
}

impl Graph {
    pub fn new(max_node_ids: MaxNodeIds) -> Graph {
        Graph {
            known_node_edges: vec![HashSet::<i64>::new(); max_node_ids.known + 1],
            unknown_node_edges: vec![HashSet::<i64>::new(); max_node_ids.unknown + 1],
        }
    }

    pub fn parse(&mut self, edges_path: &PathBuf) {
        let mut edges_reader = csv::Reader::from_path(edges_path).unwrap();
        edges_reader.deserialize()
            .skip(1)
            .take_while(|edge| edge.is_ok())
            .for_each(|edge: Result<CsvEdge, _>| {
                let data = edge.unwrap();

                if data.from >= 0 {
                    self.known_node_edges[data.from.unsigned_abs() as usize].insert(data.to);
                } else {
                    self.unknown_node_edges[data.from.unsigned_abs() as usize].insert(data.to);
                }
            });
    }

    pub fn known_node_edges(&self) -> &Vec<HashSet<i64>> {
        &self.known_node_edges
    }

    pub fn unknown_node_edges(&self) -> &Vec<HashSet<i64>> {
        &self.unknown_node_edges
    }
}