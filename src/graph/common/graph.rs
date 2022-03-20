use std::collections::HashSet;
use std::path::PathBuf;
use lazy_init::Lazy;
use crate::common::structs::data::{CsvEdge, MaxNodeIds, NodeBoundaries};
use crate::graph::common::offset_list::OffsetList;

pub struct Graph {
    edges: OffsetList<HashSet<i64>>,
    reverse: Lazy<OffsetList<HashSet<i64>>>,
    boundaries: NodeBoundaries,
}

impl Graph {
    pub fn new(max_node_ids: MaxNodeIds) -> Graph {
        let boundaries = NodeBoundaries::new(max_node_ids);
        Graph {
            edges: OffsetList::new(
                HashSet::<i64>::new(),
                boundaries.clone(),
            ),
            reverse: Lazy::new(),
            boundaries,
        }
    }

    pub fn parse(&mut self, edges_path: &PathBuf) {
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

    pub fn edges(&self) -> &OffsetList<HashSet<i64>> {
        &self.edges
    }

    pub fn reverse_edges(&self) -> &OffsetList<HashSet<i64>> {
        self.reverse.get_or_create(|| self.calculate_reverse_graph())
    }

    fn calculate_reverse_graph(&self) -> OffsetList<HashSet<i64>> {
        let mut reversed: OffsetList<HashSet<i64>> = OffsetList::new(
            HashSet::<i64>::new(),
            self.boundaries.clone(),
        );

        for s in self.boundaries.range_inclusive() {
            for &u in &self.edges[s] {
                reversed[u].insert(s);
            }
        }

        reversed
    }
}