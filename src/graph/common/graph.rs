use std::collections::HashSet;
use std::path::PathBuf;
use lazy_init::Lazy;

use crate::common::structs::data::{CsvEdge, MaxNodeIds, NodeBoundaries};
use crate::graph::common::sparse_offset_list::SparseOffsetList;
use crate::GraphBuilderParameters;

pub struct Graph {
    edges: SparseOffsetList<HashSet<i64>>,
    reverse: Lazy<SparseOffsetList<HashSet<i64>>>,
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
            reverse: Lazy::new(),
            boundaries,
        }
    }

    pub fn ensure_reversed_edges_exist(&mut self) {
        let edges = &self.edges;
        self.reverse.get_or_create(|| Self::calculate_reverse_graph(edges));
    }

    /**
     * Returns a reversed version of the edge list.
     * WARNING: You need to call `ensure_reversed_edges_exist` first (once),
     * otherwise you get a panic!().
     */
    pub fn edges_reversed(&self) -> &SparseOffsetList<HashSet<i64>> {
        // We could also just make this function mutable and call get_or_create
        // but then we'd lock the result in a mutable borrow, which creates issues

        if let Some(reversed) = self.reverse.get() {
            reversed
        } else {
            panic!("ERROR: You tried accessing the reverse edges before initializing them. \
            Please make sure to call `ensure_reversed_edges_exist` before using this function.");
        }
    }

    fn calculate_reverse_graph(edges: &SparseOffsetList<HashSet<i64>>) -> SparseOffsetList<HashSet<i64>> {
        let mut reversed: SparseOffsetList<HashSet<i64>> = SparseOffsetList::new(
            HashSet::<i64>::new(),
        );

        for s in edges.keys() {
            for &u in &edges[s] {
                reversed[u].insert(s);
            }
        }

        reversed
    }

    fn parse(&mut self, edges_path: &PathBuf) {
        let mut edges_reader = csv::Reader::from_path(edges_path).unwrap();
        edges_reader.deserialize()
            .skip(1)
            .filter(|edge| edge.is_ok())
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
