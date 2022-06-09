use std::collections::HashSet;
use std::path::PathBuf;

use lazy_init::Lazy;

use crate::common::structs::data::{CsvEdge, MaxNodeIds, NodeBoundaries};
use crate::graph::common::collection_wrappers::Queue;
use crate::graph::common::offset_list::OffsetList;
use crate::graph::common::sparse_offset_list::SparseOffsetList;

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

    pub fn calculate_shortest_path_dag(&self, root: i64) -> SparseOffsetList<HashSet<i64>> {
        let mut spd: SparseOffsetList<HashSet<i64>> = SparseOffsetList::new(HashSet::new());

        let mut node_queue: Queue<i64> = Queue::new();
        node_queue.push(root);

        let mut already_queued_nodes: HashSet<i64> = HashSet::new();

        while !node_queue.is_empty() {
            let n = node_queue.upoll();

            let successors = &self.edges[n];
            for &successor in successors {
                if !spd.has(successor) && !already_queued_nodes.contains(&successor) {
                    spd.get_mut(n).insert(successor);
                    node_queue.push(successor);
                    already_queued_nodes.insert(successor);
                }
            }
        }

        spd
    }

    pub fn boundaries(&self) -> &NodeBoundaries {
        &self.boundaries
    }
}
