use std::collections::HashSet;

use crate::graph::common::graph::Graph;
use crate::graph::common::sparse_offset_list::SparseOffsetList;

pub struct DegreeMemory {
    memory: SparseOffsetList<DegreeCount>,
}

impl DegreeMemory {
    pub fn new() -> DegreeMemory {
        DegreeMemory { memory: SparseOffsetList::new(DegreeCount::new()) }
    }

    pub fn set_in_out_count(&mut self, id: i64, count: usize) -> bool {
        // we just increment "in" by one every time we visit the node.
        // we do both "in" and "out" at once to avoid the additional lookup

        let mut entry = &mut self.memory[id];
        if id != 0 {
            entry.d_in += 1;
        }

        let is_new = entry.d_out == 0;
        if is_new {
            entry.d_out = count as u32;
        }

        is_new
    }

    pub fn collect_values_for_node(&self, node_id: i64, graph: &mut Graph) -> DegreeValues {
        graph.ensure_reversed_edges_exist();

        let mut results = DegreeValues::new(node_id);

        let degrees = &self.memory[node_id];
        results.d_in = degrees.d_in;
        results.d_out = degrees.d_out;

        results.and_in = self.average_neighbor_degree(node_id, graph, Direction::IN);
        results.and_out = self.average_neighbor_degree(node_id, graph, Direction::OUT);
        results.and_total = self.average_neighbor_degree(node_id, graph, Direction::BOTH);

        results.iand_in = self.iterated_average_neighbor_degree(node_id, graph, Direction::IN);
        results.iand_out = self.iterated_average_neighbor_degree(node_id, graph, Direction::OUT);
        results.iand_total = self.iterated_average_neighbor_degree(node_id, graph, Direction::BOTH);

        results
    }

    fn average_neighbor_degree(
        &self, node_id: i64, graph: &mut Graph, direction: Direction,
    ) -> f64 {
        let edges = graph.edges();
        let reverse_edges = graph.edges_reversed();
        let mut first_hop_neighbors = Vec::new();

        match direction {
            Direction::IN => {
                first_hop_neighbors.extend(&reverse_edges[node_id]);
            }
            Direction::OUT => {
                first_hop_neighbors.extend(&edges[node_id]);
            }
            Direction::BOTH => {
                first_hop_neighbors.extend(&edges[node_id]);
                first_hop_neighbors.extend(&reverse_edges[node_id]);
            }
        }

        Self::obtain_average(direction, edges, reverse_edges, &mut first_hop_neighbors)
    }

    fn obtain_average(
        direction: Direction,
        edges: &SparseOffsetList<HashSet<i64>>,
        reverse_edges: &SparseOffsetList<HashSet<i64>>,
        neighbors: &mut Vec<i64>
    ) -> f64 {
        if neighbors.is_empty() {
            return 0.0
        }

        let neighbor_sum: usize = neighbors.iter()
            .map(|&neighbor| {
                match direction {
                    Direction::IN => reverse_edges[neighbor].len(),
                    Direction::OUT => edges[neighbor].len(),
                    Direction::BOTH => edges[neighbor].len() + reverse_edges[neighbor].len()
                }
            })
            .sum();

        (neighbor_sum as f64) / (neighbors.len() as f64)
    }

    fn iterated_average_neighbor_degree(
        &self, node_id: i64, graph: &mut Graph, direction: Direction,
    ) -> f64 {
        let edges = graph.edges();
        let reverse_edges = graph.edges_reversed();
        let mut two_hop_neighbors: Vec<i64> = Vec::new();

        match direction {
            Direction::IN => {
                two_hop_neighbors.extend(&reverse_edges[node_id]);
                two_hop_neighbors.extend(
                    Self::obtain_second_degree_neighborhood(node_id, &reverse_edges)
                );
            }
            Direction::OUT => {
                two_hop_neighbors.extend(&edges[node_id]);
                two_hop_neighbors.extend(
                    Self::obtain_second_degree_neighborhood(node_id, &edges)
                );
            }
            Direction::BOTH => {
                two_hop_neighbors.extend(&edges[node_id]);
                two_hop_neighbors.extend(&reverse_edges[node_id]);

                two_hop_neighbors.extend(
                    Self::obtain_second_degree_neighborhood(node_id, &edges)
                );
                two_hop_neighbors.extend(
                    Self::obtain_second_degree_neighborhood(node_id, &reverse_edges)
                );
            }
        }

        Self::obtain_average(direction, edges, reverse_edges, &mut two_hop_neighbors)
    }

    fn obtain_second_degree_neighborhood(node_id: i64, edges: &SparseOffsetList<HashSet<i64>>) -> Vec<i64> {
        edges[node_id].iter()
            .flat_map(|&neighbor| &edges[neighbor])
            .map(|&n| n)
            .collect()
    }
}

struct DegreeCount {
    d_in: u32,
    d_out: u32,
}

impl DegreeCount {
    fn new() -> DegreeCount {
        DegreeCount { d_in: 0, d_out: 0 }
    }
}

impl Clone for DegreeCount {
    fn clone(&self) -> Self {
        DegreeCount {
            d_in: self.d_in,
            d_out: self.d_out,
        }
    }
}

pub struct DegreeValues {
    node_id: i64,
    d_in: u32,
    d_out: u32,
    and_in: f64,
    and_out: f64,
    and_total: f64,
    iand_in: f64,
    iand_out: f64,
    iand_total: f64,
}

impl DegreeValues {
    fn new(node_id: i64) -> DegreeValues {
        DegreeValues {
            node_id,
            d_in: 0,
            d_out: 0,
            and_in: 0.0,
            and_out: 0.0,
            and_total: 0.0,
            iand_in: 0.0,
            iand_out: 0.0,
            iand_total: 0.0,
        }
    }

    pub fn as_tuple(&self) -> (i64, u32, u32, f64, f64, f64, f64, f64, f64) {
        (
            self.node_id,
            self.d_in,
            self.d_out,
            self.and_in,
            self.and_out,
            self.and_total,
            self.iand_in,
            self.iand_out,
            self.iand_total,
        )
    }
}

enum Direction {
    IN,
    OUT,
    BOTH,
}