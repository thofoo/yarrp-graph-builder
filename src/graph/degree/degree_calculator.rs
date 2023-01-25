use std::collections::HashSet;

use crate::graph::common::graph::Graph;
use crate::graph::common::sparse_list::SparseList;

pub struct DegreeCalculator {
}

impl DegreeCalculator {
    pub fn new() -> DegreeCalculator {
        DegreeCalculator {  }
    }

    pub fn collect_values_for_node(&self, node_id: i64, graph: &mut Graph) -> DegreeValues {
        graph.ensure_reversed_edges_exist();

        let mut results = DegreeValues::new(node_id);

        results.d_in = graph.edges_reversed()[node_id].len() as u32;
        results.d_out = graph.edges()[node_id].len() as u32;

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
        edges: &SparseList<HashSet<i64>>,
        reverse_edges: &SparseList<HashSet<i64>>,
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

    fn obtain_second_degree_neighborhood(node_id: i64, edges: &SparseList<HashSet<i64>>) -> Vec<i64> {
        edges[node_id].iter()
            .flat_map(|&neighbor| &edges[neighbor])
            .map(|&n| n)
            .collect()
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

    pub fn is_non_zero(&self) -> bool {
        !(self.d_in == 0 && self.d_out == 0)
    }
}

enum Direction {
    IN,
    OUT,
    BOTH,
}