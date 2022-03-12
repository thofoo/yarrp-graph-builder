use std::collections::{HashMap, HashSet};
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;

use crate::graph::graph::Graph;

pub struct XeBetweennessCalculator {
    graph: Graph,
    writer: Writer<File>,
}

impl XeBetweennessCalculator {
    pub fn new(graph: Graph, writer: Writer<File>) -> XeBetweennessCalculator {
        XeBetweennessCalculator { graph, writer }
    }

    pub fn write_values_to_disk(&mut self) {
        let node_count = self.graph.known_node_edges().len();
        info!("Processing {} nodes...", node_count);
        let mut progress_bar = ProgressBar::new(node_count as u64);

        progress_bar.set(0);

        self.writer.serialize(("node_id", "xe_betweenness")).unwrap();

        for i in 0..node_count {
            if i % 100000 == 0 {
                progress_bar.set(i as u64);
            }

            self.writer.serialize((i, self.calculate_betweenness(i))).unwrap();
        }

        progress_bar.set(node_count as u64);
    }

    fn calculate_betweenness(&self, node_id: usize) -> f64 {
        let one_hop_neighbors: Vec<i64> = self.graph.known_node_edges().get(node_id).unwrap()
            .into_iter()
            .map(|&i| i)
            .collect();

        let two_hop_neighbors: Vec<i64> = HashSet::<i64>::from_iter(one_hop_neighbors.iter()
            .flat_map(|&neighbor_id| self.get_neighbor_set(neighbor_id))
            .map(|&i| i))
            .into_iter()
            .collect();

        let mut node_and_neighbors: Vec<i64> = Vec::new();
        node_and_neighbors.push(node_id as i64);
        node_and_neighbors.extend(&one_hop_neighbors);
        node_and_neighbors.extend(&two_hop_neighbors);

        let neighbor_mapping: HashMap<i64, usize> = node_and_neighbors.iter().enumerate()
            .map(|(index, &value)| (value, index))
            .collect();

        let u = one_hop_neighbors.len();
        let w = two_hop_neighbors.len();
        let neighbor_size = node_and_neighbors.len();

        let mut dependency: Vec<Vec<f64>> = vec![vec![0.0; neighbor_size]; neighbor_size];
        let mut sum = 0.0;

        for p_index in 1..=u {
            let p = node_and_neighbors[p_index];
            for q_index in (p_index + 1)..=u {
                let q = node_and_neighbors[q_index];

                let result = self.dependency_fn1(p, q);
                dependency[p_index][q_index] = result;
                sum += result;
            }
            for q_index in (u + 1)..=(u + w) {
                let q = node_and_neighbors[q_index];

                let result = self.dependency_fn2(p_index, q, &neighbor_mapping, &dependency);
                dependency[p_index][q_index] = result;
                sum += result;
            }
        }
        for p_index in (u + 1)..=(u + w) {
            for q_index in (p_index + 1)..(u + w) {
                let q = node_and_neighbors[q_index];

                let result = self.dependency_fn2(p_index, q, &neighbor_mapping, &dependency);
                dependency[p_index][q_index] = result;
                sum += result;
            }
        }

        (2.0 * sum as f64) / ((u + w) * (u + w - 1)) as f64
    }

    fn get_neighbor_set(&self, node_id: i64) -> &HashSet<i64> {
        if node_id >= 0 {
            self.graph.known_node_edges().get(node_id.unsigned_abs() as usize).unwrap()
        } else {
            self.graph.unknown_node_edges().get(node_id.unsigned_abs() as usize).unwrap()
        }
    }

    fn dependency_fn1(&self, p: i64, q: i64) -> f64 {
        let p_neighbors = self.get_neighbor_set(p);
        if p_neighbors.contains(&q) {
            0.0
        } else {
            let q_neighbors = self.get_neighbor_set(q);
            let count = p_neighbors.intersection(q_neighbors).count();

            1.0 / count as f64
        }
    }

    fn dependency_fn2(
        &self,
        p_index: usize,
        q: i64,
        neighbor_to_index: &HashMap<i64, usize>,
        dependency: &Vec<Vec<f64>>,
    ) -> f64 {
        let q_neighbors = self.get_neighbor_set(q);

        let mut dependency_values = Vec::<f64>::new();
        for r in q_neighbors {
            let r_index = *neighbor_to_index.get(r).unwrap();

            let tau = if p_index < r_index {
                dependency[p_index][r_index]
            } else {
                dependency[r_index][p_index]
            };

            if tau == 0.0 {
                return 0.0
            } else {
                dependency_values.push(tau);
            }
        }

        math::mean::harmonic(dependency_values.as_slice())
    }
}