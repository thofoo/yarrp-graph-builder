use std::collections::HashSet;
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use crate::graph::betweenness::BetweennessCalculatorMethod;
use crate::graph::betweenness::brandes::brandes_calculator::BrandesCalculator;
use crate::graph::common::collection_wrappers::Stack;

use crate::graph::common::graph::Graph;
use crate::graph::common::sparse_offset_list::SparseOffsetList;

pub struct BcdCalculator {
    graph: Graph,
    writer: Writer<File>,
    approx_threshold: usize,
    rng: ThreadRng,
}

impl BcdCalculator {
    pub fn new(graph: Graph, writer: Writer<File>) -> BcdCalculator {
        let rng = rand::thread_rng();

        BcdCalculator {
            graph,
            writer,
            approx_threshold: 100,
            rng,
        }
    }

    fn calculate_and_persist(&mut self) {
        let neighbors = self.graph.edges();
        let node_count = neighbors.total_nodes();

        info!("Processing {} nodes...", node_count);
        let mut progress_bar = ProgressBar::new(node_count as u64);
        progress_bar.set(0);

        let boundaries = self.graph.edges().node_boundaries();
        let offset = boundaries.offset();

        self.writer.serialize(("node_id", "betweenness")).unwrap();
        for s in boundaries.range_inclusive() {
            let value = self.compute_value(s);
            self.writer.serialize((s, value)).unwrap();

            if (s + offset) % 1 == 0 {
                progress_bar.set((s + offset).unsigned_abs());
            }
        }

        progress_bar.set(node_count as u64);
    }

    fn compute_value(&mut self, node: i64) -> f64 {
        let rv = self.compute_rv(node);

        if rv.len() > self.approx_threshold {
            self.compute_value_estimate(rv, node)
        } else {
            self.compute_value_exact(rv, node)
        }
    }

    fn compute_rv(&self, node: i64) -> Vec<i64> {
        let mut rv: HashSet<i64> = HashSet::new();

        let reverse = self.graph.reverse_edges();
        let mut node_stack: Stack<i64> = Stack::new();
        node_stack.push(node);

        while !node_stack.is_empty() {
            let n = node_stack.upop();
            for &m in &reverse[n] {
                if !rv.contains(&m) {
                    node_stack.push(m);
                    rv.insert(m);
                }
            }
        }

        rv.remove(&node);

        rv.iter().map(|&i| i).collect()
    }

    fn compute_value_estimate(&mut self, rv: Vec<i64>, node: i64) -> f64 {
        let mut bc = 0.0;
        for _ in 1..=self.approx_threshold {
            let sampled_root = *rv.choose(&mut self.rng).unwrap();
            let delta_vt_of_node = self.compute_delta_vt(sampled_root, node);
            let scaled_delta = (delta_vt_of_node * rv.len() as f64) / self.approx_threshold as f64;

            bc += scaled_delta;
        }
        bc
    }

    fn compute_value_exact(&self, rv: Vec<i64>, node: i64) -> f64 {
        let mut bc = 0.0;
        for v in rv {
            let delta_vt_of_node = self.compute_delta_vt(node, v);
            bc += delta_vt_of_node;
        }
        bc
    }

    fn compute_delta_vt(&self, node: i64, spd_root: i64) -> f64 {
        let mut spd = self.graph.calculate_shortest_path_dag(spd_root);
        self.compute_delta(&mut spd, node)
    }

    fn compute_delta(&self, spd: &mut SparseOffsetList<HashSet<i64>>, target: i64) -> f64 {
        let mut c_list: SparseOffsetList<f64> = SparseOffsetList::new(0.0);

        let keys: Vec<i64> = spd.keys();
        for key in keys {
            BrandesCalculator::calculate_delta_for_node(spd, &mut c_list, key);
        }

        *c_list.get(target)
    }
}

impl BetweennessCalculatorMethod for BcdCalculator {
    fn calculate_and_write_to_disk(&mut self) {
        self.calculate_and_persist();
    }
}