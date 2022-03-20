use std::collections::HashSet;
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use crate::graph::common::collection_wrappers::Stack;

use crate::graph::common::graph::Graph;

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

    pub fn write_values_to_disk(&mut self) {
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

            if (s + offset) % 1000 == 0 {
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
        // TODO Form the SPD rooted at spd_root and compute
        // TODO dependency scores of vt on the other vertices.
        0.0
    }
}