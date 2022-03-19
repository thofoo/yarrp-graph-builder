use std::collections::HashSet;
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;

use crate::graph::graph::Graph;

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

        self.writer.serialize(("node_id", "betweenness")).unwrap();
        for s in boundaries.range_inclusive() {
            let value = self.compute_value(s);
            self.writer.serialize((s, value)).unwrap();
        }
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
        let rv: HashSet<i64> = HashSet::new();
        // TODO 1. compute reverse graph (in-place?)
        // TODO 2.1 BFS/DFS on reverse graph, starting from r
        // TODO 2.2 add all visited nodes to result_set
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