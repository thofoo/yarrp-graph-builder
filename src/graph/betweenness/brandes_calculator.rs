use std::collections::{HashSet, VecDeque};
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rayon::prelude::*;

use crate::graph::betweenness::brandes_memory::BrandesMemory;
use crate::graph::common::graph::Graph;
use crate::graph::common::sparse_offset_list::SparseOffsetList;

pub struct BrandesCalculator {
    graph: Graph,
    writer: Writer<File>,
}

impl BrandesCalculator {
    pub fn new(graph: Graph, writer: Writer<File>) -> BrandesCalculator {
        BrandesCalculator { graph, writer }
    }

    pub fn calculate_and_persist(&mut self) {
        let c_list = &self.compute_betweenness_in_parallel();

        self.writer.serialize(("node_id", "betweenness")).unwrap();
        for s in self.graph.boundaries().range_inclusive() {
            let value = c_list[s];
            if value != 0.0 {
                self.writer.serialize((s, value)).unwrap();
            }
        }
    }

    fn compute_betweenness_in_parallel(&mut self) -> SparseOffsetList<f64> {
        let edges = self.graph.edges();

        let nodes = edges.keys();

        info!("Processing {} nodes...", nodes.len());

        let mut partial_results: Vec<SparseOffsetList<f64>> = Vec::new();

        // Make sure to not use too many threads, as that could lead to out-of-memory errors if you
        // have plenty of input data (=> the thread results are piling up before they are collected)
        // Good baseline is to use the number of threads available on your machine
        let mut thread_id = 0;
        let num_of_threads = 8.0;
        nodes.chunks(((nodes.len() as f64) / num_of_threads).ceil() as usize)
            .map(|chunk| {
                let result = (chunk, thread_id);
                thread_id += 1;
                result
            })
            .collect::<Vec<(&[i64], i32)>>()
            .into_par_iter()
            .map(|(nodes_to_visit, index)| {
                let mut local_c_list = SparseOffsetList::new(0.0);
                let mut counter = 0;

                let node_count = nodes_to_visit.len();

                let thread_info = format!("Thread {}: {} nodes", index, node_count);
                info!("{}", thread_info);

                for &s in nodes_to_visit {
                    self.calculate_delta_for_node(edges, &mut local_c_list, s);
                    counter += 1;
                    if counter % 1_000 == 0 {
                        let thread_info = format!("Thread {}: {} / {}", index, counter, node_count);
                        info!("{}", thread_info);
                    }
                }

                info!("Thread {}: finished", index);
                local_c_list
            })
            .collect_into_vec(&mut partial_results);

        let result_count = partial_results.len() as u64;
        let mut progress_bar = ProgressBar::new(result_count);
        let mut global_c_list = SparseOffsetList::new(0.0);
        for result in partial_results {
            for (&node, &value) in result.iter() {
                global_c_list[node] += value;
            }
            progress_bar.inc();
        }
        progress_bar.set(result_count);

        global_c_list
    }

    pub fn calculate_delta_for_node(
        &self,
        neighbors: &SparseOffsetList<HashSet<i64>>,
        c_list: &mut SparseOffsetList<f64>,
        s: i64,
    ) {
        let memory = BrandesMemory::new();
        let mut s_stack = memory.s_stack;
        let mut p_list = memory.p_list;
        let mut sigma = memory.sigma;
        let mut d = memory.d;
        let mut q = memory.q;
        let mut delta = memory.delta;

        sigma.set(s, 1);
        d.set(s, 0);
        q.push_back(s);

        self.calculate_dependencies(&neighbors, &mut s_stack, &mut p_list, &mut sigma, d, q);

        self.accumulate_dependency(s, c_list, &mut s_stack, &mut p_list, &mut sigma, &mut delta)
    }

    fn calculate_dependencies(
        &self,
        neighbors: &SparseOffsetList<HashSet<i64>>,
        s_stack: &mut Vec<i64>,
        p_list: &mut SparseOffsetList<Vec<i64>>,
        sigma: &mut SparseOffsetList<u64>,
        mut d: SparseOffsetList<i64>,
        mut q: VecDeque<i64>
    ) {
        while !q.is_empty() {
            let v = q.pop_front().unwrap();
            s_stack.push(v);
            for &w in &neighbors[v] {
                if d[w] < 0 {
                    q.push_back(w);
                    d.set(w, d[v] + 1);
                }
                if d[w] == d[v] + 1 {
                    let sigma_w = sigma[w];
                    let sigma_v = sigma[v];
                    sigma.set(w, sigma_w + sigma_v);
                    p_list[w].push(v);
                }
            }
        }
    }

    fn accumulate_dependency(
        &self,
        s: i64,
        c_list: &mut SparseOffsetList<f64>,
        s_stack: &mut Vec<i64>,
        p_list: &mut SparseOffsetList<Vec<i64>>,
        sigma: &mut SparseOffsetList<u64>,
        delta: &mut SparseOffsetList<f64>
    ) {
        while !s_stack.is_empty() {
            let w = s_stack.pop().unwrap();
            for &v in &p_list[w] {
                let delta_v = delta[v];
                let delta_w = delta[w];
                let sigma_v = sigma[v] as f64;
                let sigma_w = sigma[w] as f64;
                delta.set(v, delta_v + (sigma_v / sigma_w) * (1.0 + delta_w));

                if w != s {
                    c_list[w] += delta[w];
                }
            }
        }
    }

    pub fn graph(self) -> Graph {
        self.graph
    }
}
