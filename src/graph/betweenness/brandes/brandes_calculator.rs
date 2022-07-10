use std::collections::HashSet;
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rayon::prelude::*;

use crate::graph::betweenness::brandes::brandes_memory::BrandesMemory;
use crate::graph::common::collection_wrappers::GettableList;
use crate::graph::common::graph::Graph;
use crate::graph::common::offset_list::OffsetList;
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
        for s in self.graph.edges().keys() {
            self.writer.serialize((s, c_list[s])).unwrap();
        }
    }

    fn compute_betweenness_in_parallel(&mut self) -> OffsetList<f64> {
        let edges = self.graph.edges();

        let nodes = edges.keys();

        info!("Processing {} nodes...", nodes.len());

        let mut partial_results: Vec<SparseOffsetList<f64>> = Vec::new();

        // Make sure to not use too many threads, as that could lead to out-of-memory errors if you
        // have plenty of input data (=> the thread results are piling up before they are collected)
        // Good baseline is to use the number of threads available on your machine
        let num_of_threads = 8.0;
        nodes.chunks(((nodes.len() as f64) / num_of_threads).ceil() as usize)
            .collect::<Vec<&[i64]>>()
            .into_par_iter()
            .map(|nodes_to_visit| {
                let mut local_c_list = SparseOffsetList::new(0.0);
                let mut counter = 0;

                let node_count = nodes_to_visit.len();

                let thread_info = format!("Thread (first node {}): {} nodes", nodes_to_visit[0], node_count);
                info!("{}", thread_info);

                for &s in nodes_to_visit {
                    Self::calculate_delta_for_node(edges, &mut local_c_list, s);
                    counter += 1;
                    if counter % 1_000 == 0 {
                        let thread_info = format!("Thread (first node {}): {} / {}", nodes_to_visit[0], counter, node_count);
                        info!("{}", thread_info);
                    }
                }

                info!("Thread (first node {}): finished", nodes_to_visit[0]);
                local_c_list
            })
            .collect_into_vec(&mut partial_results);

        let result_count = partial_results.len() as u64;
        let mut progress_bar = ProgressBar::new(result_count);
        let mut global_c_list: OffsetList<f64> = OffsetList::new(0.0, self.graph.boundaries().clone());
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
        neighbors: &impl GettableList<HashSet<i64>>,
        c_list: &mut impl GettableList<f64>,
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
        q.push(s);

        while !q.is_empty() {
            let v = q.upoll();
            s_stack.push(v);
            for &w in neighbors.get(v) {
                if *d.get(w) < 0 {
                    q.push(w);

                    let d_v = *d.get(v);
                    d.set(w, d_v + 1);
                }
                if *d.get(w) == *d.get(v) + 1 {
                    let sigma_w = *sigma.get(w);
                    let sigma_v = *sigma.get(v);
                    sigma.set(w, sigma_w + sigma_v);
                    p_list.get_mut(w).push(v);
                }
            }
        }

        while !s_stack.is_empty() {
            let w = s_stack.upop();
            for &v in p_list.get(w) {
                let delta_v = *delta.get(v);
                let delta_w = *delta.get(w);
                let sigma_v = *sigma.get(v) as f64;
                let sigma_w = *sigma.get(w) as f64;
                delta.set(v, delta_v + (sigma_v / sigma_w) * (1.0 + delta_w));

                if w != s {
                    *c_list.get_mut(w) += delta.get(w);
                }
            }
        } // https://github.com/v4rrlo/brandes-betweenness-centrality
    }

    pub fn graph(self) -> Graph {
        self.graph
    }
}
