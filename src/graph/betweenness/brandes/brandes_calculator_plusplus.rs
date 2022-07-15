/*use std::collections::HashSet;
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rayon::prelude::*;

use crate::graph::betweenness::brandes::brandes_memory::BrandesMemory;
use crate::graph::common::collection_wrappers::{Queue, Stack};
use crate::graph::common::graph::Graph;
use crate::graph::common::sparse_offset_list::SparseOffsetList;

pub struct BrandesCalculatorPlusPlus {
    graph: Graph,
    writer: Writer<File>,
}

impl BrandesCalculatorPlusPlus {
    pub fn new(graph: Graph, writer: Writer<File>) -> BrandesCalculatorPlusPlus {
        BrandesCalculatorPlusPlus { graph, writer }
    }

    pub fn calculate_and_persist(&mut self) {
        let c_list = &self.compute_betweenness_in_parallel();

        self.writer.serialize(("node_id", "betweenness")).unwrap();
        for s in self.graph.boundaries().range_inclusive() {
            self.writer.serialize((s, c_list[s])).unwrap();
        }
    }

    fn compute_betweenness_in_parallel(&mut self) -> SparseOffsetList<f64> {
        let edges = self.graph.edges();

        let nodes = Vec::from([
            6,14,8961,1974,6826,20220,10656,4883,108057,135,-1031,893,5439,5493,983,151021,30254,3368,7875,-70,7232,-87,87,456,-28,6018,6471,1159,2185,9599,12394,-13,7797,15825,1488,939,4383,179,15866,-522,-43,-166,-290,-137,-114,4,22653,24,6614,169,13262,43561,-1114,20157,4308,20940,3087,54979,3888,-91,4714,-198,3984,2033,96987,91,8911,4146,-232,1334,-44,-138,-519,-167,2664,-734,-247,-653,-600,6792,3604,6195,759,517161,1011,18811,51925,103092,10487,-16,2029,23239,55552,7328,62907,5676,40133,-665,-15,269649
        ]);

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


                let mut constant_info = SparseOffsetList::new(Vec::<i64>::new());
                for &s in nodes_to_visit {
                    let mut q = Queue::new();
                    q.push(s);

                    while !q.is_empty() {
                        let v = q.upoll();
                        for &w in &edges[v] {
                            if !constant_info[w].contains(w) {

                            }
                            if d[w] < 0 {
                                q.push(w);
                                d.set(w, d[v] + 1);
                            }
                            if d[w] == d[v] + 1 {
                                let sigma_w = sigma[w];
                                let sigma_v = sigma[v];
                                sigma.set(w, sigma_w + sigma_v);
                                p_list.get_mut(w).push(v);
                            }
                        }
                    }
                }


                let node_count = nodes_to_visit.len();

                let thread_info = format!("Thread (first node {}): {} nodes", nodes_to_visit[0], node_count);
                info!("{}", thread_info);

                for &s in nodes_to_visit {
                    self.calculate_delta_for_node(edges, &mut local_c_list, s);
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
        maybe_log(s, "start");
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

        maybe_log(s, "start of loop");

        self.calculate_dependencies(&neighbors, &mut s_stack, &mut p_list, &mut sigma, d, &mut q);

        maybe_log(s, "end of loop, start accumulating dependency");

        self.accumulate_dependency(s, c_list, &mut s_stack, &mut p_list, &mut sigma, &mut delta)
    }

    fn calculate_dependencies(
        &self,
        neighbors: &SparseOffsetList<HashSet<i64>>,
        s_stack: &mut Stack<i64>,
        p_list: &mut SparseOffsetList<Vec<i64>>,
        sigma: &mut SparseOffsetList<u64>,
        mut d: SparseOffsetList<i64>,
        q: &mut Queue<i64>
    ) {
        while !q.is_empty() {
            let v = q.upoll();
            s_stack.push(v);
            for &w in &neighbors[v] {
                if d[w] < 0 {
                    q.push(w);
                    d.set(w, d[v] + 1);
                }
                if d[w] == d[v] + 1 {
                    let sigma_w = sigma[w];
                    let sigma_v = sigma[v];
                    sigma.set(w, sigma_w + sigma_v);
                    p_list.get_mut(w).push(v);
                }
            }
        }
    }

    fn accumulate_dependency(
        &self,
        s: i64,
        c_list: &mut SparseOffsetList<f64>,
        s_stack: &mut Stack<i64>,
        p_list: &mut SparseOffsetList<Vec<i64>>,
        sigma: &mut SparseOffsetList<u64>,
        delta: &mut SparseOffsetList<f64>
    ) {
        while !s_stack.is_empty() {
            let w = s_stack.upop();
            for &v in &p_list[w] {
                let delta_v = delta[v];
                let delta_w = delta[w];
                let sigma_v = sigma[v] as f64;
                let sigma_w = sigma[w] as f64;
                delta.set(v, delta_v + (sigma_v / sigma_w) * (1.0 + delta_w));

                if w != s {
                    *c_list.get_mut(w) += delta[w];
                }
            }
        }

        maybe_log(s, "end accumulating dependency");
    }

    pub fn graph(self) -> Graph {
        self.graph
    }
}

fn maybe_log(node: i64, msg: &str) {
    if node % 100 > -1 {
        info!("{}: {}", node, msg);
    }
}
*/