use std::collections::HashSet;
use std::fs::File;
use std::sync::Mutex;

use csv::Writer;
use linya::{Bar, Progress};
use log::info;
use pbr::ProgressBar;
use rayon::prelude::*;

use crate::graph::brandes::betweenness_memory::BetweennessMemory;
use crate::graph::common::collection_wrappers::GettableList;
use crate::graph::common::graph::Graph;
use crate::graph::common::offset_list::OffsetList;
use crate::graph::common::sparse_offset_list::SparseOffsetList;

pub struct BetweennessCalculator {
    graph: Graph,
    writer: Writer<File>,
}

impl BetweennessCalculator {
    pub fn new(graph: Graph, writer: Writer<File>) -> BetweennessCalculator {
        BetweennessCalculator { graph, writer }
    }

    pub fn write_values_to_disk(&mut self) {
        let c_list = &self.compute_betweenness_in_parallel();

        let boundaries = self.graph.edges().node_boundaries();

        self.writer.serialize(("node_id", "betweenness")).unwrap();
        for s in boundaries.range_inclusive() {
            self.writer.serialize((s, c_list[s])).unwrap();
        }
    }

    fn compute_betweenness_in_parallel(
        &self,
    ) -> OffsetList<f64> {
        let edges = self.graph.edges();

        let node_count = edges.total_nodes();
        info!("Processing {} nodes...", node_count);

        let boundaries = self.graph.edges().node_boundaries();
        let mut partial_results: Vec<SparseOffsetList<f64>> = Vec::new();

        let progress = Mutex::new(Progress::new());

        boundaries.range_inclusive_chopped(100)
            .into_par_iter()
            .map(|(size, range)| {
                let bar: Bar = progress.lock().unwrap().bar(size, format!("Calculating betweenness for {:?}", range));
                let mut local_c_list = SparseOffsetList::new(0.0);

                let mut counter = 0;
                for s in range {
                    Self::calculate_delta_for_node(edges, &mut local_c_list, s);
                    counter += 1;
                    if counter % 10000 == 0 {
                        progress.lock().unwrap().inc_and_draw(&bar, 10000);
                    }
                }
                progress.lock().unwrap().set_and_draw(&bar, size);
                local_c_list
            })
            .collect_into_vec(&mut partial_results);

        let result_count = partial_results.len() as u64;
        let mut progress_bar = ProgressBar::new(result_count);
        let mut global_c_list: OffsetList<f64> = OffsetList::new(0.0, boundaries.clone());
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
        s: i64
    ) {
        let memory = BetweennessMemory::new();
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
        }
    }
}