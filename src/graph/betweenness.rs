use std::collections::HashSet;
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use crate::graph::betweenness_memory::BetweennessMemory;

use crate::graph::graph::Graph;
use crate::graph::offset_list::OffsetList;

pub struct BetweennessCalculator {
    graph: Graph,
    writer: Writer<File>,
}

impl BetweennessCalculator {
    pub fn new(graph: Graph, writer: Writer<File>) -> BetweennessCalculator {
        BetweennessCalculator { graph, writer }
    }

    pub fn write_values_to_disk(&mut self) {
        let neighbors = self.graph.edges();
        let node_count = neighbors.total_nodes();

        info!("Processing {} nodes...", node_count);
        let mut progress_bar = ProgressBar::new(node_count as u64);
        progress_bar.set(0);

        let mut c_list: OffsetList<f64> = OffsetList::new_same_size_as(0.0, neighbors);
        for s in 0..node_count {
            progress_bar.set(s as u64);

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
                for w in self.get_offset_neighbors(neighbors, v) {
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
                        c_list[w] += delta.get(w);
                    }
                }
            }
        }

        let offset = neighbors.offset();
        self.writer.serialize(("node_id", "betweenness")).unwrap();
        for s in 0..node_count {
            let node_id = s - offset;
            self.writer.serialize((node_id, c_list[s])).unwrap();
        }

        progress_bar.set(node_count as u64);
    }

    fn get_offset_neighbors(&self, neighbors: &OffsetList<HashSet<i64>>, node_id: usize) -> Vec<usize> {
        neighbors[node_id].iter()
            .map(|&i| i.unsigned_abs() as usize)
            .collect()
    }
}