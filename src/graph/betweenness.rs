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

        let c_list: OffsetList<f64> = OffsetList::new_same_size_as(0.0, neighbors);
        for s in 0..node_count {
            if s % 100000 == 0 {
                progress_bar.set(s as u64);
            }

            let mut memory = BetweennessMemory::new();
            let mut s_stack = memory.s_stack();
            let mut p_list = memory.p_list();
            let mut sigma = memory.sigma();
            let mut d = memory.d();
            let mut q = memory.q();
            let mut delta = memory.delta();

            sigma[s] = 1;
            d[s] = 0;
            q.push(s);

            while !q.is_empty() {
                let v = q.upoll();
                s_stack.push(v);
                for w in self.get_offset_neighbors(neighbors, v) {
                    if d[w] < 0 {
                        q.push(w);
                        d[w] = d[v] + 1;
                    }
                    if d[w] == d[v] + 1 {
                        sigma[w] += sigma[v];
                        p_list[w].push(v);
                    }
                }
            }

            while !s_stack.is_empty() {
                let w = s_stack.upop();
                for v in p_list[w] {
                    delta[v] += (sigma[v] as f64 / sigma[w] as f64) * (1.0 + delta[w]);
                    if w != s {
                        c_list[w] += delta[w];
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
            .map(|&i| (i + neighbors.offset() as i64).unsigned_abs() as usize)
            .collect()
    }
}