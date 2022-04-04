use std::collections::HashSet;
use std::fs::File;
use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rand::distributions::Uniform;
use rand::seq::SliceRandom;
use rand::distributions::Distribution;
use crate::graph::common::graph::Graph;
use crate::graph::common::offset_list::OffsetList;

#[allow(dead_code)]
pub struct KpathCentralityCalculator {
    graph: Graph,
    writer: Writer<File>,
}

#[allow(dead_code)]
impl KpathCentralityCalculator {
    pub fn new(graph: Graph, writer: Writer<File>) -> KpathCentralityCalculator {
        KpathCentralityCalculator { graph, writer }
    }

    pub fn write_values_to_disk(&mut self) {
        let k: i32 = 20; // max length of path
        // let alpha = 0.5; // max error modifier
        let double_alpha = 0;

        let neighbors = self.graph.edges();
        let node_count = neighbors.total_nodes();

        info!("Processing {} nodes...", node_count);

        let boundaries = self.graph.edges().node_boundaries();
        let mut rng = rand::thread_rng();

        let node_sampler = Uniform::new_inclusive(
            boundaries.min_node(), boundaries.max_node()
        );
        let k_sampler = Uniform::new(1, k + 1);

        let mut count_list: OffsetList<u32> = OffsetList::new(0, boundaries.clone());
        let mut explored: OffsetList<bool> = OffsetList::new(false, boundaries.clone());

        let t_raw = 2 as f64 * k.pow(2) as f64 * (node_count as i32).pow(1 - double_alpha) as f64 * (node_count as f64).ln();
        let t_floor: u64 = t_raw.floor() as u64;

        let mut progress_bar = ProgressBar::new(t_floor);
        progress_bar.set(0);

        for _ in 1..=t_floor {
            let s = node_sampler.sample(&mut rng);
            let l =  k_sampler.sample(&mut rng);
            explored[s] = true;

            let mut j = 1;
            let mut targets: &HashSet<i64> = &neighbors[s];
            while j <= l && targets.iter().any(|&u| !explored[u]) {
                let unexplored: Vec<&i64> = targets.iter()
                    .filter(|&&u| !explored[u])
                    .collect();

                let v = **unexplored.choose(&mut rng).unwrap();
                explored[v] = true;
                count_list[v] += 1;
                targets = &neighbors[v];
                j += 1;
            }

            explored = OffsetList::new(false, boundaries.clone());
            progress_bar.inc();
        }

        self.writer.serialize(("node_id", "kpc")).unwrap();
        let kn: f64 = (k as i64 * node_count as i64) as f64;
        for v in boundaries.range_inclusive() {
            let value = kn * (count_list[v] as f64 / t_raw);
            self.writer.serialize((v, value)).unwrap();
        }
    }
}