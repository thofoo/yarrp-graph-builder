use std::collections::{HashSet, VecDeque};
use std::fs;
use std::fs::File;
use std::path::PathBuf;

use csv::Writer;
use log::info;
use pbr::ProgressBar;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use crate::BetweennessParameters;

use crate::graph::betweenness::brandes_memory::BrandesMemory;
use crate::graph::common::graph::Graph;
use crate::graph::common::sparse_list::SparseList;
use crate::preprocess::file_util::write_binary_to_file;

#[derive(Serialize)]
#[derive(Deserialize)]
struct BrandesThreadState {
    index: u32,
    counter: u32,
    local_c_list: SparseList<f64>,
}

pub struct BrandesCalculator {
    graph: Graph,
    intermediate_folder_path: PathBuf,
    params: BetweennessParameters,
    writer: Writer<File>,
}

impl BrandesCalculator {
    pub fn new(
        graph: Graph,
        intermediate_folder_path: &PathBuf,
        params: BetweennessParameters,
        writer: Writer<File>
    ) -> BrandesCalculator {
        BrandesCalculator {
            graph,
            intermediate_folder_path: intermediate_folder_path.clone(),
            params,
            writer
        }
    }

    /**
     * Calculates the betweenness centrality for each node and writes the values to the betweenness CSV file.
     */
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

    /**
     * Calculates the betweenness centrality dependencies in parallel. At the end, the intermediate
     * results are summed up to obtain the final values, which are then written to the output file.
     * The implementation follows the original algorithm pseudocode of Brandes (2001) closely.
     *
     * If the intermediate save feature is enabled in the config, after every batch, the
     * intermediate states are dumped as binary files into the intermediate folder.
     *
     * IMPORTANT: If intermediate files are present, it will read them in and continue computation
     * from there. If you want to start a new calculation, DELETE THE INTERMEDIATE FILES!
     */
    fn compute_betweenness_in_parallel(&mut self) -> SparseList<f64> {
        let edges = self.graph.edges();

        let nodes = edges.keys();

        info!("Processing {} nodes...", nodes.len());

        let mut partial_results: Vec<SparseList<f64>> = Vec::new();

        let mut thread_counter: u32 = 0;
        let num_of_threads = self.params.max_thread_count as f64;
        nodes.chunks(((nodes.len() as f64) / num_of_threads).ceil() as usize)
            .map(|chunk| {
                let result = (chunk, thread_counter);
                thread_counter += 1;
                result
            })
            .collect::<Vec<(&[i64], u32)>>()
            .into_par_iter()
            .map(|(nodes_to_visit, thread_id)| {
                let total_node_count = nodes_to_visit.len();

                let thread_info = format!("Thread {}: {} nodes", thread_id, total_node_count);
                info!("{}", thread_info);

                let (mut local_c_list, mut counter) = self.restore_or_create_state(thread_id);

                Self::print_thread_progress(thread_id, counter, total_node_count);

                let nodes_left_to_visit = &nodes_to_visit[(counter as usize)..];
                for &s in nodes_left_to_visit {
                    self.calculate_delta_for_node(edges, &mut local_c_list, s);
                    counter += 1;
                    let batch_size = self.params.result_batch_size;
                    if counter % batch_size == 0 {
                        Self::print_thread_progress(thread_id, counter, total_node_count);
                        if self.params.save_intermediate_results_periodically {
                            info!("Thread {}: Saving intermediate results to binary file", thread_id);
                            local_c_list = self.persist_current_state(thread_id, counter, local_c_list);
                        }
                    }
                }

                info!("Thread {}: finished", thread_id);
                local_c_list
            })
            .collect_into_vec(&mut partial_results);

        let result_count = partial_results.len() as u64;
        let mut progress_bar = ProgressBar::new(result_count);
        let mut global_c_list = SparseList::new(0.0);
        for result in partial_results {
            for (&node, &value) in result.iter() {
                global_c_list[node] += value;
            }
            progress_bar.inc();
        }
        progress_bar.set(result_count);

        global_c_list
    }

    fn print_thread_progress(index: u32, counter: u32, total_node_count: usize) {
        let thread_info = format!("Thread {}: {} / {}", index, counter, total_node_count);
        info!("{}", thread_info);
    }

    fn restore_or_create_state(&self, index: u32) -> (SparseList<f64>, u32) {
        let file = self.get_state_path_for_index(index);
        if !file.exists() {
            (SparseList::new(0.0), 0)
        } else {
            let state: BrandesThreadState = Self::read_from_file(&file).unwrap_or(
                Self::get_fresh_thread_state(index)
            );

            (state.local_c_list, state.counter)
        }
    }

    fn persist_current_state(&self, index: u32, counter: u32, local_c_list: SparseList<f64>) -> SparseList<f64> {
        let file = self.get_state_path_for_index(index);

        let state = BrandesThreadState {
            index, counter, local_c_list
        };

        write_binary_to_file(&file, &state);

        state.local_c_list
    }

    fn get_state_path_for_index(&self, index: u32) -> PathBuf {
        let directory = self.intermediate_folder_path.join("betweenness");
        fs::create_dir_all(&directory).expect("Could not create intermediary directory for betweenness");

        directory.join(format!("thread_{}.bin", index))
    }

    fn read_from_file(path: &PathBuf) -> Option<BrandesThreadState> {
        let f = File::open(path);
        if f.is_ok() {
            let file = f.unwrap();

            let data = bincode::deserialize_from(file);

            if data.is_ok() {
                Some(data.unwrap())
            } else {
                info!(
                    "File at {} does not contain or contains invalid thread state data",
                    path.to_str().unwrap()
                );
                None
            }
        } else {
            None
        }
    }

    fn get_fresh_thread_state(index: u32) -> BrandesThreadState {
        BrandesThreadState {
            index,
            counter: 0,
            local_c_list: SparseList::new(0.0),
        }
    }

    pub fn calculate_delta_for_node(
        &self,
        neighbors: &SparseList<HashSet<i64>>,
        c_list: &mut SparseList<f64>,
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
        neighbors: &SparseList<HashSet<i64>>,
        s_stack: &mut Vec<i64>,
        p_list: &mut SparseList<Vec<i64>>,
        sigma: &mut SparseList<u64>,
        mut d: SparseList<i64>,
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
        c_list: &mut SparseList<f64>,
        s_stack: &mut Vec<i64>,
        p_list: &mut SparseList<Vec<i64>>,
        sigma: &mut SparseList<u64>,
        delta: &mut SparseList<f64>
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
