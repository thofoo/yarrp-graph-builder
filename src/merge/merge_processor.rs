use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use csv::Writer;
use crate::buckets::bucket::GraphBucket;

pub struct MergeProcessor<'a> {
    edge_writer: &'a mut Writer<File>,
    missing_node_counter: i64,
    missing_node_memory: HashMap<i64, i64>,
}

impl<'a> MergeProcessor<'a> {
    pub fn new(edge_writer: &'a mut Writer<File>) -> MergeProcessor {
        MergeProcessor {
            edge_writer,
            missing_node_counter: -1,
            missing_node_memory: HashMap::new(),
        }
    }

    pub fn process_bucket(&mut self, files_to_process: Vec<PathBuf>) -> i64 {
        let merged_edge_map = self.merge_edge_maps(files_to_process);

        for (_, mut edges) in merged_edge_map {
            edges.sort_by_key(|&i| i.1);

            let mut previous_node: i64 = 0; // 0 == source IP
            let mut previous_hop = 0;
            for (current_node, current_hop) in edges {
                if current_hop > previous_hop + 1 {
                    let missing_hops = (current_hop - 1) - (previous_hop + 1);
                    for _ in 0..missing_hops {
                        if !self.missing_node_memory.contains_key(&previous_node) {
                            self.missing_node_memory.insert(previous_node, self.missing_node_counter);
                            self.missing_node_counter -= 1;
                        }

                        let new_node_id = *self.missing_node_memory.get(&previous_node).unwrap();

                        self.edge_writer.serialize((previous_node, new_node_id)).unwrap();
                        previous_node = i64::try_from(new_node_id).unwrap();
                        previous_hop += 1;
                    }
                }

                self.edge_writer.serialize((previous_node, current_node)).unwrap();
                previous_node = i64::try_from(current_node).unwrap();
                previous_hop = current_hop;
            }
        }

        self.edge_writer.flush().unwrap();
        self.missing_node_counter
    }

    fn merge_edge_maps(&self, files_to_process: Vec<PathBuf>) -> HashMap<u64, Vec<(u64, u8)>> {
        let mut edge_map = HashMap::new();

        for file in files_to_process {
            let partial_map = GraphBucket::new(file).edge_map();

            for (key, value) in partial_map {
                if !edge_map.contains_key(&key) {
                    edge_map.insert(key, Vec::new());
                }

                let list = edge_map.get_mut(&key).unwrap();
                list.extend(value);
            }
        }

        edge_map
    }

    pub fn max_unknown_node(&self) -> usize {
        // +1 because we decrement *after* every node assignment
        (self.missing_node_counter + 1).unsigned_abs() as usize
    }
}