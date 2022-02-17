pub mod bucket {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufWriter;
    use std::path::PathBuf;

    use serde::{Deserialize, Serialize};

    use crate::structs::yarrp_row::{NodeV4, NodeV6};

    #[derive(Serialize)]
    #[derive(Deserialize)]
    pub struct GraphBucket {
        node_index: HashMap<u128, u32>,
        edge_map: HashMap<u32, Vec<(u32, u8)>>,
        counter: u32,
        file_path: PathBuf,
        is_in_memory: bool,
    }

    impl GraphBucket {
        pub fn new(file_path: PathBuf) -> GraphBucket {
            // TODO store bucket ID
            // TODO load from disk if exists
            GraphBucket::load_from_disk(file_path)
        }

        fn load_from_disk(file_path: PathBuf) -> GraphBucket {
            let f = File::open(&file_path);
            if f.is_ok() {
                let file = f.unwrap();
                bincode::deserialize_from(file).expect(
                    &format!(
                        "File at {} does not contain or contains invalid graph bucket data",
                        file_path.to_str().unwrap()
                    )
                )
            } else {
                GraphBucket {
                    node_index: HashMap::new(),
                    edge_map: HashMap::new(),
                    counter: 0,
                    file_path,
                    is_in_memory: true,
                }
            }
        }

        fn ensure_loaded(&mut self) {
            if !self.is_in_memory {
                let file_path = self.file_path.to_path_buf();
                let bucket = GraphBucket::load_from_disk(file_path);
                self.node_index = bucket.node_index;
                self.edge_map = bucket.edge_map;
                self.counter = bucket.counter;
                self.file_path = bucket.file_path;
                self.is_in_memory = true;
            }
        }

        pub fn add_node_v4(&mut self, node: NodeV4) {
            self.ensure_loaded();

            self.add_node_v6(NodeV6 {
                target_ip: u128::from(node.target_ip),
                hop_ip: u128::from(node.hop_ip),
                hop_count: node.hop_count,
            });
        }

        pub fn add_node_v6(&mut self, node: NodeV6) {
            self.ensure_loaded();

            let target_node_id = if self.node_index.contains_key(&node.target_ip) {
                *self.node_index.get(&node.target_ip).unwrap()
            } else {
                let new_node_id = self.counter;
                self.counter += 1;
                self.node_index.insert(node.target_ip, new_node_id);
                new_node_id
            };

            let hop_node_id = if self.node_index.contains_key(&node.hop_ip) {
                *self.node_index.get(&node.hop_ip).unwrap()
            } else {
                let new_node_id = self.counter;
                self.counter += 1;
                self.node_index.insert(node.hop_ip, new_node_id);
                new_node_id
            };

            if !self.edge_map.contains_key(&target_node_id) {
                let new_list = Vec::<(u32, u8)>::new();
                self.edge_map.insert(target_node_id, new_list);
            }

            let list = self.edge_map.get_mut(&target_node_id).unwrap();
            list.push((hop_node_id, node.hop_count));
        }

        pub fn evict_to_disk(&mut self) {
            let path = &self.file_path;
            let file = File::create(path).expect("Error while creating file to write");
            let writer = BufWriter::new(file);
            bincode::serialize_into(writer, self).expect("Error while serializing bucket");
            self.is_in_memory = false;
        }

        pub fn is_in_memory(&self) -> bool {
            self.is_in_memory
        }
    }
}