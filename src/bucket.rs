pub mod bucket {
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Serialize};

    use log::trace;
    use std::io::BufWriter;

    use crate::structs::yarrp_row::{NodeV4, NodeV6};

    #[derive(Serialize)]
    #[derive(Deserialize)]
    pub struct GraphBucket {
        node_index: HashMap<u128, u32>,
        edge_map: HashMap<u32, Vec<(u32, u8)>>,
        counter: u32,
        file_path: PathBuf,
    }

    impl GraphBucket {
        pub fn new() -> GraphBucket {
            // TODO pass intermediary path
            // TODO store bucket ID
            // TODO load from disk if exists
            let mut bucket = GraphBucket {
                node_index: HashMap::new(),
                edge_map: HashMap::new(),
                counter: 0,
                // this is just a placeholder path; it will get overwritten below
                file_path: Path::new("placeholder-path").to_path_buf(),
            };

            bucket.load_from_disk(Path::new("").to_path_buf());
            bucket
        }

        pub fn add_node_v4(&mut self, node: NodeV4) {
            self.add_node_v6(NodeV6 {
                target_ip: u128::from(node.target_ip),
                hop_ip: u128::from(node.hop_ip),
                hop_count: node.hop_count,
            });
        }

        pub fn add_node_v6(&mut self, node: NodeV6) {
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

        pub fn load_from_disk(&mut self, file_path: PathBuf) {
            let f = File::open(&file_path);
            if f.is_ok() {
                let file = f.unwrap();
                let bucket: GraphBucket = bincode::deserialize_from(file).expect(
                    &format!(
                        "File at {} does not contain or contains invalid graph bucket data",
                        &self.file_path.to_str().unwrap()
                    )
                );
                *self = GraphBucket {
                    node_index: bucket.node_index,
                    edge_map: bucket.edge_map,
                    counter: bucket.counter,
                    file_path: bucket.file_path,
                }
            } else {
                *self = GraphBucket {
                    node_index: HashMap::new(),
                    edge_map: HashMap::new(),
                    counter: 0,
                    file_path,
                }
            }
        }

        pub fn evict_to_disk(&self) {
            trace!("Evicting  to disk...");
            let path = &self.file_path;
            let file = File::create(path).expect("Error while creating file to write");
            let writer = BufWriter::new(file);
            bincode::serialize_into(writer, self).expect("Error while serializing bucket");
            trace!("Result written to disk.");
        }
    }
}