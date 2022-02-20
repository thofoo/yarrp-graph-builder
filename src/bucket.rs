pub mod bucket {
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::PathBuf;

    use serde::{Deserialize, Serialize};

    use crate::structs::yarrp_row::InternalNode;
    use crate::util;

    #[derive(Serialize)]
    #[derive(Deserialize)]
    pub struct GraphBucket {
        edge_map: HashMap<u32, Vec<(u32, u8)>>,
        file_path: PathBuf,
        is_in_memory: bool,
    }

    impl GraphBucket {
        pub fn new(file_path: PathBuf) -> GraphBucket {
            GraphBucket {
                edge_map: GraphBucket::load_or_create(&file_path),
                is_in_memory: true,
                file_path,
            }
        }

        fn load_or_create(file_path: &PathBuf) -> HashMap<u32, Vec<(u32, u8)>> {
            let f = File::open(file_path);
            if f.is_ok() {
                let file = f.unwrap();
                bincode::deserialize_from(file).expect(
                    &format!(
                        "File at {} does not contain or contains invalid graph bucket data",
                        file_path.to_str().unwrap()
                    )
                )
            } else {
                HashMap::new()
            }
        }

        pub fn add_node(&mut self, node: InternalNode) {
            self.ensure_loaded();

            if !self.edge_map.contains_key(&node.target_id) {
                let new_list = Vec::<(u32, u8)>::new();
                self.edge_map.insert(node.target_id, new_list);
            }

            let list = self.edge_map.get_mut(&node.target_id).unwrap();
            list.push((node.hop_id, node.hop_count));
        }

        fn ensure_loaded(&mut self) {
            if !self.is_in_memory {
                self.edge_map = GraphBucket::load_or_create(&self.file_path);
                self.is_in_memory = true;
            }
        }

        pub fn evict_to_disk(&mut self) {
            let path = &self.file_path;
            util::util::write_to_file(path, &self.edge_map);
            self.edge_map.clear();
            self.is_in_memory = false;
        }

        pub fn edge_map(self) -> HashMap<u32, Vec<(u32, u8)>> {
            self.edge_map
        }
    }
}