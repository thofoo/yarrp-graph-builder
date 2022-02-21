pub mod merger {
    use std::collections::HashMap;
    use std::fs;
    use std::fs::{DirEntry, File};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::path::{Path, PathBuf};
    use csv::Writer;

    use log::info;
    use pbr::ProgressBar;

    use crate::{GraphBuilderParameters, IpType, parameters};
    use crate::bucket::bucket::GraphBucket;

    pub struct Merger {
        config: GraphBuilderParameters,
    }

    impl Merger {
        pub fn new(config: &GraphBuilderParameters) -> Merger {
            Merger { config: config.clone() }
        }

        pub fn merge_data(self) {
            if !self.config.should_merge() {
                info!("Merging flag is FALSE - skipping merging.");
                return;
            }

            info!("Beginning with the merging of the intermediate results.");
            info!("Creating empty output files...");

            let index_path = self.config.intermediary_file_path().join(
                Path::new(parameters::parameters::NODE_INDEX_PATH)
            );

            let node_mapping_output_path = self.config.output_path().join(
                Path::new("mapping.csv")
            );
            let edge_output_path = self.config.output_path().join(
                Path::new("edges.csv")
            );

            let mut index_writer = csv::Writer::from_path(&node_mapping_output_path)
                .expect(&format!(
                    "Could not create file for storing node mapping at {}", node_mapping_output_path.to_str().unwrap()
                ));
            let mut edge_writer = csv::Writer::from_path(&edge_output_path)
                .expect(&format!(
                    "Could not create file for storing edges at {}", edge_output_path.to_str().unwrap()
                ));

            let raw_files_list = fs::read_dir(&self.config.intermediary_file_path()).unwrap();
            let dirs_to_process: Vec<DirEntry> = raw_files_list
                .map(|entry| entry.unwrap())
                .filter(|i| i.path().is_dir())
                .collect();

            info!("Reading in intermediate files...");

            self.write_node_mapping(index_path, &mut index_writer);
            self.write_edge_mapping(dirs_to_process, &mut edge_writer);
        }

        fn write_node_mapping(&self, index_path: PathBuf, index_writer: &mut Writer<File>) {
            if !self.config.should_persist_index() {
                info!("Index persistence flag is FALSE - skipping index persistence.");
                return;
            }

            let index_file = File::open(&index_path).expect(&format!(
                "File at {} does not exist", index_path.to_str().unwrap()
            ));
            let index: HashMap<u128, u32> = bincode::deserialize_from(index_file).expect(&format!(
                "File at {} does not contain or contains invalid node index data",
                index_path.to_str().unwrap()
            ));

            info!("Writing node mapping to disk...");

            index_writer.serialize(("ip", "node_id")).unwrap();

            index.iter()
                .map(|(&ip, &node_id)| {
                    let ip_addr = if self.config.address_type() == &IpType::V4 {
                        IpAddr::V4(Ipv4Addr::from(u32::try_from(ip & 0xffffffff).unwrap()))
                    } else {
                        IpAddr::V6(Ipv6Addr::from(ip))
                    };
                    (ip_addr, node_id)
                })
                .for_each(|row| index_writer.serialize(row).unwrap());

            index_writer.flush().unwrap();
        }

        fn write_edge_mapping(&self, dirs_to_process: Vec<DirEntry>, edge_writer: &mut Writer<File>) {
            if !self.config.should_persist_edges() {
                info!("Edge persistence flag is FALSE - skipping edge persistence.");
                return;
            }

            let bucket_count = 256;

            info!(
                "Processing {} intermediary directories with {} buckets each to the final format...",
                dirs_to_process.len(),
                bucket_count
            );

            let mut progress_bar = ProgressBar::new(bucket_count);
            progress_bar.set(0);

            edge_writer.serialize(("from", "to")).unwrap();
            for bucket_id in 0..bucket_count {
                let bucket_name = &format!("yarrp.{}.bin", bucket_id);

                let files_to_process = dirs_to_process.iter()
                    .map(|dir| dir.path().join(Path::new(bucket_name)))
                    .collect();

                self.process_single(files_to_process, edge_writer);
                progress_bar.inc();
            }
        }

        fn process_single(&self, files_to_process: Vec<PathBuf>, edge_writer: &mut Writer<File>) {
            let mut missing_node_counter = -1;
            let merged_edge_map = self.merge_edge_maps(files_to_process);

            for (_, mut edges) in merged_edge_map {
                edges.sort_by_key(|&i| i.1);

                let mut previous_node: i64 = 0; // 0 == source IP
                let mut previous_hop = 0;
                for (current_node, current_hop) in edges {
                    if current_hop > previous_hop + 1 {
                        let missing_hops = (current_hop - 1) - (previous_hop + 1);
                        for _ in 1..=missing_hops {
                            edge_writer.serialize((previous_node, missing_node_counter)).unwrap();
                            previous_node = i64::try_from(missing_node_counter).unwrap();
                            previous_hop += 1;

                            missing_node_counter -= 1;
                        }
                    }

                    edge_writer.serialize((previous_node, current_node)).unwrap();
                    previous_node = i64::try_from(current_node).unwrap();
                    previous_hop = current_hop;
                }
            }

            edge_writer.flush().unwrap();
        }

        fn merge_edge_maps(&self, files_to_process: Vec<PathBuf>) -> HashMap<u32, Vec<(u32, u8)>> {
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
    }
}