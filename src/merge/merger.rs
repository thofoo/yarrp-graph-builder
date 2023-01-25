use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use csv::Writer;

use log::info;
use pbr::ProgressBar;

use crate::{DatasetConfig, IpType, OutputPaths};
use crate::common::parameters;
use crate::common::structs::data::MaxNodeIds;
use crate::merge::merge_processor::MergeProcessor;

pub struct Merger {
    config: DatasetConfig,
    output_paths: OutputPaths,
}

impl Merger {
    pub fn new(config: &DatasetConfig, output_paths: &OutputPaths) -> Merger {
        Merger {
            config: config.clone(),
            output_paths: output_paths.clone(),
        }
    }

    pub fn merge_data(self) {
        info!("Expecting to work with IP{:?} addresses.", self.config.address_type);

        info!("Beginning with the merging of the intermediate results.");
        info!("Creating empty output files...");

        let index_path = self.config.intermediate_path.join(
            Path::new(parameters::NODE_INDEX_PATH)
        );

        let node_mapping_output_path = self.output_paths.mapping();
        let edge_output_path = self.output_paths.edges();
        let max_node_id_path = self.output_paths.max_node_ids();

        let mut index_writer = csv::Writer::from_path(&node_mapping_output_path)
            .expect(&format!(
                "Could not create file for storing node mapping at {}", node_mapping_output_path.to_str().unwrap()
            ));
        let mut edge_writer = csv::Writer::from_path(&self.output_paths.edges())
            .expect(&format!(
                "Could not create file for storing edges at {}", edge_output_path.to_str().unwrap()
            ));

        let raw_files_list = fs::read_dir(&self.config.intermediate_path).unwrap();
        let dirs_to_process: Vec<DirEntry> = raw_files_list
            .map(|entry| entry.unwrap())
            .filter(|i| i.path().is_dir())
            .collect();

        info!("Reading in intermediate files...");

        let max_known_node_id = self.write_node_mapping(index_path, &mut index_writer);
        let max_unknown_node_id = self.write_edge_mapping(dirs_to_process, &mut edge_writer);

        let mut max_node_ids_writer = csv::Writer::from_path(&self.output_paths.max_node_ids())
            .expect(&format!(
                "Could not create file for storing max node ids at {}", max_node_id_path.to_str().unwrap()
            ));

        let max_node_ids = MaxNodeIds {
            known: max_known_node_id,
            unknown: max_unknown_node_id,
        };
        max_node_ids_writer.serialize(max_node_ids).unwrap()
    }

    fn write_node_mapping(&self, index_path: PathBuf, index_writer: &mut Writer<File>) -> usize {
        let index_file = File::open(&index_path).expect(&format!(
            "File at {} does not exist", index_path.to_str().unwrap()
        ));
        let index: HashMap<u128, u64> = bincode::deserialize_from(index_file).expect(&format!(
            "File at {} does not contain or contains invalid node index data",
            index_path.to_str().unwrap()
        ));

        info!("Writing node mapping to disk...");

        index_writer.serialize(("ip", "node_id")).unwrap();

        let mut max_node_id: u64 = 0;
        index.iter()
            .map(|(&ip, &node_id)| {
                let ip_addr = if self.config.address_type == IpType::V4 {
                    IpAddr::V4(Ipv4Addr::from(u32::try_from(ip).unwrap()))
                } else {
                    IpAddr::V6(Ipv6Addr::from(ip))
                };
                if node_id > max_node_id {
                    max_node_id = node_id;
                }
                (ip_addr, node_id)
            })
            .for_each(|row| index_writer.serialize(row).unwrap());

        index_writer.flush().unwrap();
        max_node_id as usize
    }

    fn write_edge_mapping(&self, dirs_to_process: Vec<DirEntry>, edge_writer: &mut Writer<File>) -> usize {
        let bucket_count = 256;

        info!(
            "Processing {} intermediary directories with {} buckets each to the final format...",
            dirs_to_process.len(),
            bucket_count
        );

        let mut progress_bar = ProgressBar::new(bucket_count);
        progress_bar.set(0);

        edge_writer.serialize(("from", "to")).unwrap();

        let mut merge_processor = MergeProcessor::new(edge_writer);
        for bucket_id in 0..bucket_count {
            let bucket_name = &format!("yarrp.{}.bin", bucket_id);

            let files_to_process = dirs_to_process.iter()
                .map(|dir| dir.path().join(Path::new(bucket_name)))
                .collect();

            merge_processor.process_bucket(files_to_process);
            progress_bar.inc();
        }
        merge_processor.max_unknown_node()
    }
}