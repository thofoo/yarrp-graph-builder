use std::fs;
use std::fs::{DirEntry, File};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::MetadataExt;
use csv::Writer;
use hashbrown::HashMap;
use log::{debug, info};
use warts::{Address, Object};
use crate::common::structs::parse_data::MaxNodeIds;
use crate::{DatasetConfig, IpType, OutputPaths};
use crate::preprocess::file_util;
use crate::preprocess::parser::{ipv4_to_numeric, ipv6_to_numeric};

pub struct WartsDataPreprocessor {
    config: DatasetConfig,
    output_paths: OutputPaths,
}

impl WartsDataPreprocessor {
    pub fn new(config: &DatasetConfig, output_paths: &OutputPaths) -> WartsDataPreprocessor {
        WartsDataPreprocessor {
            config: config.clone(),
            output_paths: output_paths.clone(),
        }
    }

    /**
     * Assigns IDs to the IP nodes and generates an edge list out of the paths.
     * The paths are then generated as an edge list CSV (from,to).
     * Additionally, the IP-to-NodeID mapping is also stored to a CSV (IP,ID).
     * (-> see also merger.rs)
     *
     *  Requires: .warts.gz input files at dataset.warts.input_path
     * Generates:
     *     - edges.csv (edge list)
     *     - mapping.csv (IP to ID mapping)
     *     - max_node_ids.csv (maximum IDs assigned, both known and unknown)
     * The edges and mapping are in no particular order.
     */
    pub fn preprocess_files(&self) {
        info!("Step: Preprocessing WARTS files.");
        info!("Expecting to work with IP{:?} addresses.", self.config.address_type);

        let input_path = &self.config.input_path;

        let files = fs::read_dir(&input_path).unwrap();

        let empty_file_size_bytes = 100;
        let files_to_process: Vec<DirEntry> = files
            .map(|entry| entry.unwrap())
            .filter(|i| i.path().is_file())
            .filter(|i| i.path().to_str().unwrap().trim().ends_with(".warts.gz"))
            .filter(|i| i.metadata().unwrap().size() > empty_file_size_bytes)
            .collect();

        let mapping_file_name = &self.output_paths.mapping;
        let edges_file_name = &self.output_paths.edges;
        let max_node_file_name = &self.output_paths.max_node_ids;

        let index_writer = csv::Writer::from_path(mapping_file_name)
            .expect(&format!(
                "Could not create file for storing node mapping at {}", mapping_file_name.to_str().unwrap()
            ));
        let mut edge_writer = csv::Writer::from_path(edges_file_name)
            .expect(&format!(
                "Could not create file for storing edges at {}", edges_file_name.to_str().unwrap()
            ));
        let mut max_node_ids_writer = csv::Writer::from_path(max_node_file_name)
            .expect(&format!(
                "Could not create file for storing max node ids at {}", max_node_file_name.to_str().unwrap()
            ));

        let mut index: HashMap<u128, i64> = HashMap::new();
        let mut counter: i64 = 0;
        let mut missing_node_counter: i64 = -1;
        let mut missing_node_memory: HashMap<i64, i64> = HashMap::new();

        let mut file_processed_counter = 1;
        edge_writer.serialize(("from", "to")).unwrap();
        for file in &files_to_process {
            info!("Processing {} / {} files", file_processed_counter, files_to_process.len());
            self.process_single_file(
                &file,
                &mut index,
                &mut counter,
                &mut missing_node_counter,
                &mut missing_node_memory,
                &mut edge_writer
            );
            file_processed_counter += 1;
        }

        info!("Writing node mapping to disk...");
        self.write_node_mapping_to_disk(index_writer, index);
        Self::write_max_node_ids_to_disk(&mut max_node_ids_writer, counter, missing_node_counter);
    }

    /**
     * Processes all the paths in one file and directly writes the edges to the output file.
     * For missing hops, a negative ID is assigned. The ID is pinned to the starting point -
     * for any edge A-B with a known A and an unknown B, the same negative ID is used for B.
     */
    fn process_single_file(
        &self,
        file: &DirEntry,
        index: &mut HashMap<u128, i64>,
        counter: &mut i64,
        missing_node_counter: &mut i64,
        missing_node_memory: &mut HashMap<i64, i64>,
        edge_writer: &mut Writer<File>,
    ) {
        let objects = file_util::read_warts_from_gzip(file.path());
        for object in objects {
            match object {
                Object::Traceroute(t) => {
                    let src_addr = uint_from_raw_address(t.src_addr.unwrap());

                    let src_id = get_or_put(index, src_addr, counter);

                    let mut previous_node = src_id;
                    let mut previous_hop = 0;

                    for hop in t.hops {
                        let current_hop = hop.probe_ttl.unwrap();

                        let hop_addr_object = hop.addr.unwrap();
                        match hop_addr_object {
                            Address::IPv4(_, _) => { /* we can proceed */ }
                            Address::IPv6(_, _) => { /* we can proceed */ }
                            Address::Reference(reference) => {
                                debug!("Got REFERENCE for traceroute addr at TTL {}: {}", current_hop, reference);
                                continue;
                            }
                            Address::Ethernet(e1, e2) => {
                                debug!("Got ETHERNET for traceroute addr at TTL {}: {} {:?}", current_hop, e1, e2);
                                continue;
                            }
                            Address::FireWire(f1, f2) => {
                                debug!("Got FIREWIRE for traceroute addr at TTL {}: {} {:?}", current_hop, f1, f2);
                                continue;
                            }
                        }
                        let addr = uint_from_raw_address(hop_addr_object);
                        let addr_id = get_or_put(index, addr, counter);

                        if current_hop > previous_hop + 1 {
                            let missing_hops = (current_hop - 1) - (previous_hop + 1);
                            for _ in 0..missing_hops {
                                if !missing_node_memory.contains_key(&previous_node) {
                                    missing_node_memory.insert(previous_node, *missing_node_counter);
                                    *missing_node_counter -= 1;
                                }

                                let new_node_id = *missing_node_memory.get(&previous_node).unwrap();

                                edge_writer.serialize((previous_node, new_node_id)).unwrap();
                                previous_node = i64::try_from(new_node_id).unwrap();
                                previous_hop += 1;
                            }
                        }

                        edge_writer.serialize((previous_node, addr_id)).unwrap();
                        previous_node = i64::try_from(addr_id).unwrap();
                        previous_hop = current_hop;
                    }
                }
                _ => debug!("Encountered non-traceroute entry: {:?}", object)
            }
        }
    }

    /**
     * Writes the node mapping to a CSV file.
     */
    fn write_node_mapping_to_disk(&self, mut index_writer: Writer<File>, index: HashMap<u128, i64>) {
        index_writer.serialize(("ip", "node_id")).unwrap();
        index.iter()
            .map(|(&ip, &node_id)| {
                let ip_addr = if self.config.address_type == IpType::V4 {
                    IpAddr::V4(Ipv4Addr::from(u32::try_from(ip).unwrap()))
                } else {
                    IpAddr::V6(Ipv6Addr::from(ip))
                };
                (ip_addr, node_id)
            })
            .for_each(|row| index_writer.serialize(row).unwrap());

        index_writer.flush().unwrap();
    }

    /**
     * Writes the max node IDs that were assigned to a separate file.
     */
    fn write_max_node_ids_to_disk(max_node_ids_writer: &mut Writer<File>, counter: i64, missing_node_counter: i64) {
        let max_node_ids = MaxNodeIds {
            known: (counter - 1) as usize,
            unknown: -(missing_node_counter + 1) as usize,
        };
        max_node_ids_writer.serialize(max_node_ids).unwrap();
        max_node_ids_writer.flush().unwrap();
    }
}

fn get_or_put(index: &mut HashMap<u128, i64>, addr: u128, counter: &mut i64) -> i64 {
    if index.contains_key(&addr) {
        *index.get(&addr).unwrap()
    } else {
        let new_value = *counter;
        (*counter) += 1;
        index.insert(addr, new_value);
        new_value
    }
}

fn uint_from_raw_address(address: Address) -> u128 {
    let ip = IpAddr::from(address);
    match ip {
        IpAddr::V4(target) => {
            u128::from(ipv4_to_numeric(target))
        }
        IpAddr::V6(target) => {
            ipv6_to_numeric(target)
        }
    }
}