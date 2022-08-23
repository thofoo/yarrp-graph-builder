use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use hashbrown::HashMap;
use log::{debug, info};
use warts::{Address, Object};
use crate::common::structs::data::MaxNodeIds;
use crate::IpType;
use crate::preprocess::file_util;
use crate::preprocess::parser::{ipv4_to_numeric, ipv6_to_numeric};

pub struct WartsDataPreprocessor {
    base_dir: PathBuf,
    ip_type: IpType,
}

impl WartsDataPreprocessor {
    pub fn new(base_dir: PathBuf, ip_type: IpType) -> WartsDataPreprocessor {
        WartsDataPreprocessor { base_dir, ip_type }
    }

    pub fn preprocess_files(&self) {
        let input_path = self.base_dir.join("input");

        let files = fs::read_dir(&input_path).unwrap();

        let files_to_process = files
            .map(|entry| entry.unwrap())
            .filter(|i| i.path().is_file())
            .filter(|i| i.path().to_str().unwrap().ends_with(".warts.gz"));

        for file in files_to_process {
            let output_dir = &self.base_dir.join("output")
                .join(file.file_name());
            fs::create_dir_all(output_dir).unwrap();

            let mapping_file_name = &output_dir.join("mapping.csv");
            let edges_file_name = &output_dir.join("edges.csv");
            let max_node_file_name = &output_dir.join("max_node_ids.csv");

            let mut index_writer = csv::Writer::from_path(mapping_file_name)
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

            edge_writer.serialize(("from", "to")).unwrap();

            let objects = file_util::read_warts_from_gzip(file.path());
            for object in objects {
                match object {
                    Object::Traceroute(t) => {
                        let src_addr = uint_from_raw_address(t.src_addr.unwrap());

                        let src_id = get_or_put(&mut index, src_addr, &mut counter);

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
                                    continue
                                }
                                Address::Ethernet(e1, e2) => {
                                    info!("Got ETHERNET for traceroute addr at TTL {}: {} {:?}", current_hop, e1, e2);
                                    continue
                                }
                                Address::FireWire(f1, f2) => {
                                    info!("Got FIREWIRE for traceroute addr at TTL {}: {} {:?}", current_hop, f1, f2);
                                    continue
                                }
                            }
                            let addr = uint_from_raw_address(hop_addr_object);
                            let addr_id = get_or_put(&mut index, addr, &mut counter);

                            if current_hop > previous_hop + 1 {
                                let missing_hops = (current_hop - 1) - (previous_hop + 1);
                                for _ in 0..missing_hops {
                                    if !missing_node_memory.contains_key(&previous_node) {
                                        missing_node_memory.insert(previous_node, missing_node_counter);
                                        missing_node_counter -= 1;
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
                    _ => info!("Encountered non-traceroute entry: {:?}", object)
                }
            }

            info!("Writing node mapping to disk...");
            index_writer.serialize(("ip", "node_id")).unwrap();
            index.iter()
                .map(|(&ip, &node_id)| {
                    let ip_addr = if self.ip_type == IpType::V4 {
                        IpAddr::V4(Ipv4Addr::from(u32::try_from(ip).unwrap()))
                    } else {
                        IpAddr::V6(Ipv6Addr::from(ip))
                    };
                    (ip_addr, node_id)
                })
                .for_each(|row| index_writer.serialize(row).unwrap());

            index_writer.flush().unwrap();

            let max_node_ids = MaxNodeIds {
                known: (counter - 1) as usize,
                unknown: -(missing_node_counter + 1) as usize,
            };
            max_node_ids_writer.serialize(max_node_ids).unwrap();
            max_node_ids_writer.flush().unwrap();
        }
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