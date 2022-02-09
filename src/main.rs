mod processor_util;
mod yarrp_row;

extern crate core;

use std::collections::HashMap;
use std::io::BufWriter;
use crate::yarrp_row::yarrp_row::YarrpRow;

fn main() {
    let raw_rows = read_all("../01_yarrp_scan/ipv4_0000.yarrp");
    let nodes: Vec<YarrpRow> = raw_rows.iter()
        .map(processor_util::processor_util::parse_data_from_row)
        .filter_map(|row| row)
        .collect();

    // yes i know this is extremely bad. i just want to see how well it works
    let mut edge1_map = HashMap::<u32, Vec<(u32, u8)>>::new();
    let mut edge2_map = HashMap::<u128, Vec<(u128, u8)>>::new();
    for node in nodes {
        match node {
            YarrpRow::V4(row) => {
                if !edge1_map.contains_key(&row.target_ip) {
                    let new_list = Vec::<(u32, u8)>::new();
                    edge1_map.insert(row.target_ip, new_list);
                }
                let list = edge1_map.get_mut(&row.target_ip).unwrap();
                list.push((row.hop_ip, row.hop_count));
            }
            YarrpRow::V6(row) => {
                if !edge2_map.contains_key(&row.target_ip) {
                    let new_list = Vec::<(u128, u8)>::new();
                    edge2_map.insert(row.target_ip, new_list);
                }
                let list = edge2_map.get_mut(&row.target_ip).unwrap();
                list.push((row.hop_ip, row.hop_count));
            }
            _ => {
                panic!("?!")
            }
        }
    }

    let file = std::fs::File::create("../01_yarrp_scan/ipv4_0000.yarrp.rust")
       .expect("Error while creating file to write...feels bad man");
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, &edge1_map).expect("should have worked");
}

fn read_all(file_name: &str) -> Vec<String> {
    std::fs::read_to_string(file_name)
        .expect("file not found!")
        .lines()
        .filter(|&s| !s.starts_with("#"))
        .map(str::to_string)
        .collect()
}