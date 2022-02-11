extern crate core;

use std::collections::HashMap;
use std::hash::Hash;
use std::io::BufWriter;
use std::process::exit;
use env_logger::Env;

use log::{error, info, LevelFilter};
use serde::Serialize;
use crate::structs::util::IpType;

use crate::structs::yarrp_row::{Row, RowIpv4, RowIpv6};

mod processor_util;
mod structs;

fn main() {
    let mut env_builder = env_logger::builder();
    let env = Env::new().filter("YARRP_LOG");

    env_builder.filter_level(LevelFilter::Info);
    env_builder.parse_env(env);
    env_builder.init();

    info!("Let's go!");
    let address_type = IpType::V6; // TODO get from cmd line args
    info!("Expecting to read IP{:?} addresses.", address_type);

    info!("Reading in input file...");
    let raw_rows = read_all("../01_yarrp_scan/input/ipv6_0001.yarrp"); // TODO get from cmd line args
    info!("Parsing row data...");
    let entries: Vec<Row> = raw_rows.iter()
        .map(|row| processor_util::parser::parse_data_from_row(row, &address_type))
        .filter_map(|row| row)
        .collect();

    info!("Processing parsed entries...");
    match address_type {
        IpType::V4 => process_entries_as_ipv4(entries),
        IpType::V6 => process_entries_as_ipv6(entries),
    }
}

fn process_entries_as_ipv4(entries: Vec<Row>) {
    let mut edge_map = HashMap::<u32, Vec<(u32, u8)>>::new();
    for entry in entries {
        match entry {
            Row::V4(row) => process_ipv4_entry(row, &mut edge_map),
            Row::V6(_) => {
                error!("Received an IPv6 address in the IPV4 row processing stage, this should be impossible");
                exit(1);
            },
        }
    }
    write_to_file(&edge_map);
}

fn process_entries_as_ipv6(entries: Vec<Row>) {
    let mut edge_map = HashMap::<u128, Vec<(u128, u8)>>::new();
    for entry in entries {
        match entry {
            Row::V4(_) => {
                error!("Received an IPv4 address in the IPV6 row processing stage, this should be impossible");
                exit(1);
            },
            Row::V6(row) => process_ipv6_entry(row, &mut edge_map),
        }
    }
    write_to_file(&edge_map);
}

fn process_ipv4_entry(row: RowIpv4, edge_map: &mut HashMap<u32, Vec<(u32, u8)>>) {
    if !edge_map.contains_key(&row.target_ip) {
        let new_list = Vec::<(u32, u8)>::new();
        edge_map.insert(row.target_ip, new_list);
    }
    let list = edge_map.get_mut(&row.target_ip).unwrap();
    list.push((row.hop_ip, row.hop_count));
}

fn process_ipv6_entry(row: RowIpv6, edge_map: &mut HashMap<u128, Vec<(u128, u8)>>) {
    if !edge_map.contains_key(&row.target_ip) {
        let new_list = Vec::<(u128, u8)>::new();
        edge_map.insert(row.target_ip, new_list);
    }
    let list = edge_map.get_mut(&row.target_ip).unwrap();
    list.push((row.hop_ip, row.hop_count));
}

fn write_to_file<T1: Hash + Eq + Serialize, T2: Serialize>(edge_map: &HashMap<T1, Vec<(T1, T2)>>) {
    info!("Writing result to disk...");
    let file = std::fs::File::create("../01_yarrp_scan/output/ipv6_0001.yarrp.rust")
        .expect("Error while creating file to write...feels bad man");
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, edge_map).expect("should have worked");
    info!("Result written to disk.");
}

fn read_all(file_name: &str) -> Vec<String> {
    std::fs::read_to_string(file_name)
        .expect("file not found!")
        .lines()
        .filter(|&s| !s.starts_with("#"))
        .map(str::to_string)
        .collect()
}