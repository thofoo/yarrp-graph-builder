extern crate core;

use std::collections::HashMap;
use std::io::BufWriter;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

#[derive(Debug)]
enum YarrpRow {
    V4(YarrpRowIpv4),
    V6(YarrpRowIpv6),
    UNKNOWN(String)
}

#[derive(Debug)]
struct YarrpRowIpv4 {
    target_ip: u32,
    hop_ip: u32,
    hop_count: u8,
}

#[derive(Debug)]
struct YarrpRowIpv6 {
    target_ip: u128,
    hop_ip: u128,
    hop_count: u8,
}

fn main() {
    println!("Hello, world!");
    let raw_rows = read_all("../01_yarrp_scan/ipv4_0000.yarrp");
    let nodes: Vec<YarrpRow> = raw_rows.iter()
        .map(self::parse_data_from_row)
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

fn parse_data_from_row(row: &String) -> YarrpRow {
    // Why not split? It was 3 seconds slower on a 410 MB IPv4 test run
    let ascii_row = row.as_bytes();
    let last_index = ascii_row.len() - 1;
    let mut target_ip_split = 0;
    for i in 0..last_index {
        if ascii_row[i] == 0x20 {
            target_ip_split = i;
            break;
        }
    }

    let mut spaces_to_skip = 3;
    let mut hop_count_split_start = 0;
    let mut hop_count_split_end = 0;
    let mut hop_ip_split_end = 0;
    for i in (target_ip_split + 1)..last_index {
        if ascii_row[i] == 0x20 {
            if spaces_to_skip == 0 {
                if hop_count_split_start == 0 {
                    hop_count_split_start = i + 1;
                } else if hop_count_split_end == 0 {
                    hop_count_split_end = i;
                } else {
                    hop_ip_split_end = i;
                    break;
                }
            } else {
                spaces_to_skip -= 1;
            }
        }
    }

    let row = row.as_str();

    // let mut split_row = row.split(" ").take(7);
    let raw_target_ip = &row[0..target_ip_split]; //split_row.next().expect("error");

    // let mut new_split_row = split_row.skip(4).take(2);
    let raw_hop_count = &row[hop_count_split_start..hop_count_split_end];//new_split_row.next().expect("error");
    let raw_hop_ip = &row[hop_count_split_end+1..hop_ip_split_end];//new_split_row.next().expect("error");

    let hop_count = u8::from_str(raw_hop_count).expect(&construct_error(row));
    if row.contains(".") {
        YarrpRow::V4(YarrpRowIpv4 {
            target_ip: ipv4_str_to_numeric(raw_target_ip),
            hop_ip: ipv4_str_to_numeric(raw_hop_ip),
            hop_count,
        })
    } else if row.contains(":") {
        YarrpRow::V6(YarrpRowIpv6 {
            target_ip: ipv6_str_to_numeric(raw_target_ip),
            hop_ip: ipv6_str_to_numeric(raw_hop_ip),
            hop_count,
        })
    } else {
        YarrpRow::UNKNOWN(row.to_owned())
    }
}

fn construct_error(row: &str) -> String {
    format!("Could not parse data for row: {}", row)
}

fn ipv4_str_to_numeric(ip_str: &str) -> u32 {
    let parsed_ip: Ipv4Addr = ip_str.parse().unwrap();
    let mut shift = 4;
    return parsed_ip.octets()
        .iter()
        .fold(0, |ip, e| {
            shift -= 1;
            ip | u32::from(e << shift)
        });
}

fn ipv6_str_to_numeric(ip_str: &str) -> u128 {
    let parsed_ip: Ipv6Addr = ip_str.parse().unwrap();
    let mut shift = 16;
    return parsed_ip.octets()
        .iter()
        .fold(0, |ip, &e| {
            shift -= 1;
            ip | (u128::from(e) << shift)
        });
}

fn read_all(file_name: &str) -> Vec<String> {
    std::fs::read_to_string(file_name)
        .expect("file not found!")
        .lines()
        .filter(|&s| !s.starts_with("#"))
        .map(str::to_string)
        .collect()
}