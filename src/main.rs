extern crate core;

use std::collections::HashMap;
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

    println!("{:#?}", nodes);
}

fn parse_data_from_row(row: &String) -> YarrpRow {
    let split_row: Vec<&str> = row.split(" ").collect();
    let raw_target_ip = split_row[0];
    let raw_hop_ip = split_row[6];
    let raw_hop_count = split_row[5];

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

fn construct_ip_segment_error(ip_str: &str, ip_segment: &str) -> String {
    format!("Could not parse segment {} in ip {}", ip_segment, ip_str)
}

fn ipv4_str_to_numeric(ip_str: &str) -> u32 {
    let split_ip: Vec<&str> = ip_str.split(".").collect();

    let mut ip: u32 = 0;
    for (i, segment) in split_ip.iter().enumerate() {
        let parsed_segment = u32::from_str(*segment)
            .expect(&construct_ip_segment_error(ip_str, segment));
        ip += parsed_segment << ((3 - i) * 8)
    }
    u32::from(ip)
}

fn ipv6_str_to_numeric(ip_str: &str) -> u128 {
    let mut split_ip: Vec<&str> = ip_str.split(":").collect();
    if split_ip[0] == "" {
        split_ip[0] = "0"
    }
    let last_index = split_ip.len() - 1;
    if split_ip[last_index] == "" {
        split_ip[last_index] = "0"
    }
    if split_ip.contains(&"") {
        let i = split_ip.iter().position(|&s| s == "").unwrap();
        split_ip[i] = "0";
        let missing_zero_segments = 8 - last_index;
        for _ in 1..missing_zero_segments {
            split_ip.insert(i, "0")
        }
    }
    let mut ip = 0;
    for (i, segment) in split_ip.iter().enumerate() {
        let parsed_segment = u128::from_str_radix(*segment, 16)
            .expect(&construct_ip_segment_error(ip_str, segment));
        ip += parsed_segment << ((7 - i) * (8 * 2)) // block 7 - i, 8 bits per byte, 2 bytes
    }
    u128::from(ip)
}

fn read_all(file_name: &str) -> Vec<String> {
    std::fs::read_to_string(file_name)
        .expect("file not found!")
        .lines()
        .filter(|&s| !s.starts_with("#"))
        .map(str::to_string)
        .collect()
}