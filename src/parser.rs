pub mod parser {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::process::exit;
    use std::str::FromStr;

    use log::{error, warn};

    use crate::bucket_manager::bucket_manager::GraphBucketManager;
    use crate::structs::util::IpType;
    use crate::structs::yarrp_row::{NodeV4, NodeV6};

    pub fn parse_data_from_row(row: &String, memory: &mut GraphBucketManager, expected_ip_type: &IpType) {
        let (raw_target_ip, raw_hop_count, raw_hop_ip) = extract_strings_from_row(row);

        let hop_count = hop_count_str_to_numeric(raw_hop_count);

        let target_ip: IpAddr;
        let hop_ip: IpAddr;

        if let Ok(address) = IpAddr::from_str(raw_target_ip) {
            target_ip = address;
        } else {
            warn!("SKIPPING ROW: Could not parse target IP: {}", raw_target_ip);
            return
        }

        if let Ok(address) = IpAddr::from_str(raw_hop_ip) {
            hop_ip = address;
        } else {
            warn!("SKIPPING ROW: Could not parse hop IP: {}", raw_hop_ip);
            return
        }

        let ips = (target_ip, hop_ip);

        match ips {
            (IpAddr::V4(target), IpAddr::V4(hop)) => {
                if expected_ip_type == &IpType::V6 {
                    error!("Expected IPV6 addresses only but got IPV4 addresses: target ip {} hop ip {}", target, hop);
                    exit(1);
                }

                let _ = &memory.add_node_v4(NodeV4 {
                    target_ip: ipv4_to_numeric(target),
                    hop_ip: ipv4_to_numeric(hop),
                    hop_count,
                });
            },
            (IpAddr::V6(target), IpAddr::V6(hop)) => {
                if expected_ip_type == &IpType::V4 {
                    error!("Expected IPV4 addresses only but got IPV6 addresses: target ip {} hop ip {}", target, hop);
                    exit(1);
                }

                let _ = &memory.add_node_v6(NodeV6 {
                    target_ip: ipv6_to_numeric(target),
                    hop_ip: ipv6_to_numeric(hop),
                    hop_count,
                });
            },
            _ => {
                warn!("SKIPPING ROW: IP type mismatch: Encountered a route with IPs of 2 \
                different/unknown types: target ip {} hop ip {}", raw_target_ip, raw_hop_ip);
            }
        }
    }

    fn extract_strings_from_row(row: &String) -> (&str, &str, &str) {
        // Why not &str.split? It is a lot slower than manually iterating it as done here.
        // That adds up quickly, so this "simple" manual space-skip parsing saves us a lot of time.

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

        let raw_target_ip = &row[0..target_ip_split];

        let raw_hop_count = &row[hop_count_split_start..hop_count_split_end];
        let raw_hop_ip = &row[hop_count_split_end+1..hop_ip_split_end];

        (raw_target_ip, raw_hop_count, raw_hop_ip)
    }

    fn hop_count_str_to_numeric(hop_count_str: &str) -> u8 {
        u8::from_str(&hop_count_str).expect(&construct_error("hop count", hop_count_str))
    }

    fn construct_error(data_label: &str, info: &str) -> String {
        format!("Error while parsing {}: '{}'", data_label, info)
    }

    fn ipv4_to_numeric(parsed_ip: Ipv4Addr) -> u32 {
        let mut shift = 4;
        return parsed_ip.octets()
            .iter()
            .fold(0, |ip, &e| {
                shift -= 1;
                ip | (u32::from(e) << (shift * 8))
            });
    }

    fn ipv6_to_numeric(parsed_ip: Ipv6Addr) -> u128 {
        let mut shift = 16;
        return parsed_ip.octets()
            .iter()
            .fold(0, |ip, &e| {
                shift -= 1;
                ip | (u128::from(e) << (shift * 8))
            });
    }
}