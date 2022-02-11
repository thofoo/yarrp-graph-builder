pub mod util {
    #[derive(PartialEq)]
    #[derive(Debug)]
    pub enum IpType {
        V4,
        V6,
    }
}

pub mod yarrp_row {
    pub enum Row {
        V4(RowIpv4),
        V6(RowIpv6),
    }

    pub struct RowIpv4 {
        pub target_ip: u32,
        pub hop_ip: u32,
        pub hop_count: u8,
    }

    pub struct RowIpv6 {
        pub target_ip: u128,
        pub hop_ip: u128,
        pub hop_count: u8,
    }
}