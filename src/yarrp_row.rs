pub mod yarrp_row {
    pub enum YarrpRow {
        V4(YarrpRowIpv4),
        V6(YarrpRowIpv6),
    }

    pub struct YarrpRowIpv4 {
        pub target_ip: u32,
        pub hop_ip: u32,
        pub hop_count: u8,
    }

    pub struct YarrpRowIpv6 {
        pub target_ip: u128,
        pub hop_ip: u128,
        pub hop_count: u8,
    }
}