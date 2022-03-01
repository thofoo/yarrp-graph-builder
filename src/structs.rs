pub mod util {
    #[derive(PartialEq)]
    #[derive(Debug)]
    #[derive(Clone)]
    pub enum IpType {
        V4,
        V6,
    }
}

pub mod data {
    use serde::Deserialize;

    pub struct NodeV4 {
        pub target_ip: u32,
        pub hop_ip: u32,
        pub hop_count: u8,
    }

    pub struct NodeV6 {
        pub target_ip: u128,
        pub hop_ip: u128,
        pub hop_count: u8,
    }

    pub struct InternalNode {
        pub target_id: u32,
        pub hop_id: u32,
        pub hop_count: u8,
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    pub struct CsvEdge {
        pub from: i32,
        pub to: i32,
    }
}