pub mod util {
    use serde::{Serialize, Deserialize};

    #[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
    pub enum IpType {
        V4,
        V6,
    }
}

pub mod parse_data {
    use std::ops::RangeInclusive;
    use serde::Serialize;
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
        pub target_id: u64,
        pub hop_id: u64,
        pub hop_count: u8,
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    pub struct CsvEdge {
        pub from: i64,
        pub to: i64,
    }

    #[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct MaxNodeIds {
        pub known: usize,
        pub unknown: usize,
    }

    #[derive(Clone)]
    pub struct NodeBoundaries {
        min_node: i64,
        max_node: i64,
    }

    impl NodeBoundaries {
        pub fn new(max_node_ids: MaxNodeIds) -> NodeBoundaries {
            NodeBoundaries {
                min_node: -(max_node_ids.unknown as i64),
                max_node: max_node_ids.known as i64,
            }
        }

        pub fn range_inclusive(&self) -> RangeInclusive<i64> {
            self.min_node..=self.max_node
        }
        pub fn len(&self) -> usize {
            (self.min_node + self.max_node + 1) as usize
        }
    }
}