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
    use std::ops::RangeInclusive;
    use math::round::ceil;
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
        pub fn range_inclusive_chopped(&self, pieces: u16) -> Vec<(usize, RangeInclusive<i64>)> {
            let mut result = Vec::new();
            let node_count = self.max_node + self.offset() + 1;
            let single_range_size = ceil((node_count as f64) / (pieces as f64), 0) as i64;

            let mut min = self.min_node;

            for _ in 0..pieces {
                let max = if min + single_range_size <= self.max_node {
                    min + single_range_size
                } else {
                    self.max_node
                };
                let range_size = (max - min + 1) as usize;
                result.push((range_size, min..=max));
                min = max + 1;
            }

            result
        }
        pub fn min_node(&self) -> i64 {
            self.min_node
        }
        pub fn max_node(&self) -> i64 {
            self.max_node
        }
        pub fn offset(&self) -> i64 {
            self.min_node.abs()
        }
    }
}