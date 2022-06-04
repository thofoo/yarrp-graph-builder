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
    pub struct NodeBucket {
        bucket_index: i64,
        max_node: i64,
        step: i64,
        size: usize,

        current_node: i64,
    }

    impl NodeBucket {
        pub fn new(bucket_index: i64, min_node: i64, max_node: i64, step: i64, size: u64) -> NodeBucket {
            NodeBucket {
                bucket_index,
                max_node,
                step,
                size: size as usize,
                current_node: min_node
            }
        }

        pub fn index(&self) -> i64 {
            self.bucket_index
        }

        pub fn size(&self) -> usize {
            self.size
        }

        pub fn textual_description_of_range(&self) -> String {
            format!("Thread {}: {} + n * {}", self.bucket_index, self.current_node, self.step)
        }

        pub fn has_next(&self) -> bool {
            self.current_node <= self.max_node
        }
        pub fn next(&mut self) -> i64 {
            let next = self.current_node;
            self.current_node += self.step;

            next
        }
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

        pub fn divide_into_buckets(&self, buckets: u16) -> Vec<NodeBucket> {
            let bucket_count = buckets as i64;

            let total_nodes_to_cover = (self.max_node - self.min_node + 1) as u64;
            let regular_bucket_size = ceil(total_nodes_to_cover as f64 / bucket_count as f64, 0) as u64;
            let last_bucket_size = if total_nodes_to_cover % regular_bucket_size == 0 {
                regular_bucket_size
            } else {
                total_nodes_to_cover % regular_bucket_size
            };

            let mut result = Vec::new();
            for bucket_index in 0..bucket_count {
                let min = self.min_node + bucket_index;
                let is_last_bucket = bucket_index == (bucket_count - 1);

                let bucket_size = if is_last_bucket {
                    last_bucket_size
                } else {
                    regular_bucket_size
                };

                result.push(
                    NodeBucket::new(bucket_index,min, self.max_node, bucket_count, bucket_size)
                );
            }
            result
        }

        pub fn range_inclusive(&self) -> RangeInclusive<i64> {
            self.min_node..=self.max_node
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