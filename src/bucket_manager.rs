pub mod bucket_manager {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::bucket::bucket::GraphBucket;
    use crate::structs::yarrp_row::{InternalNode, NodeV4, NodeV6};
    use crate::GraphBuilderParameters;

    pub struct GraphBucketManager<'a> {
        buckets: HashMap<u8, GraphBucket>,
        global_ip_mapping: &'a mut HashMap<u128, u32>,
        id_counter: u32,
        config: GraphBuilderParameters,
    }

    impl<'a> GraphBucketManager<'a> {
        pub fn new(config: &GraphBuilderParameters, global_ip_mapping: &'a mut HashMap<u128, u32>) -> GraphBucketManager<'a> {
            GraphBucketManager {
                buckets: HashMap::new(),
                global_ip_mapping,
                id_counter: 1, // 0 is reserved for the source IP
                config: config.clone(),
            }
        }

        pub fn add_node_v4(&mut self, node: NodeV4) {
            let bucket_id = self.calculate_bucket_id_v4(node.target_ip);
            let internal_node = self.convert_to_internal_node_v4(&node);

            let bucket = self.fetch_bucket(bucket_id);
            bucket.add_node(internal_node);
        }

        fn convert_to_internal_node_v4(&mut self, node: &NodeV4) -> InternalNode {
            let target_ip: u32 = node.target_ip.into();
            let hop_ip: u32 = node.hop_ip.into();
            self.convert_to_internal_node_v6(
                &NodeV6 {
                    target_ip: u128::from(target_ip),
                    hop_ip: u128::from(hop_ip),
                    hop_count: node.hop_count.into(),
                }
            )
        }

        fn convert_to_internal_node_v6(&mut self, node: &NodeV6) -> InternalNode {
            let target_node_id = if self.global_ip_mapping.contains_key(&node.target_ip) {
                *self.global_ip_mapping.get(&node.target_ip).unwrap()
            } else {
                let new_node_id = self.id_counter;
                self.id_counter += 1;
                self.global_ip_mapping.insert(node.target_ip, new_node_id);
                new_node_id
            };

            let hop_node_id = if self.global_ip_mapping.contains_key(&node.hop_ip) {
                *self.global_ip_mapping.get(&node.hop_ip).unwrap()
            } else {
                let new_node_id = self.id_counter;
                self.id_counter += 1;
                self.global_ip_mapping.insert(node.hop_ip, new_node_id);
                new_node_id
            };

            InternalNode {
                target_id: target_node_id,
                hop_id: hop_node_id,
                hop_count: node.hop_count.into(),
            }
        }

        pub fn add_node_v6(&mut self, node: NodeV6) {
            let bucket_id = self.calculate_bucket_id_v6(node.target_ip);
            let internal_node = self.convert_to_internal_node_v6(&node);

            let bucket = self.fetch_bucket(bucket_id);
            bucket.add_node(internal_node);
        }

        fn create_path_for_bucket_id(&self, bucket_id: u8) -> PathBuf {
            self.config.intermediary_file_path().join(format!("yarrp.{}.bin", bucket_id))
        }

        fn calculate_bucket_id_v4(&mut self, ip: u32) -> u8 {
            // IPv4 has 4 bytes, 1 byte per IP segment
            // We XOR the second and fourth byte from the left

            let byte1 = u8::try_from((ip & 0x00ff0000) >> (2 * 8)).unwrap();
            let byte2 = u8::try_from(ip & 0xff).unwrap();

            byte1 ^ byte2
        }

        fn calculate_bucket_id_v6(&mut self, ip: u128) -> u8 {
            // IPv6 has 16 byte, 2 bytes per IP segment
            // We XOR last byte of public half + last byte of private half
            // (that's the 8th and 16th bytes from the left)

            let byte1 = u8::try_from((ip & 0x00_00_00_00_00_00_00_ff__00_00_00_00_00_00_00_00) >> (8 * 8)).unwrap();
            let byte2 = u8::try_from(ip & 0xff).unwrap();

            byte1 ^ byte2
        }

        fn fetch_bucket(&mut self, bucket_id: u8) -> &mut GraphBucket {
            let bucket = if self.buckets.contains_key(&bucket_id) {
                self.buckets.get_mut(&bucket_id).unwrap()
            } else {
                let path = self.create_path_for_bucket_id(bucket_id);
                let bucket = GraphBucket::new(path);
                self.buckets.insert(bucket_id, bucket);
                self.buckets.get_mut(&bucket_id).unwrap()
            };

            bucket
        }

        pub fn store_buckets_to_disk(self) {
            for (_, mut bucket) in self.buckets {
                bucket.evict_to_disk()
            }
        }
    }
}