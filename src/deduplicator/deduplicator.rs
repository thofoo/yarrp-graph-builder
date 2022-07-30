use std::collections::HashSet;
use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;

use crate::graph::common::collection_wrappers::Stack;
use crate::graph::common::graph::Graph;
use crate::GraphBuilderParameters;

pub struct Deduplicator {
    config: GraphBuilderParameters,
}

impl Deduplicator {
    pub fn new(config: &GraphBuilderParameters) -> Deduplicator {
        Deduplicator { config: config.clone() }
    }

    pub fn deduplicate_edges(&self) {
        if !self.config.enabled_features().should_deduplicate_edges() {
            info!("Deduplication flag is FALSE - skipping deduplication.");
            return;
        }

        info!("Starting edge deduplication by reading in graph...");
        let mut writer = self.create_file_writer();

        let graph = Graph::new(&self.config, /* from_deduplicated = */ false);

        info!("Storing deduplicated paths to disk...");
        let edges = graph.edges();

        let mut node_stack = Stack::<i64>::new();
        node_stack.push(0);

        let mut visited_nodes = HashSet::<i64>::new();

        let mut progress_bar = ProgressBar::new(edges.total_nodes() as u64);
        let mut counter = 0;

        writer.serialize(("from", "to")).unwrap();
        while !node_stack.is_empty() {
            let node = node_stack.upop();
            if visited_nodes.contains(&node) {
                continue;
            }

            visited_nodes.insert(node);

            let next_nodes = &edges[node];
            for &next_node in next_nodes {
                writer.serialize((node, next_node)).unwrap();
                node_stack.push(next_node);
            }

            counter += 1;
            if counter % 10_000 == 0 {
                progress_bar.add(10_000);
            }
        }
        progress_bar.finish();
        writer.flush().unwrap();
    }

    fn create_file_writer(&self) -> Writer<File> {
        csv::Writer::from_path(&self.config.output_paths().edges_deduplicated())
            .expect(&format!(
                "Could not create file for storing deduplicated edges at {}",
                &self.config.output_paths().degree().to_str().unwrap()
            ))
    }
}