use std::fs::File;

use csv::Writer;
use log::info;
use pbr::ProgressBar;

use crate::graph::common::graph::Graph;
use crate::OutputPaths;

pub struct Deduplicator {
    output_paths: OutputPaths,
}

impl Deduplicator {
    pub fn new(output_paths: &OutputPaths) -> Deduplicator {
        Deduplicator {
            output_paths: output_paths.clone(),
        }
    }

    pub fn deduplicate_edges(&self) {
        info!("Starting edge deduplication by reading in graph...");
        let mut writer = self.create_file_writer();

        let graph = Graph::new(&self.output_paths, /* from_deduplicated = */ false);

        info!("Storing deduplicated paths to disk...");
        let edges = graph.edges();

        let mut progress_bar = ProgressBar::new(graph.boundaries().len() as u64);
        let mut counter = 0;

        writer.serialize(("from", "to")).unwrap();
        for node in graph.boundaries().range_inclusive() {
            let neighbors = &edges[node];
            for neighbor in neighbors {
                writer.serialize((node, neighbor)).unwrap();
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
        csv::Writer::from_path(&self.output_paths.edges_deduplicated())
            .expect(&format!(
                "Could not create file for storing deduplicated edges at {}",
                &self.output_paths.degree().to_str().unwrap()
            ))
    }
}