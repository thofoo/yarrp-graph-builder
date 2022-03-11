use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use log::info;
use crate::common::structs::data::{CsvEdge, MaxNodeIds};

use crate::GraphBuilderParameters;

pub struct Grapher {
    config: GraphBuilderParameters,
}

impl Grapher {
    pub fn new(config: &GraphBuilderParameters) -> Grapher {
        Grapher { config: config.clone() }
    }

    pub fn graph_data(self) {
        if !self.config.should_compute_graph() {
            info!("Graph computation flag is FALSE - skipping graph computation.");
            return;
        }

        let edges_path = self.config.output_path().join(Path::new("edges.csv"));
        let mapping_path = self.config.output_path().join(Path::new("mapping.csv"));
        let max_node_id_path = self.config.output_path().join(Path::new("max_node_ids.csv"));

        let max_node_ids: MaxNodeIds = csv::Reader::from_path(max_node_id_path).unwrap()
            .deserialize()
            .next()
            .unwrap()
            .unwrap();

        let mut known_node_edges = vec![HashSet::<i64>::new(); max_node_ids.known + 1];
        // 0 is deliberately left empty to avoid off-by-one errors from conversions
        let mut unknown_node_edges = vec![HashSet::<i64>::new(); max_node_ids.unknown + 1];

        let mut edges_reader = csv::Reader::from_path(edges_path).unwrap();
        edges_reader.deserialize()
            .skip(1)
            .take_while(|edge| edge.is_ok())
            .for_each(|edge: Result<CsvEdge, _>| {
                let data = edge.unwrap();

                if data.from >= 0 {
                    known_node_edges[data.from.unsigned_abs() as usize].insert(data.to);
                } else {
                    unknown_node_edges[data.from.unsigned_abs() as usize].insert(data.to);
                }
            });

        println!("done");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(
            std::time::Duration::from_secs(20)
        );
        println!("now done for real");
        known_node_edges[0].len();
        unknown_node_edges[0].len();
    }
}