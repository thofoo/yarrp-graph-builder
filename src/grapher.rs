pub mod grapher {
    use std::collections::HashSet;
    use std::io::{BufReader, Write};
    use std::path::{Path, PathBuf};

    use log::{info, warn};
    use rev_lines::RevLines;

    use crate::GraphBuilderParameters;
    use crate::structs::data::CsvEdge;

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

            let known_node_count = linecount::count_lines(
                std::fs::File::open(mapping_path).unwrap()
            ).unwrap();
            let unknown_node_count = find_highest_unknown_node(&edges_path) + 1;

            let mut known_node_edges = vec![HashSet::<i64>::new(); known_node_count];
            // size must be unknown_node_count + 1 because we have no node 0
            // 0 is deliberately left empty to avoid any off-by-one errors
            let mut unknown_node_edges = vec![HashSet::<i64>::new(); unknown_node_count + 1];

            let mut edges_reader = csv::Reader::from_path(edges_path).unwrap();
            edges_reader.deserialize()
                .skip(1)
                .take_while(|edge| edge.is_ok())
                .for_each(|edge: Result<CsvEdge, _>| {
                    let data = edge.unwrap();

                    if data.from >= 0 {
                        known_node_edges[data.from.unsigned_abs() as usize].insert(data.to);
                    } else {
                        known_node_edges[data.from.unsigned_abs() as usize].insert(data.to);
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

    fn find_highest_unknown_node(edges_path: &PathBuf) -> usize {
        // we assign the negative IDs "incrementally", thus the highest value has to be at the end
        // of the file. once we find a negative value (bottom-up), that is the value we want
        // THIS ASSUMPTION IS WRONG // TODO FIX
        // TODO FIX

        let buffer = BufReader::new(std::fs::File::open(edges_path).unwrap());
        let rev_lines = RevLines::new(buffer).unwrap();

        for line in rev_lines {
            let csv_edge: CsvEdge = csv::StringRecord::from(line.split(",").collect::<Vec<&str>>())
                .deserialize(None).unwrap();

            if csv_edge.to < 0 {
                return csv_edge.to.unsigned_abs() as usize
            }
            if csv_edge.from < 0 {
                return csv_edge.from.unsigned_abs() as usize
            }
        }

        warn!("No unknown nodes found, this could be a mistake");
        return 0
    }
}