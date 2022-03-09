pub mod grapher {
    use std::collections::HashMap;
    use std::io::{stdout, Write};
    use std::path::Path;
    use std::thread::sleep;
    use std::time;
    use std::time::Duration;

    use log::info;
    use petgraph::Graph;
    use petgraph::graph::NodeIndex;

    use crate::{GraphBuilderParameters, x_ego};
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

            let mut reader = csv::Reader::from_path(
                self.config.output_path().join(Path::new("edges.csv"))
            ).unwrap();

            let mut node_cache = HashMap::<i32, NodeIndex>::new();
            let mut graph = Graph::<i32, i32>::new();
            let parsed_edges: Vec<(NodeIndex, NodeIndex)> = reader.deserialize()
                .skip(1)
                .take_while(|edge| edge.is_ok())
                .map(|edge: Result<CsvEdge, _>| {
                    let data = edge.unwrap();
                    (
                        self.retrieve_node(&mut graph, &mut node_cache, data.from),
                        self.retrieve_node(&mut graph, &mut node_cache, data.to),
                    )
                })
                .into_iter()
                .collect();

            print!("done parsing");

            let starting_node: NodeIndex = node_cache[&0];

            // we don't need this anymore
            node_cache.clear();
            node_cache.shrink_to_fit();

            graph.extend_with_edges(parsed_edges.iter());


            let edges = graph.edges(starting_node);
            print!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            stdout().flush().unwrap();
            sleep(Duration::from_secs(20));
            print!("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
            x_ego::x_ego_betweenness::calculate(&graph, &starting_node);
            print!("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
        }

        fn retrieve_node(&self, graph: &mut Graph<i32, i32>, node_cache: &mut HashMap<i32, NodeIndex>, label: i32) -> NodeIndex {
            if !node_cache.contains_key(&label) {
                let node = graph.add_node(label);
                node_cache.insert(label, node);
            }

            *node_cache.get(&label).unwrap()
        }
    }
}