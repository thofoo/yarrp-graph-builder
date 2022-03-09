pub mod grapher {
    use std::path::Path;
    use graphrs::{Edge, EdgeDedupeStrategy, Graph, GraphSpecs, MissingNodeStrategy, SelfLoopsFalseStrategy};
    use graphrs::algorithms::centrality::betweenness::betweenness_centrality;

    use log::info;

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

            let mut reader = csv::Reader::from_path(
                self.config.output_path().join(Path::new("edges.csv"))
            ).unwrap();

            let parsed_edges = reader.deserialize()
                .skip(1)
                .take_while(|edge| edge.is_ok())
                .map(|edge: Result<CsvEdge, _>| {
                    let data = edge.unwrap();
                    Edge::new(data.from, data.to)
                })
                .into_iter()
                .collect();

            let graph = Graph::<i32, i32>::new_from_nodes_and_edges(
                vec![], // we let the missing node strategy create our nodes
                parsed_edges,
                GraphSpecs {
                    directed: true,
                    edge_dedupe_strategy: EdgeDedupeStrategy::KeepFirst,
                    missing_node_strategy: MissingNodeStrategy::Create,
                    multi_edges: false,
                    self_loops: true,
                    self_loops_false_strategy: SelfLoopsFalseStrategy::Drop,
                }
            ).expect("Building graph failed");

            let centralities = betweenness_centrality(
                &graph,
                /* weighted = */ false,
                /* normalized = */ false,
            ).expect("Could not compute centralities for graph");

            println!("Centralities for {:?}", self.config.address_type());
            println!("{:?}", centralities);
        }
    }
}