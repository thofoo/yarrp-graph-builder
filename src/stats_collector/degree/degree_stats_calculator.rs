use std::collections::HashMap;
use std::path::{Path, PathBuf};
use crate::common::structs::output::DegreesForNode;

pub struct DegreeStatsCalculator {
    degree_folder_path: PathBuf,
    counts: HashMap<u32, (u32, u32)>,
    high_value_nodes: Vec<DegreesForNode>
}

impl DegreeStatsCalculator {
    pub fn new(degree_values_path: PathBuf) -> DegreeStatsCalculator {
        DegreeStatsCalculator {
            degree_folder_path: degree_values_path,
            counts: HashMap::new(),
            high_value_nodes: Vec::new(),
        }
    }

    pub fn calculate_degree_stats(&mut self) {
        let degree_input_path = self.degree_folder_path.join(Path::new("degree.csv"));
        let high_value_nodes_output_path = self.degree_folder_path.join(Path::new("high_value_nodes.csv"));

        let mut high_value_nodes_writer = csv::Writer::from_path(high_value_nodes_output_path)
            .expect(&format!(
                "Could not create file for storing high value nodes at {}",
                high_value_nodes_output_path.to_str().unwrap()
            ));

        let mut degree_values_reader = csv::Reader::from_path(degree_input_path).unwrap();
        degree_values_reader.deserialize()
            .skip(1)
            .take_while(|value| value.is_ok())
            .for_each(|degrees_for_node: Result<DegreesForNode, _>| {
                self.process_degree_entry(degrees_for_node.unwrap());
            });

        // TODO flush memory into frequency distribution file
    }

    fn process_degree_entry(&mut self, degrees_for_node: DegreesForNode) {
        let deg_in = degrees_for_node.deg_in;
        let deg_out = degrees_for_node.deg_out;

        if !self.counts.contains_key(&deg_in) {
            self.counts.insert(deg_in, (0, 0));
        }
        if !self.counts.contains_key(&deg_out) {
            self.counts.insert(deg_out, (0, 0));
        }

        self.counts.get_mut(&deg_in).unwrap().0 += 1;
        self.counts.get_mut(&deg_out).unwrap().1 += 1;

        if deg_in >= 10 || deg_out >= 10 {
            self.high_value_nodes.push(degrees_for_node);
        }
    }
}