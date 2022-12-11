use std::collections::HashSet;
use std::fs::File;
use std::io::Write;

use hashbrown::HashMap;
use log::info;
use pbr::ProgressBar;
use serde::Serialize;

use crate::graph::common::graph::Graph;

pub struct ConnectivityCalculator {
    graph: Graph,
    file: File,
}


impl ConnectivityCalculator {
    pub fn new(
        graph: Graph,
        file: File
    ) -> ConnectivityCalculator {
        ConnectivityCalculator { graph, file }
    }

    pub fn calculate_and_persist(&mut self) {
        info!("Calculating degree connectivity values...");
        let mut progress_bar = ProgressBar::new(self.graph.boundaries().len() as u64);
        let mut counter = 0;

        writeln!(&mut self.file, "node,stats").unwrap();

        for node in self.graph.boundaries().range_inclusive() {
            if counter == 70 {
                println!("hello world");
            }
            if self.graph.edges()[node].len() == 0 {
                continue
            }

            let connectivity_stats = self.calculate_stats(node);
            self.write_stats(node, connectivity_stats);

            counter += 1;
            if counter % 1/*0_000*/ == 0 {
                progress_bar.add(1/*0_000*/);
            }
        }

        progress_bar.finish();
        self.file.flush().unwrap();
    }

    /*fn calculate_stats(&self, starting_node: i64) -> HashMap<u32, u32> {
        let edges = self.graph.edges();

        let mut encounters = HashMap::<i64, u32>::new();
        let mut visited_nodes = HashSet::<i64>::new();

        let mut stack = Vec::<i64>::new();
        stack.push(starting_node);

        while let Some(node) = stack.pop() {
            visited_nodes.insert(node);

            let current_value = *encounters.get(&node).unwrap_or(&0);
            encounters.insert(node, current_value + 1);

            edges[node].iter()
                .filter(|neighbor| !visited_nodes.contains(neighbor))
                .for_each(|&neighbor| stack.push(neighbor));
        }

        let mut stats = HashMap::<u32, u32>::new();
        for &value in encounters.values() {
            let current_value = *stats.get(&value).unwrap_or(&0);
            stats.insert(value, current_value + 1);
        }
        stats
    }*/

    fn calculate_stats(&self, starting_node: i64) -> HashMap<u32, u32> {
        let mut encounters = HashMap::<i64, u32>::new();
        let mut visited_nodes = HashSet::<i64>::new();

        self.count_encounters_dfs(starting_node, &mut encounters, &mut visited_nodes);

        let mut stats = HashMap::<u32, u32>::new();
        for &value in encounters.values() {
            let current_value = *stats.get(&value).unwrap_or(&0);
            stats.insert(value, current_value + 1);
        }
        stats
    }

    fn count_encounters_dfs(
        &self,
        node: i64,
        encounters: &mut HashMap<i64, u32>,
        visited_nodes: &mut HashSet<i64>
    ) {
        // let indent = ">".repeat(visited_nodes.len());
        // println!("{}{}", indent, node);
        visited_nodes.insert(node);

        let edges = self.graph.edges();

        // let current_value = *encounters.get(&node).unwrap_or(&0);
        // encounters.insert(node, current_value + 1);

        edges[node].iter()
            .for_each(
                |&neighbor| {
                    if !visited_nodes.contains(&neighbor) {
                        self.count_encounters_dfs(neighbor, encounters, visited_nodes)
                    } /*else {
                        println!("{}dupl {}", indent, node);
                    }*/
                }
            );
    }

    fn write_stats(&mut self, node: i64, stats: HashMap<u32, u32>) {
        write!(&mut self.file, "{},", node).unwrap();

        let joined_stats = stats.iter()
            .map(|(&k, &v)| {
                let result = (k, v);
                format!("\"{:?}\"", result)
            })
            .reduce(|c, s| format!("{},{}", c, s))
            .unwrap_or(String::from(""));

        writeln!(&mut self.file, "{}", joined_stats).unwrap();
    }

    pub fn graph(self) -> Graph {
        self.graph
    }
}

#[derive(Serialize)]
struct StatsContainer {
    node: i64,
    tuples: Vec<(u32, u32)>,
}