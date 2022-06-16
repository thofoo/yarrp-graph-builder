extern crate core;

use env_logger::Env;
use log::{info, LevelFilter};
use crate::common::parameters::{GraphBuilderParameters, GraphParametersToCompute};
use crate::common::structs::util::IpType;
use crate::graph::grapher::Grapher;
use crate::merge::merger::Merger;
use crate::preprocess::preprocessor::Preprocessor;
use crate::stats_collector::stats_collector::StatsCollector;

mod merge;
mod graph;
mod preprocess;
mod common;
mod buckets;
mod stats_collector;

fn main() {
    let mut env_builder = env_logger::builder();
    let env = Env::new().filter("YARRP_LOG");

    env_builder.filter_level(LevelFilter::Info);
    env_builder.parse_env(env);
    env_builder.init();

    info!("Let's go!");

    // TODO get from cmd line args
    let config = GraphBuilderParameters::new(
        /* read_compressed: */ false,
        IpType::V4,
        "../../01_yarrp_scan/input/v4",
        "../../01_yarrp_scan/output/v4/intermediate",
        "../../01_yarrp_scan/output/v4",
        /* should_preprocess: */ false,
        /* should_merge: */ false,
        /* should_persist_index: */ false,
        /* should_persist_edges: */ false,
        /* should_compute_graph_parameters: */ false,
        /* should_compute_stats: */ true,
        GraphParametersToCompute {
            betweenness: false,
            degree: true,
        },
    );

    info!("Expecting to read IP{:?} addresses.", &config.address_type());

    config.print_path_info();

    let mut preprocessor = Preprocessor::new(&config);
    preprocessor.preprocess_files();

    let merger = Merger::new(&config);
    merger.merge_data();

    let grapher = Grapher::new(&config);
    grapher.collect_graph_parameter_values();

    let stats_collector = StatsCollector::new(&config);
    stats_collector.calculate_stats();
}
