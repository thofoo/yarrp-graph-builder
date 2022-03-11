extern crate core;

use env_logger::Env;
use log::{info, LevelFilter};
use crate::common::parameters::GraphBuilderParameters;
use crate::common::structs::util::IpType;
use crate::graph::grapher::Grapher;
use crate::merge::merger::Merger;
use crate::preprocess::preprocessor::Preprocessor;

mod merge;
mod graph;
mod preprocess;
mod common;
mod buckets;

fn main() {
    let mut env_builder = env_logger::builder();
    let env = Env::new().filter("YARRP_LOG");

    env_builder.filter_level(LevelFilter::Info);
    env_builder.parse_env(env);
    env_builder.init();

    info!("Let's go!");

    // TODO get from cmd line args
    let config = GraphBuilderParameters::new(
        IpType::V6,
        "../../01_yarrp_scan/input/v6",
        "../../01_yarrp_scan/output/v6/intermediate",
        "../../01_yarrp_scan/output/v6",
        /* should_preprocess: */ true,
        /* should_merge: */ true,
        /* should_persist_index: */ true,
        /* should_persist_edges: */ true,
        /* should_compute_graph: */ true,
    );

    info!("Expecting to read IP{:?} addresses.", &config.address_type());

    info!("Input path: {}", &config.input_path().to_str().unwrap());
    info!("Intermediary file path: {}", &config.intermediary_file_path().to_str().unwrap());
    info!("Output path: {}", &config.output_path().to_str().unwrap());

    let mut preprocessor = Preprocessor::new(&config);
    preprocessor.preprocess_files();

    let merger = Merger::new(&config);
    merger.merge_data();

    let grapher = Grapher::new(&config);
    grapher.graph_data();
}
