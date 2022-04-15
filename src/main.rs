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
        /* read_compressed: */ true,
        IpType::V6,
        "/mnt/scans/2022_02_routingloops/01_yarrp_scan/ipv6",
        "./2022_02_yarrp_graph/v6/intermediate",
        "../../01_yarrp_scan/output/v6/output",
        /* should_preprocess: */ true,
        /* should_merge: */ true,
        /* should_persist_index: */ true,
        /* should_persist_edges: */ true,
        /* should_compute_graph: */ true,
    );

    info!("Expecting to read IP{:?} addresses.", &config.address_type());

    config.print_path_info();

    let mut preprocessor = Preprocessor::new(&config);
    preprocessor.preprocess_files();

    let merger = Merger::new(&config);
    merger.merge_data();

    let grapher = Grapher::new(&config);
    grapher.collect_graph_stats();
}
