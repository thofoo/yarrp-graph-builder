extern crate core;

use env_logger::Env;
use log::{info, LevelFilter};
use crate::merger::merger::Merger;

use crate::parameters::parameters::GraphBuilderParameters;
use crate::preprocessor::preprocessor::Preprocessor;
use crate::structs::util::IpType;

mod util;
mod structs;
mod preprocessor;
mod parameters;
mod bucket;
mod bucket_manager;
mod merger;
mod parser;

fn main() {
    let mut env_builder = env_logger::builder();
    let env = Env::new().filter("YARRP_LOG");

    env_builder.filter_level(LevelFilter::Info);
    env_builder.parse_env(env);
    env_builder.init();

    info!("Let's go!");

    // TODO get from cmd line args
    let config = GraphBuilderParameters::new(
        IpType::V4,
        "../01_yarrp_scan/input/v4",
        "../01_yarrp_scan/output/v4",
        /* should_preprocess: */ true,
        /* should_merge: */ true,
        /* should_persist_index: */ true,
        /* should_persist_edges: */ true,
    );

    info!("Expecting to read IP{:?} addresses.", &config.address_type());

    info!("Input path: {}", &config.input_path().to_str().unwrap());
    info!("Intermediary file path: {}", &config.intermediary_file_path().to_str().unwrap());
    info!("Output path: {}", &config.output_path().to_str().unwrap());

    let preprocessor = Preprocessor::new(&config);
    preprocessor.preprocess_files();

    let merger = Merger::new(&config);
    merger.merge_data();
}
