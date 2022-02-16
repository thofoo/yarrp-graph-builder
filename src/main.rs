extern crate core;

use env_logger::Env;
use log::{info, LevelFilter};

use crate::parameters::parameters::GraphBuilderParameters;
use crate::preprocessor::preprocessor::Preprocessor;
use crate::structs::util::IpType;

mod preprocessor_util;
mod structs;
mod preprocessor;
mod parameters;
mod bucket;
mod bucket_manager;

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
        "../01_yarrp_scan/input/v6",
        "../01_yarrp_scan/output/v6",
    );
    let preprocessor = Preprocessor::new(config);
    preprocessor.preprocess_files();
}
