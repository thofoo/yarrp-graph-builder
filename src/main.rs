extern crate core;

use env_logger::Env;
use log::{info, LevelFilter};

use crate::parameters::parameters::GraphBuilderParameters;
use crate::preprocessor::preprocessor::Preprocessor;
use crate::structs::util::IpType;
use crate::structs::yarrp_row::{Row, RowIpv4, RowIpv6};

mod preprocessor_util;
mod structs;
mod preprocessor;
mod parameters;

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
    );
    let preprocessor = Preprocessor::new(config);
    preprocessor.preprocess_files();
}
