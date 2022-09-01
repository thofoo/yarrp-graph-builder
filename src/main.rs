extern crate core;

use env_logger::Env;
use log::{info, LevelFilter};
use crate::common::parameters::{FeatureToggle, GraphBuilderParameters, GraphParametersToCompute};
use crate::common::structs::util::IpType;
use crate::deduplicator::deduplicator::Deduplicator;
use crate::graph::grapher::Grapher;
use crate::merge::merger::Merger;
use crate::preprocess::warts_data_preprocessor::WartsDataPreprocessor;
use crate::preprocess::yarrp_data_preprocessor::YarrpDataPreprocessor;

mod merge;
mod graph;
mod preprocess;
mod common;
mod buckets;
mod deduplicator;

fn main() {
    let mut env_builder = env_logger::builder();
    let env = Env::new().filter("YARRP_LOG");

    env_builder.filter_level(LevelFilter::Info);
    env_builder.parse_env(env);
    env_builder.init();

    info!("Let's go!");

    // TODO get from config file
    let run_pipeline_on_yarrp_scan = false;
    let run_pipeline_on_caida_scans = true;

    let feature_toggle = FeatureToggle {
        should_preprocess: false,
        should_merge: false,
        should_persist_index: false,
        should_persist_edges: false,
        should_deduplicate_edges: false,
        should_compute_graph: true,
        graph_parameters_to_compute: GraphParametersToCompute {
            degree: true,
            betweenness: true,
        }
    };

    if run_pipeline_on_yarrp_scan {
        run_on_yarrp_scan(feature_toggle);
    } else if run_pipeline_on_caida_scans {
        run_on_caida_scans(feature_toggle);
    } else {
        info!("Nothing to do! Please recheck the configuration.");
    }
}

fn run_on_yarrp_scan(feature_toggle: FeatureToggle) {
    let config = GraphBuilderParameters::new(
        /* read_compressed: */ false,
        IpType::V4,
        "../../01_yarrp_scan/input/v4",
        "../../01_yarrp_scan/output/v4/intermediate",
        "../../01_yarrp_scan/output/v4",
        feature_toggle
    );

    config.print_path_info();

    let mut preprocessor = YarrpDataPreprocessor::new(&config);
    preprocessor.preprocess_files();

    let merger = Merger::new(&config);
    merger.merge_data();

    run(config);
}

fn run_on_caida_scans(feature_toggle: FeatureToggle) {
    let config = GraphBuilderParameters::new(
        /* read_compressed: */ false,
        IpType::V4,
        "../../caida-ip-scans/v4/20210802/input",
        "../../caida-ip-scans/v4/20210802/output/intermediate",
        "../../caida-ip-scans/v4/20210802/output",
        feature_toggle
    );

    config.print_path_info();

    let preprocessor = WartsDataPreprocessor::new(&config);
    preprocessor.preprocess_files();

    run(config);
}

fn run(config: GraphBuilderParameters) {
    info!("Expecting to read IP{:?} addresses.", &config.address_type());

    let deduplicator = Deduplicator::new(&config);
    deduplicator.deduplicate_edges();

    let grapher = Grapher::new(&config);
    grapher.collect_graph_stats();

    info!("############## Finished ##############");
}
