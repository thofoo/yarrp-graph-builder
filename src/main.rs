extern crate core;

use std::fs;
use env_logger::Env;
use log::{info, LevelFilter};
use crate::common::parameters::{BetweennessParameters, compute_output_paths, Config, DatasetConfig, FeatureToggle, GraphParametersToCompute, OutputPaths};
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

    let config_str = fs::read_to_string("Config.toml").unwrap();
    let config: Config = toml::from_str(config_str.as_str()).unwrap();

    info!("Running with following config: {:?}", config);

    if config.dataset.yarrp.enabled {
        run_on_yarrp_scan(config.dataset.yarrp, &config.features);
    }

    if config.dataset.caida.enabled {
        run_on_caida_scans(config.dataset.caida, &config.features);
    }
}

fn run_on_yarrp_scan(config: DatasetConfig, toggle: &FeatureToggle) {
    let output_paths = compute_output_paths(&config);

    if toggle.should_preprocess {
        let mut preprocessor = YarrpDataPreprocessor::new(&config);
        preprocessor.preprocess_files();
    } else {
        info!("Preprocessing flag is FALSE - skipping preprocessing.");
    }

    if toggle.should_merge {
        let merger = Merger::new(&config, &output_paths);
        merger.merge_data();
    } else {
        info!("Merging flag is FALSE - skipping merging.");
    }

    run(config, toggle, output_paths);
}

fn run_on_caida_scans(config: DatasetConfig, toggle: &FeatureToggle) {
    let output_paths = compute_output_paths(&config);

    if toggle.should_preprocess {
        let preprocessor = WartsDataPreprocessor::new(&config, &output_paths);
        preprocessor.preprocess_files();
    } else {
        info!("Preprocessing flag is FALSE - skipping preprocessing.");
    }

    info!("No merging step necessary for CAIDA scans.");

    run(config, toggle, output_paths);
}

fn run(config: DatasetConfig, toggle: &FeatureToggle, output_paths: OutputPaths) {
    if toggle.should_deduplicate_edges {
        let deduplicator = Deduplicator::new(&output_paths);
        deduplicator.deduplicate_edges();
    } else {
        info!("Deduplication flag is FALSE - skipping deduplication.");
    }

    if toggle.should_compute_graph {
        let grapher = Grapher::new(&config, &output_paths, &toggle.parameters);
        grapher.collect_graph_stats();
    } else {
        info!("Graph computation flag is FALSE - skipping graph computation.");
    }

    info!("############## Finished ##############");
}
