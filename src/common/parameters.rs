use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

use log::error;
use serde::{Serialize, Deserialize};

use crate::IpType;

pub const NODE_INDEX_FILENAME: &str = "yarrp.node_index.bin";

#[derive(Clone, Debug, Deserialize)]
pub struct OutputPaths {
    pub mapping: PathBuf,
    pub edges: PathBuf,
    pub edges_deduplicated: PathBuf,
    pub max_node_ids: PathBuf,
    pub betweenness: PathBuf,
    pub degree: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub dataset: Dataset,
    pub features: FeatureToggle,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub yarrp: DatasetConfig,
    pub warts: DatasetConfig,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    pub enabled: bool,
    pub read_compressed: bool,
    pub address_type: IpType,
    pub input_path: PathBuf,
    pub intermediate_path: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FeatureToggle {
    pub should_preprocess: bool,
    pub should_merge: bool,
    pub should_persist_index: bool,
    pub should_persist_edges: bool,
    pub should_deduplicate_edges: bool,
    pub should_compute_graph: bool,
    pub parameters: GraphParametersToCompute,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GraphParametersToCompute {
    pub degree: DegreeParameters,
    pub betweenness: BetweennessParameters,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DegreeParameters {
    pub enabled: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BetweennessParameters {
    pub enabled: bool,
    pub save_intermediate_results_periodically: bool,
    pub result_batch_size: u32,
    pub max_thread_count: u16,
}

pub fn compute_output_paths(config: &DatasetConfig) -> OutputPaths {
    ensure_paths_exist(config);

    OutputPaths {
        mapping: config.output_path.to_path_buf().join(Path::new("mapping.csv")),
        edges: config.output_path.to_path_buf().join(Path::new("edges.csv")),
        edges_deduplicated: config.output_path.to_path_buf().join(Path::new("edges_deduplicated.csv")),
        max_node_ids: config.output_path.to_path_buf().join(Path::new("max_node_ids.csv")),
        betweenness: config.output_path.to_path_buf().join(Path::new("betweenness.csv")),
        degree: config.output_path.to_path_buf().join(Path::new("degree.csv")),
    }
}

fn ensure_paths_exist(config: &DatasetConfig) {
    let input_path = Path::new(&config.input_path).to_path_buf();
    let intermediary_file_path = Path::new(&config.intermediate_path).to_path_buf();
    let output_path = Path::new(&config.output_path).to_path_buf();

    if !input_path.exists() {
        error!("Specified input path does not exist");
        exit(1);
    }

    if !input_path.is_dir() {
        error!("Specified input path is not a directory");
        exit(1);
    }

    if intermediary_file_path.exists() && !intermediary_file_path.is_dir() {
        error!("Specified intermediate path is not a directory");
        exit(1);
    }

    if output_path.exists() && !output_path.is_dir() {
        error!("Specified output path is not a directory");
        exit(1);
    }

    fs::create_dir_all(&intermediary_file_path).expect("Could not create intermediary file paths");
    fs::create_dir_all(&output_path).expect("Could not create output file paths");
}