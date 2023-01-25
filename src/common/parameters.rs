use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

use log::error;
use serde::Deserialize;

use crate::IpType;

pub const NODE_INDEX_PATH: &str = "yarrp.node_index.bin";

#[derive(Clone, Debug, Deserialize)]
pub struct OutputPaths {
    mapping: PathBuf,
    edges: PathBuf,
    edges_deduplicated: PathBuf,
    max_node_ids: PathBuf,
    betweenness: PathBuf,
    degree: PathBuf,
}

impl OutputPaths {
    pub fn mapping(&self) -> &PathBuf {
        &self.mapping
    }
    pub fn edges(&self) -> &PathBuf {
        &self.edges
    }
    pub fn edges_deduplicated(&self) -> &PathBuf {
        &self.edges_deduplicated
    }
    pub fn max_node_ids(&self) -> &PathBuf {
        &self.max_node_ids
    }
    pub fn betweenness(&self) -> &PathBuf {
        &self.betweenness
    }
    pub fn degree(&self) -> &PathBuf {
        &self.degree
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub dataset: Dataset,
    pub features: FeatureToggle,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Dataset {
    pub yarrp: DatasetConfig,
    pub caida: DatasetConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetConfig {
    pub enabled: bool,
    pub read_compressed: bool,
    pub address_type: IpType,
    pub input_path: PathBuf,
    pub intermediate_path: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureToggle {
    pub should_preprocess: bool,
    pub should_merge: bool,
    pub should_persist_index: bool,
    pub should_persist_edges: bool,
    pub should_deduplicate_edges: bool,
    pub should_compute_graph: bool,
    pub parameters: GraphParametersToCompute,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GraphParametersToCompute {
    pub degree: DegreeParameters,
    pub betweenness: BetweennessParameters,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DegreeParameters {
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BetweennessParameters {
    pub enabled: bool,
    pub save_intermediate_results_periodically: bool,
    pub result_batch_size: u32,
    pub max_thread_count: u16,
}

impl DatasetConfig {
    pub fn ensure_paths_exist(&self) {
        let input_path = Path::new(&self.input_path).to_path_buf();
        let intermediary_file_path = Path::new(&self.intermediate_path).to_path_buf();
        let output_path = Path::new(&self.output_path).to_path_buf();

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
}

pub fn compute_output_paths(config: &DatasetConfig) -> OutputPaths {
    config.ensure_paths_exist();

    OutputPaths {
        mapping: config.output_path.to_path_buf().join(Path::new("mapping.csv")),
        edges: config.output_path.to_path_buf().join(Path::new("edges.csv")),
        edges_deduplicated: config.output_path.to_path_buf().join(Path::new("edges_deduplicated.csv")),
        max_node_ids: config.output_path.to_path_buf().join(Path::new("max_node_ids.csv")),
        betweenness: config.output_path.to_path_buf().join(Path::new("betweenness.csv")),
        degree: config.output_path.to_path_buf().join(Path::new("degree.csv")),
    }
}