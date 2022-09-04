use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

use log::{error, info};

use crate::IpType;

pub const NODE_INDEX_PATH: &str = "yarrp.node_index.bin";

#[derive(Clone, Debug)]
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

#[derive(Clone)]
pub struct FeatureToggle {
    pub should_preprocess: bool,
    pub should_merge: bool,
    pub should_persist_index: bool,
    pub should_persist_edges: bool,
    pub should_deduplicate_edges: bool,
    pub should_compute_graph: bool,
    pub graph_parameters_to_compute: GraphParametersToCompute,
}

impl FeatureToggle {
    pub fn should_preprocess(&self) -> bool {
        self.should_preprocess
    }
    pub fn should_merge(&self) -> bool {
        self.should_merge
    }
    pub fn should_persist_index(&self) -> bool {
        self.should_persist_index
    }
    pub fn should_persist_edges(&self) -> bool {
        self.should_persist_edges
    }
    pub fn should_deduplicate_edges(&self) -> bool {
        self.should_deduplicate_edges
    }
    pub fn should_compute_graph(&self) -> bool {
        self.should_compute_graph
    }

    pub fn graph_parameters_to_compute(&self) -> &GraphParametersToCompute {
        &self.graph_parameters_to_compute
    }
}

#[derive(Clone)]
pub struct GraphBuilderParameters {
    read_compressed: bool,
    address_type: IpType,
    input_path: PathBuf,
    intermediary_file_path_original: PathBuf,
    intermediary_file_path: PathBuf,
    enabled_features: FeatureToggle,
    output_paths: OutputPaths,
}

#[derive(Clone)]
pub struct GraphParametersToCompute {
    pub degree: bool,
    pub betweenness: BetweennessParameters,
}

#[derive(Clone)]
pub struct BetweennessParameters {
    pub enabled: bool,
    pub save_intermediate_results_periodically: bool,
    pub result_batch_size: u32,
    pub max_thread_count: u16,
}

impl GraphBuilderParameters {
    pub fn new(
        read_compressed: bool,
        address_type: IpType,
        input_folder: &str,
        intermediate_folder: &str,
        output_folder: &str,
        enabled_features: FeatureToggle,
    ) -> GraphBuilderParameters {
        let input_path = Path::new(input_folder).to_path_buf();
        let intermediary_file_path = Path::new(intermediate_folder).to_path_buf();
        let output_path = Path::new(output_folder).to_path_buf();

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

        let output_paths = OutputPaths {
            mapping: output_path.to_path_buf().join(Path::new("mapping.csv")),
            edges: output_path.to_path_buf().join(Path::new("edges.csv")),
            edges_deduplicated: output_path.to_path_buf().join(Path::new("edges_deduplicated.csv")),
            max_node_ids: output_path.to_path_buf().join(Path::new("max_node_ids.csv")),
            betweenness: output_path.to_path_buf().join(Path::new("betweenness.csv")),
            degree: output_path.to_path_buf().join(Path::new("degree.csv")),
        };

        GraphBuilderParameters {
            read_compressed,
            address_type,
            input_path,
            intermediary_file_path_original: intermediary_file_path.to_path_buf(),
            intermediary_file_path,
            enabled_features,
            output_paths,
        }
    }

    /**
     * Returns true if the directory was successfully created, false if the directory already existed.
     */
    pub fn add_intermediate_suffix(&mut self, suffix: &str) -> bool {
        self.intermediary_file_path = self.intermediary_file_path_original.join(
            Path::new(suffix),
        );
        let path_is_new = !self.intermediary_file_path.exists();
        if path_is_new {
            fs::create_dir_all(&self.intermediary_file_path).unwrap();
        }
        path_is_new
    }

    pub fn read_compressed(&self) -> bool {
        self.read_compressed
    }
    pub fn address_type(&self) -> &IpType {
        &self.address_type
    }
    pub fn input_path(&self) -> &PathBuf {
        &self.input_path
    }
    pub fn intermediary_file_path_original(&self) -> &PathBuf {
        &self.intermediary_file_path_original
    }
    pub fn intermediary_file_path(&self) -> &PathBuf {
        &self.intermediary_file_path
    }

    pub fn enabled_features(&self) -> &FeatureToggle {
        &self.enabled_features
    }

    pub fn output_paths(&self) -> &OutputPaths {
        &self.output_paths
    }

    pub fn print_path_info(&self) {
        info!("Input path: {}", self.input_path().to_str().unwrap());
        info!("Intermediary file path: {}", self.intermediary_file_path().to_str().unwrap());
        info!("Output paths: {:?}", self.output_paths());
    }
}