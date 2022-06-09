use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

use log::{error, info};

use crate::IpType;

pub const NODE_INDEX_PATH: &str = "yarrp.node_index.bin";

#[derive(Clone, Debug)]
pub struct OutputPaths {
    _root: PathBuf,
    mapping: PathBuf,
    edges: PathBuf,
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
pub struct GraphBuilderParameters {
    read_compressed: bool,
    address_type: IpType,
    input_path: PathBuf,
    intermediary_file_path_original: PathBuf,
    intermediary_file_path: PathBuf,
    should_preprocess: bool,
    should_merge: bool,
    should_persist_index: bool,
    should_persist_edges: bool,
    should_compute_graph: bool,
    graph_parameters_to_compute: GraphParametersToCompute,

    output_paths: OutputPaths,
}

#[derive(Clone)]
pub struct GraphParametersToCompute {
    pub betweenness: bool,
    pub degree: bool
}

impl GraphBuilderParameters {
    pub fn new(
        read_compressed: bool,
        address_type: IpType,
        input_folder: &str,
        intermediate_folder: &str,
        output_folder: &str,
        should_preprocess: bool,
        should_merge: bool,
        should_persist_index: bool,
        should_persist_edges: bool,
        should_compute_graph: bool,
        graph_parameters_to_compute: GraphParametersToCompute,
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

        if !intermediary_file_path.exists() {
            error!("Specified intermediate path does not exist");
            exit(1);
        }

        if !intermediary_file_path.is_dir() {
            error!("Specified intermediate path is not a directory");
            exit(1);
        }

        if !output_path.exists() {
            error!("Specified output path does not exist");
            exit(1);
        }

        if !output_path.is_dir() {
            error!("Specified output path is not a directory");
            exit(1);
        }

        fs::create_dir_all(&intermediary_file_path).expect("Could not create intermediary file paths");

        let output_paths = OutputPaths {
            _root: output_path.to_path_buf(),
            mapping: output_path.to_path_buf().join(Path::new("mapping.csv")),
            edges: output_path.to_path_buf().join(Path::new("edges.csv")),
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
            output_paths,
            should_preprocess,
            should_merge,
            should_persist_index,
            should_persist_edges,
            should_compute_graph,
            graph_parameters_to_compute,
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
    pub fn should_compute_graph(&self) -> bool {
        self.should_compute_graph
    }

    pub fn graph_parameters_to_compute(&self) -> &GraphParametersToCompute {
        &self.graph_parameters_to_compute
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