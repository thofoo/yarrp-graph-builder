use std::collections::HashMap;
use std::fs;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};

use log::{debug, info, trace};
use pbr::ProgressBar;
use crate::buckets::bucket_manager::GraphBucketManager;
use crate::common::parameters;
use crate::DatasetConfig;

use crate::preprocess::{parser, file_util};

pub struct YarrpDataPreprocessor {
    config: DatasetConfig,
}

impl YarrpDataPreprocessor {
    pub fn new(config: &DatasetConfig) -> YarrpDataPreprocessor {
        YarrpDataPreprocessor { config: config.clone() }
    }

    /**
     * Joins the path into buckets and creates a node-to-IP mapping.
     * This step only generates binary files, no human-readable output is generated.
     * The output of this step is required for the merger step.
     *
     *  Requires: Input files (either compressed as .yarrp.bz2 or uncompressed as .yarrp)
     *            at dataset.yarrp.input_path
     * Generates: Intermediate binary files at dataset.yarrp.intermediate_path
     */
    pub fn preprocess_files(&mut self) {
        info!("Step: Preprocessing YARRP files.");
        info!("Expecting to work with IP{:?} addresses.", self.config.address_type);

        if self.config.read_compressed {
            info!("Reading COMPRESSED: Retrieving all files ending with bz2");
        } else {
            info!("Reading UNCOMPRESSED: Retrieving all files NOT ending with bz2");
        }

        let raw_files_list = fs::read_dir(&self.config.input_path).unwrap();
        let files_to_process: Vec<DirEntry> = raw_files_list
            .map(|entry| entry.unwrap())
            .filter(|i| i.path().is_file())
            .filter(|i| {
                let path_buf = i.path().to_path_buf();
                let path_string = path_buf.to_str().unwrap();
                if self.config.read_compressed {
                    path_string.ends_with(".bz2")
                } else {
                    !path_string.ends_with(".bz2")
                }
            })
            .collect();

        let file_count = files_to_process.len() as u64;
        if file_count == 0 {
            info!(
                "Found no files to process (read_compressed: {}). Proceeding with next step.",
                self.config.read_compressed
            );
            return
        }
        let dir_listing: Vec<String> = files_to_process.iter()
            .map(|entry| format!("- {:?}", entry.path()))
            .collect();

        info!("Found {} files:\n{}", file_count, dir_listing.join("\n"));

        let mut progress_bar = ProgressBar::new(file_count);
        progress_bar.set(0);

        let mut index: HashMap<u128, u64> = HashMap::new();
        let mut counter = 1; // 0 is reserved for the source IP
        for file in files_to_process {
            let (path, path_is_new) = self.create_intermediate_path(file.file_name().to_str().unwrap());
            if !path_is_new {
                progress_bar.inc();
                continue
            }

            let mut memory = GraphBucketManager::new(
                path,
                &mut index,
                counter
            );
            self.preprocess_single_file(file.path(), &mut memory);
            counter = memory.id_counter();
            memory.store_buckets_to_disk();
            progress_bar.inc();
        }

        self.store_index_to_disk(index);

        info!("Processing of {} files completed.", file_count);
    }

    /**
     * Creates an intermediate path
     * Returns (intermediate_path: PathBuf, was_newly_created: bool)
     */
    fn create_intermediate_path(&mut self, suffix: &str) -> (PathBuf, bool) {
        let path: PathBuf = self.config.intermediate_path.join(
            Path::new(suffix),
        );
        let path_is_new = !path.exists();
        if path_is_new {
            fs::create_dir_all(&path).unwrap();
        }
        (path, path_is_new)
    }

    fn preprocess_single_file(&self, input_path: PathBuf, memory: &mut GraphBucketManager) {
        trace!("Reading in input file...");
        let raw_rows = self.read_lines(&input_path);
        trace!("Parsing row data...");
        let address_type = &self.config.address_type;
        raw_rows.iter().for_each(|row|
            parser::parse_data_into_memory(row, memory, address_type)
        );
    }

    fn read_lines(&self, path: &PathBuf) -> Vec<String> {
        let file_name = path.to_str().unwrap();
        let error_string = &format!("file {} not found or invalid data", file_name);
        debug!("Reading in data for {}", file_name);

        let data = if self.config.read_compressed {
            file_util::read_bzip2_lines(path).expect(error_string)
        } else {
            fs::read_to_string(path.to_str().unwrap()).expect(error_string)
        };

        debug!("Finished reading in data for {}.", file_name);

        data.lines()
            .filter(|&s| !s.starts_with("#"))
            .map(str::to_string)
            .collect()
    }

    fn store_index_to_disk(&self, index: HashMap<u128, u64>) {
        let node_index_path = self.config.intermediate_path.join(
            Path::new(parameters::NODE_INDEX_FILENAME)
        );
        file_util::write_binary_to_file(&node_index_path, &index);
    }
}