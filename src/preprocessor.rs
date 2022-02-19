pub mod preprocessor {
    use std::fs;
    use std::fs::DirEntry;
    use std::path::PathBuf;

    use log::{info, trace};
    use pbr::ProgressBar;

    use crate::bucket_manager::bucket_manager::GraphBucketManager;
    use crate::parameters::parameters::GraphBuilderParameters;
    use crate::parser;

    pub struct Preprocessor {
        config: GraphBuilderParameters,
    }

    impl Preprocessor {
        pub fn new(config: &GraphBuilderParameters) -> Preprocessor {
            Preprocessor { config: config.clone() }
        }

        pub fn preprocess_files(&self) {
            if !self.config.should_preprocess() {
                info!("Preprocessing flag is FALSE - skipping preprocessing.");
                return;
            }

            info!("Initializing preprocessing.");

            let raw_files_list = fs::read_dir(&self.config.input_path()).unwrap();
            let files_to_process: Vec<DirEntry> = raw_files_list
                .map(|entry| entry.unwrap())
                .filter(|i| i.path().is_file())
                .collect();

            let file_count = files_to_process.len() as u64;
            let mut progress_bar = ProgressBar::new(file_count);

            info!("Processing {} files...", file_count);
            progress_bar.set(0);
            let mut memory = GraphBucketManager::new(&self.config);
            for file in files_to_process {
                self.preprocess_single_file(file.path(), &mut memory);
                progress_bar.inc();
            }
            memory.store_all_to_disk();

            info!("Processing of {} files completed.", file_count);
        }

        fn preprocess_single_file(&self, input_path: PathBuf, memory: &mut GraphBucketManager) {
            trace!("Reading in input file...");
            let raw_rows = self.read_lines(&input_path);
            trace!("Parsing row data...");
            let address_type = &self.config.address_type();
            raw_rows.iter().for_each(|row|
                parser::parser::parse_data_from_row(row, memory, address_type)
            );
        }

        fn read_lines(&self, path: &PathBuf) -> Vec<String> {
            std::fs::read_to_string(path.to_str().unwrap())
                .expect("file not found!")
                .lines()
                .filter(|&s| !s.starts_with("#"))
                .map(str::to_string)
                .collect()
        }
    }
}