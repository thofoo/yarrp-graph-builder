pub mod preprocessor {
    use std::fs;
    use std::fs::DirEntry;
    use std::io::BufWriter;
    use std::path::PathBuf;

    use log::{info, trace};
    use pbr::ProgressBar;

    use crate::{preprocessor_util};
    use crate::bucket::bucket::GraphBucket;
    use crate::parameters::parameters::GraphBuilderParameters;

    pub struct Preprocessor {
        config: GraphBuilderParameters,
    }

    impl Preprocessor {
        pub fn new(config: GraphBuilderParameters) -> Preprocessor {
            Preprocessor { config }
        }

        pub fn preprocess_files(self) {
            info!("Expecting to read IP{:?} addresses.", self.config.address_type());

            info!("Input path: {}", self.config.input_path().to_str().unwrap());
            info!("Intermediary file path: {}", self.config.intermediary_file_path().to_str().unwrap());
            info!("Output path: {}", self.config.output_path().to_str().unwrap());

            let raw_files_list = fs::read_dir(self.config.input_path()).unwrap();
            let files_to_process: Vec<DirEntry> = raw_files_list
                .map(|entry| entry.unwrap())
                .filter(|i| i.path().is_file())
                .collect();

            let file_count = files_to_process.len() as u64;
            let mut progress_bar = ProgressBar::new(file_count);

            info!("Processing {} files...", file_count);
            progress_bar.set(0);
            let mut memory = GraphBucket::new();
            for file in files_to_process {
                self.preprocess_single_file(file.path(), &mut memory);
                progress_bar.inc();
            }

            info!("Writing results to file...");
            self.write_to_file(&memory);
            info!("Processing of {} files completed.", file_count);
        }

        fn preprocess_single_file(&self, input_path: PathBuf, memory: &mut GraphBucket) {
            trace!("Reading in input file...");
            let raw_rows = self.read_lines(&input_path);
            trace!("Parsing row data...");
            let address_type = self.config.address_type();
            raw_rows.iter().for_each(|row|
                preprocessor_util::parser::parse_data_from_row(row, memory, address_type)
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

        fn write_to_file(&self, graph: &GraphBucket) {
            trace!("Writing result to disk...");
            let file = std::fs::File::create(self.config.intermediary_file_path().join("yarrp-graph.raw"))
                .expect("Error while creating file to write...feels bad man");
            let writer = BufWriter::new(file);
            bincode::serialize_into(writer, graph).expect("should have worked");
            trace!("Result written to disk.");
        }
    }
}