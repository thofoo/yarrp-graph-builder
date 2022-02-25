pub mod parameters {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::exit;

    use log::error;

    use crate::IpType;

    pub const NODE_INDEX_PATH: &str = "yarrp.node_index.bin";

    #[derive(Clone)]
    pub struct GraphBuilderParameters {
        address_type: IpType,
        input_path: PathBuf,
        intermediary_file_path_original: PathBuf,
        intermediary_file_path: PathBuf,
        output_path: PathBuf,
        should_preprocess: bool,
        should_merge: bool,
        should_persist_index: bool,
        should_persist_edges: bool,
    }

    impl GraphBuilderParameters {
        pub fn new(
            address_type: IpType,
            input_folder: &str,
            intermediate_folder: &str,
            output_folder: &str,
            should_preprocess: bool,
            should_merge: bool,
            should_persist_index: bool,
            should_persist_edges: bool,
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

            GraphBuilderParameters {
                address_type,
                input_path,
                intermediary_file_path_original: intermediary_file_path.to_path_buf(),
                intermediary_file_path,
                output_path,
                should_preprocess,
                should_merge,
                should_persist_index,
                should_persist_edges,
            }
        }

        pub fn add_intermediate_suffix(&mut self, suffix: &str) {
            self.intermediary_file_path = self.intermediary_file_path_original.join(
                Path::new(suffix),
            );
            fs::create_dir_all(&self.intermediary_file_path).unwrap();
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

        pub fn output_path(&self) -> &PathBuf {
            &self.output_path
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
    }
}