pub mod parameters {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::exit;

    use log::error;

    use crate::IpType;

    pub struct GraphBuilderParameters {
        address_type: IpType,
        input_path: PathBuf,
        intermediary_file_path: PathBuf,
        output_path: PathBuf,
    }

    impl GraphBuilderParameters {
        pub fn new(address_type: IpType, input_folder: &str, output_folder: &str) -> GraphBuilderParameters {
            let input_path = Path::new(input_folder).to_path_buf();
            let intermediary_file_path = input_path.join(Path::new("intermediate"));
            let output_path = Path::new(output_folder).to_path_buf();

            if !input_path.exists() {
                error!("Specified input path does not exist");
                exit(1);
            }

            if !input_path.is_dir() {
                error!("Specified input path is not a directory");
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

            fs::create_dir_all(&intermediary_file_path).expect("Could not create intermediary file path");

            GraphBuilderParameters {
                address_type,
                input_path,
                intermediary_file_path,
                output_path,
            }
        }

        pub fn address_type(&self) -> &IpType {
            &self.address_type
        }
        pub fn input_path(&self) -> &PathBuf {
            &self.input_path
        }
        pub fn intermediary_file_path(&self) -> &PathBuf {
            &self.intermediary_file_path
        }
        pub fn output_path(&self) -> &PathBuf {
            &self.output_path
        }
    }
}