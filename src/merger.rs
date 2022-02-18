pub mod merger {
    use std::collections::HashMap;
    use std::fs;
    use std::fs::{DirEntry, File};
    use std::path::{Path, PathBuf};

    use log::info;
    use pbr::ProgressBar;

    use crate::parameters;

    pub struct Merger {
        intermediary_file_path: PathBuf,
    }

    impl Merger {
        pub fn new(intermediary_file_path: PathBuf) -> Merger {
            Merger {
                intermediary_file_path,
            }
        }

        pub fn merge_data(self) {
            let raw_files_list = fs::read_dir(&self.intermediary_file_path).unwrap();
            let files_to_process: Vec<DirEntry> = raw_files_list
                .map(|entry| entry.unwrap())
                .filter(|i| i.path().is_file())
                .collect();

            let index_path = self.intermediary_file_path.join(
                Path::new(parameters::parameters::NODE_INDEX_PATH_SUFFIX)
            );
            let index_file = File::open(&index_path).expect(&format!(
                "File at {} does not exist", index_path.to_str().unwrap()
            ));
            let index: HashMap<u128, u32> = bincode::deserialize_from(index_file).expect(&format!(
                "File at {} does not contain or contains invalid node index data",
                index_path.to_str().unwrap()
            ));

            let file_count = files_to_process.len() as u64;
            let mut progress_bar = ProgressBar::new(file_count);

            info!("Processing {} intermediary files to the final format...", file_count);
            /* progress_bar.set(0);

            for file in files_to_process {
                let bucket = GraphBucket::new(file.path());

                progress_bar.inc();
            }

            // read in one file
            // untangle
            // write untangled edges into edge file AND INTO EDGE CACHE
            // write nodes into

             */
        }
    }
}