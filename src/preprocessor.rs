pub mod preprocessor {
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::fs;
    use std::fs::DirEntry;
    use std::hash::Hash;
    use std::io::BufWriter;
    use std::path::PathBuf;
    use std::process::exit;

    use log::{error, info, trace};
    use pbr::ProgressBar;
    use serde::Serialize;

    use crate::{IpType, preprocessor_util, Row, RowIpv4, RowIpv6};
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

            info!("Processing {} files.", file_count);
            progress_bar.set(0);
            for file in files_to_process {
                self.preprocess_single_file(file.path());
                progress_bar.inc();
            }

            info!("Processing of {} files completed.", file_count);
        }

        fn preprocess_single_file(&self, input_path: PathBuf) {
            trace!("Reading in input file...");
            let raw_rows = self.read_lines(&input_path);
            trace!("Parsing row data...");
            let address_type = self.config.address_type();
            let entries: Vec<Row> = raw_rows.iter()
                .map(|row| preprocessor_util::parser::parse_data_from_row(row, address_type))
                .filter_map(|row| row)
                .collect();

            trace!("Processing parsed entries...");
            let file_name = &input_path.file_name().unwrap();
            match address_type {
                IpType::V4 => self.process_entries_as_ipv4(entries, file_name),
                IpType::V6 => self.process_entries_as_ipv6(entries, file_name),
            }
        }

        fn read_lines(&self, path: &PathBuf) -> Vec<String> {
            std::fs::read_to_string(path.to_str().unwrap())
                .expect("file not found!")
                .lines()
                .filter(|&s| !s.starts_with("#"))
                .map(str::to_string)
                .collect()
        }

        fn process_entries_as_ipv4(&self, entries: Vec<Row>, file_name: &OsStr) {
            let mut edge_map = HashMap::<u32, Vec<(u32, u8)>>::new();
            for entry in entries {
                match entry {
                    Row::V4(row) => self.process_ipv4_entry(row, &mut edge_map),
                    Row::V6(_) => {
                        error!("Received an IPv6 address in the IPV4 row processing stage, this should be impossible");
                        exit(1);
                    },
                }
            }
            self.write_to_file(&edge_map, file_name);
        }

        fn process_entries_as_ipv6(&self, entries: Vec<Row>, file_name: &OsStr) {
            let mut edge_map = HashMap::<u128, Vec<(u128, u8)>>::new();
            for entry in entries {
                match entry {
                    Row::V4(_) => {
                        error!("Received an IPv4 address in the IPV6 row processing stage, this should be impossible");
                        exit(1);
                    },
                    Row::V6(row) => self.process_ipv6_entry(row, &mut edge_map),
                }
            }
            self.write_to_file(&edge_map, file_name);
        }

        fn process_ipv4_entry(&self, row: RowIpv4, edge_map: &mut HashMap<u32, Vec<(u32, u8)>>) {
            if !edge_map.contains_key(&row.target_ip) {
                let new_list = Vec::<(u32, u8)>::new();
                edge_map.insert(row.target_ip, new_list);
            }
            let list = edge_map.get_mut(&row.target_ip).unwrap();
            list.push((row.hop_ip, row.hop_count));
        }

        fn process_ipv6_entry(&self, row: RowIpv6, edge_map: &mut HashMap<u128, Vec<(u128, u8)>>) {
            if !edge_map.contains_key(&row.target_ip) {
                let new_list = Vec::<(u128, u8)>::new();
                edge_map.insert(row.target_ip, new_list);
            }
            let list = edge_map.get_mut(&row.target_ip).unwrap();
            list.push((row.hop_ip, row.hop_count));
        }

        fn write_to_file<T1: Hash + Eq + Serialize, T2: Serialize>(&self, edge_map: &HashMap<T1, Vec<(T1, T2)>>, file_name: &OsStr) {
            trace!("Writing result to disk...");
            let file = std::fs::File::create(self.config.intermediary_file_path().join(file_name))
                .expect("Error while creating file to write...feels bad man");
            let writer = BufWriter::new(file);
            bincode::serialize_into(writer, edge_map).expect("should have worked");
            trace!("Result written to disk.");
        }
    }
}