use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::process::Command;
use log::error;
use serde::Serialize;

pub fn write_to_file<T: Serialize>(path: &PathBuf, data: &T) {
    let file = File::create(path).expect("Error while creating file to write");
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, data).expect("Error while serializing bucket");
}

pub fn read_bzip2_lines<P>(filename: P) -> Option<String>
    where P: AsRef<Path> {
    let filename_str = match filename.as_ref().to_str() {
        None => { return None; }
        Some(value) => { value }
    };

    let output = Command::new("/usr/bin/lbzip2")
        .arg("-d").arg("-k").arg("-c").arg(filename_str)
        .output()
        .expect("Failed to execute lbzip2 for decompressing file!");

    if !output.status.success() {
        error!("Subprocessed lbzip2 failed with status {}", output.status);
        return None;
    }

    let mut output_str = String::new();
    output_str.push_str(match std::str::from_utf8(&output.stdout) {
        Ok(val) => val,
        Err(_) => panic!("got non UTF-8 data from lbzip2"),
    });

    Some(output_str)
}
