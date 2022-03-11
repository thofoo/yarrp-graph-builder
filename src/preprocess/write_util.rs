use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use serde::Serialize;

pub fn write_to_file<T: Serialize>(path: &PathBuf, data: &T) {
    let file = File::create(path).expect("Error while creating file to write");
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, data).expect("Error while serializing bucket");
}