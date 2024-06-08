use std::{
    fs::File,
    io::{BufReader, Read, Write},
};

use anyhow::Error;

pub struct InvoicingParser {}

impl InvoicingParser {
    pub async fn parse_invoicing_file(file_path: &str) -> Result<String, Error> {
        // Open the csv file.
        println!("❕ Opening file {}", file_path);
        let mut file = File::open(file_path)?;
        let mut lines = String::new();

        println!("❕ Reading file to string.");
        file.read_to_string(&mut lines)?;
        file.flush()?;
        println!("❕ Done");

        println!("❕ Replacing tab stops.");
        let lines = lines.replace("\t", ",");

        println!("{:?}", lines);

        Ok(lines)
    }
}
